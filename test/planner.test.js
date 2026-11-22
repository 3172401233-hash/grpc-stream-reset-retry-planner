import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planRecovery } from '../src/planner.js';
import { classifyMessage, MessageState } from '../src/states.js';

const T0 = 1_700_000_000_000;

function makeMessages(range, sentUpTo) {
  return range.map((seq) => ({
    seq,
    idempotencyKey: `key-${seq}`,
    sentAt: seq <= sentUpTo ? T0 + seq * 100 : null,
  }));
}

function decisionOf(plan, seq) {
  return plan.decisions.find((d) => d.seq === seq);
}

test('断线位置在消息中间：已确认继续、已发送未确认重放、未发送继续', () => {
  const plan = planRecovery({
    streamId: 3,
    messages: makeMessages([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 7),
    cumulativeAck: 4,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 16,
    events: [{ type: 'RST_STREAM', code: 'INTERNAL_ERROR', at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: { mayStillCommitUntil: T0 + 4_500 },
  });

  assert.equal(decisionOf(plan, 1).state, MessageState.COMMITTED);
  assert.equal(decisionOf(plan, 1).decision, 'continue');
  for (const seq of [5, 6, 7]) {
    assert.equal(decisionOf(plan, seq).state, 'sent-unacked');
    assert.equal(decisionOf(plan, seq).decision, 'replay');
  }
  for (const seq of [8, 9, 10]) {
    assert.equal(decisionOf(plan, seq).state, 'unsent');
    assert.equal(decisionOf(plan, seq).decision, 'continue');
  }
  assert.deepEqual(plan.safeReplayWindow, { from: 5, to: 7, seqs: [5, 6, 7] });
  assert.equal(plan.trailers[0].grpcStatus, 'INTERNAL');
  assert.equal(plan.trailers[0].retryable, true);
  assert.equal(plan.deadline.exceeded, false);
});

test('响应丢失：已处理未响应的消息等待证据，不盲目重放', () => {
  const plan = planRecovery({
    streamId: 5,
    messages: makeMessages([1, 2, 3], 3),
    cumulativeAck: 1,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 8,
    events: [{ type: 'RST_STREAM', code: 'REFUSED_STREAM', at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [
      { seq: 2, idempotencyKey: 'key-2', status: 'processed' },
      { seq: 3, idempotencyKey: 'key-3', status: 'committed' },
    ],
    oldConnection: {},
  });

  assert.equal(decisionOf(plan, 2).state, 'processed-response-lost');
  assert.equal(decisionOf(plan, 2).decision, 'wait');
  assert.equal(decisionOf(plan, 2).reason, 'response-lost-await-evidence');
  assert.equal(decisionOf(plan, 3).state, 'committed');
  assert.equal(decisionOf(plan, 3).decision, 'continue');
  assert.deepEqual(plan.safeReplayWindow.seqs, []);
});

test('GOAWAY 后迁移：旧流等待排空，新流立即在新连接重放', () => {
  const draining = planRecovery({
    streamId: 3,
    messages: makeMessages([1, 2, 3], 3),
    cumulativeAck: 1,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 8,
    events: [{ type: 'GOAWAY', lastStreamId: 5, at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: { mayStillCommitUntil: T0 + 30_000 },
  });

  assert.equal(draining.migration.required, true);
  assert.equal(draining.migration.action, 'wait-for-drain');
  assert.equal(decisionOf(draining, 2).decision, 'wait');
  assert.equal(decisionOf(draining, 2).reason, 'old-connection-may-commit');
  assert.equal(draining.trailers[0].action, 'migrate-connection');

  const fresh = planRecovery({
    streamId: 7,
    messages: makeMessages([1, 2], 2),
    cumulativeAck: 0,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 8,
    events: [{ type: 'GOAWAY', lastStreamId: 5, at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: {},
  });

  assert.equal(fresh.migration.action, 'replay-on-new-connection');
  assert.equal(decisionOf(fresh, 1).decision, 'replay');
});

test('超时：deadline 已过时未提交消息全部失败', () => {
  const plan = planRecovery({
    streamId: 3,
    messages: makeMessages([1, 2, 3, 4], 4),
    cumulativeAck: 1,
    deadline: T0 + 4_000,
    now: T0 + 5_000,
    bufferWindow: 8,
    events: [{ type: 'RST_STREAM', code: 'INTERNAL_ERROR', at: T0 + 3_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: {},
  });

  assert.equal(plan.deadline.exceeded, true);
  assert.equal(plan.deadline.remainingMs, 0);
  assert.equal(decisionOf(plan, 1).decision, 'continue');
  for (const seq of [2, 3, 4]) {
    assert.equal(decisionOf(plan, seq).decision, 'fail');
    assert.equal(decisionOf(plan, seq).reason, 'deadline-exceeded');
  }
});

test('重复重放：旧连接未排空时禁止并行重放同一幂等键', () => {
  const plan = planRecovery({
    streamId: 3,
    messages: makeMessages([1, 2], 2),
    cumulativeAck: 0,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 8,
    events: [{ type: 'RST_STREAM', code: 'REFUSED_STREAM', at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: { mayStillCommitUntil: T0 + 10_000 },
  });

  assert.equal(decisionOf(plan, 1).decision, 'wait');
  assert.equal(decisionOf(plan, 1).reason, 'old-connection-may-commit');

  // 旧连接排空后，同一幂等键可安全重放。
  const after = planRecovery({
    ...{},
    streamId: 3,
    messages: makeMessages([1, 2], 2),
    cumulativeAck: 0,
    deadline: T0 + 60_000,
    now: T0 + 11_000,
    bufferWindow: 8,
    events: [{ type: 'RST_STREAM', code: 'REFUSED_STREAM', at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: { mayStillCommitUntil: T0 + 10_000 },
  });
  assert.equal(decisionOf(after, 1).decision, 'replay');
});

test('重试次数耗尽与缓冲驱逐的消息失败', () => {
  const messages = makeMessages([1, 2, 3, 4, 5, 6], 6);
  messages[5].attempts = 3;
  const plan = planRecovery({
    streamId: 3,
    messages,
    cumulativeAck: 0,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 4,
    events: [],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: {},
  });

  // 窗口为 4，最大序号 6，序号 1、2 已被驱逐。
  assert.equal(decisionOf(plan, 1).decision, 'fail');
  assert.equal(decisionOf(plan, 1).reason, 'evicted-from-replay-buffer');
  assert.equal(decisionOf(plan, 6).decision, 'fail');
  assert.equal(decisionOf(plan, 6).reason, 'retry-exhausted');
  assert.equal(decisionOf(plan, 3).decision, 'replay');
});

test('不可重试的 trailers 状态使未提交消息失败', () => {
  const plan = planRecovery({
    streamId: 3,
    messages: makeMessages([1, 2], 2),
    cumulativeAck: 0,
    deadline: T0 + 60_000,
    now: T0 + 5_000,
    bufferWindow: 8,
    events: [{ type: 'RST_STREAM', code: 'CANCEL', at: T0 + 4_000 }],
    retryPolicy: { maxAttempts: 3 },
    commitEvidence: [],
    oldConnection: {},
  });

  assert.equal(plan.trailers[0].retryable, false);
  assert.equal(decisionOf(plan, 1).decision, 'fail');
  assert.match(decisionOf(plan, 1).reason, /non-retryable-status/);
});

test('classifyMessage 状态优先级：累计确认高于其他证据', () => {
  const ctx = {
    cumulativeAck: 5,
    evidenceByKey: new Map(),
    evidenceBySeq: new Map(),
  };
  assert.equal(
    classifyMessage({ seq: 3, idempotencyKey: 'k', sentAt: null }, ctx),
    'committed',
  );
  assert.equal(
    classifyMessage({ seq: 6, idempotencyKey: 'k', sentAt: null }, ctx),
    'unsent',
  );
});
