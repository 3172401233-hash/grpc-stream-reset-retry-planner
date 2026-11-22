import { MessageState, Decision, classifyMessage } from './states.js';
import { resolveTrailers } from './trailers.js';

/**
 * @typedef {object} PlannerInput
 * @property {string|number} streamId 流 ID
 * @property {Array<object>} messages 消息列表：{ seq, idempotencyKey, sentAt?, attempts? }
 * @property {number} cumulativeAck 累计确认序号（含）之前的消息均已确认
 * @property {number} deadline 绝对截止时间（ms 时间戳）
 * @property {number} now 当前时间（ms 时间戳）
 * @property {number} bufferWindow 客户端重放缓冲窗口大小（条数）
 * @property {Array<object>} events RST_STREAM / GOAWAY 事件
 * @property {object} retryPolicy { maxAttempts }
 * @property {Array<object>} commitEvidence 服务端提交证据：{ seq?, idempotencyKey?, status: 'committed'|'processed' }
 * @property {object} oldConnection 旧连接状态：{ mayStillCommitUntil?: number }
 */

/**
 * 规划断线后的流恢复：对每条消息给出 continue / replay / wait / fail 决定，
 * 并输出 trailers 与 deadline 处理结果。
 *
 * 核心安全约束：旧连接仍可能提交时（now < mayStillCommitUntil），
 * 新连接不得并行重放同一幂等键，对应消息决定为 wait。
 *
 * @param {PlannerInput} input
 */
export function planRecovery(input) {
  const {
    streamId,
    messages,
    cumulativeAck,
    deadline,
    now,
    bufferWindow,
    events = [],
    retryPolicy = {},
    commitEvidence = [],
    oldConnection = {},
  } = input;

  const maxAttempts = retryPolicy.maxAttempts ?? 3;
  const deadlineExceeded = now >= deadline;
  const oldMayCommit =
    oldConnection.mayStillCommitUntil != null &&
    now < oldConnection.mayStillCommitUntil;

  const evidenceByKey = new Map();
  const evidenceBySeq = new Map();
  for (const ev of commitEvidence) {
    if (ev.idempotencyKey != null) evidenceByKey.set(ev.idempotencyKey, ev);
    if (ev.seq != null) evidenceBySeq.set(ev.seq, ev);
  }

  const ctx = { cumulativeAck, evidenceByKey, evidenceBySeq };
  // 缓冲窗口覆盖的最低序号：窗口外的消息已被驱逐，无法重放。
  const maxSeq = messages.reduce((acc, m) => Math.max(acc, m.seq), 0);
  const bufferFloor = maxSeq - bufferWindow + 1;

  const trailers = events.map((event) => resolveTrailers(event));
  const nonRetryable = trailers.find((t) => !t.retryable);

  const decisions = messages.map((msg) => {
    const state = classifyMessage(msg, ctx);
    const decision = decideMessage(state, msg, {
      deadlineExceeded,
      oldMayCommit,
      nonRetryable,
      maxAttempts,
      bufferFloor,
    });
    return {
      seq: msg.seq,
      idempotencyKey: msg.idempotencyKey,
      state,
      ...decision,
    };
  });

  return {
    streamId,
    decisions,
    trailers,
    deadline: {
      deadline,
      now,
      remainingMs: Math.max(0, deadline - now),
      exceeded: deadlineExceeded,
    },
    migration: buildMigration(streamId, events),
    safeReplayWindow: computeSafeReplayWindow(decisions),
  };
}

function decideMessage(state, msg, ctx) {
  const attempts = msg.attempts ?? 0;

  if (state !== MessageState.COMMITTED) {
    if (ctx.deadlineExceeded) {
      return { decision: Decision.FAIL, reason: 'deadline-exceeded' };
    }
    if (ctx.nonRetryable) {
      return {
        decision: Decision.FAIL,
        reason: `non-retryable-status:${ctx.nonRetryable.grpcStatus}`,
      };
    }
  }

  switch (state) {
    case MessageState.COMMITTED:
      return { decision: Decision.CONTINUE, reason: 'already-committed' };

    case MessageState.UNSENT:
      return { decision: Decision.CONTINUE, reason: 'never-sent' };

    case MessageState.SENT_UNACKED:
      if (msg.seq < ctx.bufferFloor) {
        return { decision: Decision.FAIL, reason: 'evicted-from-replay-buffer' };
      }
      if (attempts >= ctx.maxAttempts) {
        return { decision: Decision.FAIL, reason: 'retry-exhausted' };
      }
      if (ctx.oldMayCommit) {
        // 旧连接仍可能提交同一幂等键，禁止并行重放。
        return { decision: Decision.WAIT, reason: 'old-connection-may-commit' };
      }
      return {
        decision: Decision.REPLAY,
        reason: 'safe-replay-same-idempotency-key',
      };

    case MessageState.PROCESSED_RESPONSE_LOST:
      // 服务端已处理但响应丢失：先等待证据或对账，不盲目重放。
      return { decision: Decision.WAIT, reason: 'response-lost-await-evidence' };

    default:
      return { decision: Decision.FAIL, reason: 'unknown-state' };
  }
}

function buildMigration(streamId, events) {
  const goaway = events.find((e) => e.type === 'GOAWAY');
  if (!goaway) {
    return { required: false };
  }
  const lastStreamId = goaway.lastStreamId ?? null;
  const processedByOld =
    lastStreamId != null && Number(streamId) <= Number(lastStreamId);
  return {
    required: true,
    lastStreamId,
    // 流 ID 大于 lastStreamId 的流服务端从未处理，可立即在新连接重放；
    // 否则需等待旧流排空（drain）后再重放。
    action: processedByOld ? 'wait-for-drain' : 'replay-on-new-connection',
  };
}

function computeSafeReplayWindow(decisions) {
  const replayable = decisions
    .filter((d) => d.decision === Decision.REPLAY)
    .map((d) => d.seq)
    .sort((a, b) => a - b);
  if (replayable.length === 0) {
    return { from: null, to: null, seqs: [] };
  }
  return {
    from: replayable[0],
    to: replayable[replayable.length - 1],
    seqs: replayable,
  };
}
