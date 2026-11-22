// 消息在断线时刻的四种状态。
export const MessageState = Object.freeze({
  UNSENT: 'unsent',
  SENT_UNACKED: 'sent-unacked',
  PROCESSED_RESPONSE_LOST: 'processed-response-lost',
  COMMITTED: 'committed',
});

// 规划器对每条消息的决定。
export const Decision = Object.freeze({
  CONTINUE: 'continue',
  REPLAY: 'replay',
  WAIT: 'wait',
  FAIL: 'fail',
});

/**
 * 根据累计确认与服务端提交证据，判定一条消息的状态。
 *
 * 优先级：累计确认 > 提交证据 > 处理证据 > 是否已发送。
 *
 * @param {object} msg 消息，含 seq / idempotencyKey / sentAt
 * @param {object} ctx 上下文：cumulativeAck、evidenceByKey、evidenceBySeq
 * @returns {string} MessageState 之一
 */
export function classifyMessage(msg, ctx) {
  if (msg.seq <= ctx.cumulativeAck) {
    return MessageState.COMMITTED;
  }
  const evidence =
    ctx.evidenceByKey.get(msg.idempotencyKey) ?? ctx.evidenceBySeq.get(msg.seq);
  if (evidence?.status === 'committed') {
    return MessageState.COMMITTED;
  }
  if (evidence?.status === 'processed') {
    return MessageState.PROCESSED_RESPONSE_LOST;
  }
  if (msg.sentAt == null) {
    return MessageState.UNSENT;
  }
  return MessageState.SENT_UNACKED;
}
