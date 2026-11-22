// gRPC 状态码（trailers）到重试语义的映射。
const STATUS_TABLE = new Map([
  ['CANCELLED', { code: 1, retryable: false }],
  ['UNKNOWN', { code: 2, retryable: true }],
  ['DEADLINE_EXCEEDED', { code: 4, retryable: false }],
  ['RESOURCE_EXHAUSTED', { code: 8, retryable: false }],
  ['INTERNAL', { code: 13, retryable: true }],
  ['UNAVAILABLE', { code: 14, retryable: true }],
  ['UNAUTHENTICATED', { code: 16, retryable: false }],
]);

// HTTP/2 RST_STREAM 错误码到 gRPC 语义的映射。
const RST_TABLE = new Map([
  ['REFUSED_STREAM', { grpcStatus: 'UNAVAILABLE', retryable: true }],
  ['CANCEL', { grpcStatus: 'CANCELLED', retryable: false }],
  ['INTERNAL_ERROR', { grpcStatus: 'INTERNAL', retryable: true }],
  ['FLOW_CONTROL_ERROR', { grpcStatus: 'INTERNAL', retryable: true }],
  ['ENHANCE_YOUR_CALM', { grpcStatus: 'RESOURCE_EXHAUSTED', retryable: false }],
]);

/**
 * 解析连接事件（RST_STREAM / GOAWAY / 显式状态）为 trailers 处理结果。
 *
 * @param {object} event { type, code?, grpcStatus?, at?, lastStreamId? }
 * @returns {object} { type, grpcStatus, retryable, action, lastStreamId? }
 */
export function resolveTrailers(event) {
  if (event.type === 'GOAWAY') {
    return {
      type: 'GOAWAY',
      grpcStatus: event.grpcStatus ?? 'UNAVAILABLE',
      retryable: true,
      action: 'migrate-connection',
      lastStreamId: event.lastStreamId ?? null,
    };
  }
  if (event.type === 'RST_STREAM') {
    const mapped = RST_TABLE.get(event.code) ?? {
      grpcStatus: 'UNKNOWN',
      retryable: true,
    };
    return {
      type: 'RST_STREAM',
      grpcStatus: mapped.grpcStatus,
      retryable: mapped.retryable,
      action: mapped.retryable ? 'retry-stream' : 'fail-stream',
    };
  }
  const known = STATUS_TABLE.get(event.grpcStatus);
  const retryable = known?.retryable ?? false;
  return {
    type: event.type ?? 'STATUS',
    grpcStatus: event.grpcStatus ?? 'UNKNOWN',
    retryable,
    action: retryable ? 'retry-stream' : 'fail-stream',
  };
}
