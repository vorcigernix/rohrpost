export const CANONICAL_STREAMS = [
  "ingress",
  "work",
  "retry",
  "dlq",
  "audit",
  "replay",
] as const;

export type CanonicalStreamName = (typeof CANONICAL_STREAMS)[number];

export const CANONICAL_ENVELOPE_FIELDS = [
  "tenantId",
  "flowId",
  "revisionId",
  "messageId",
  "sourceRef",
  "partitionKey",
  "headers",
  "payload",
  "receivedAt",
  "traceId",
] as const;

export const FLOW_SPEC_VERSION = 1 as const;
