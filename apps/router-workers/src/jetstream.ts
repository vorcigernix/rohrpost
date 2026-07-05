export const JETSTREAM_STREAMS = {
  ingress: "ingress",
  work: "work",
  retry: "retry",
  dlq: "dlq",
  audit: "audit",
  replay: "replay"
} as const;

export type JetStreamStreamName =
  (typeof JETSTREAM_STREAMS)[keyof typeof JETSTREAM_STREAMS];

export const JETSTREAM_SUBJECTS = {
  ingress: "router.ingress.>",
  work: "router.work.>",
  retry: "router.retry.>",
  dlq: "router.dlq.>",
  audit: "router.audit.>",
  replay: "router.replay.>"
} as const;

export function buildJetStreamSubject(
  stream: JetStreamStreamName,
  tenantId: string,
  flowId: string,
  revisionId: string,
  messageId: string
): string {
  return `router.${stream}.${tenantId}.${flowId}.${revisionId}.${messageId}`;
}

export function buildDeploymentIngressPattern(
  tenantId: string,
  flowId: string,
  revisionId: string,
): string {
  return `router.${JETSTREAM_STREAMS.ingress}.${tenantId}.${flowId}.${revisionId}.>`;
}

export function buildDeploymentRetryPattern(
  tenantId: string,
  flowId: string,
  revisionId: string,
): string {
  return `router.${JETSTREAM_STREAMS.retry}.${tenantId}.${flowId}.${revisionId}.>`;
}

export function buildDeploymentReplayPattern(
  tenantId: string,
  flowId: string,
  revisionId: string,
): string {
  return `router.${JETSTREAM_STREAMS.replay}.${tenantId}.${flowId}.${revisionId}.>`;
}

export function buildReplaySubject(
  tenantId: string,
  flowId: string,
  revisionId: string,
  messageId: string,
): string {
  return buildJetStreamSubject(JETSTREAM_STREAMS.replay, tenantId, flowId, revisionId, messageId);
}

export function buildDlqSubject(
  tenantId: string,
  flowId: string,
  revisionId: string,
  messageId: string,
): string {
  return buildJetStreamSubject(JETSTREAM_STREAMS.dlq, tenantId, flowId, revisionId, messageId);
}

export function buildRetrySubject(
  tenantId: string,
  flowId: string,
  revisionId: string,
  messageId: string,
): string {
  return buildJetStreamSubject(JETSTREAM_STREAMS.retry, tenantId, flowId, revisionId, messageId);
}

export function resolveJetStreamStream(subject: string): JetStreamStreamName | null {
  const [prefix, stream] = subject.split(".", 3);
  if (prefix !== "router") {
    return null;
  }

  switch (stream) {
    case "ingress":
    case "work":
    case "retry":
    case "dlq":
    case "audit":
    case "replay":
      return stream;
    default:
      return null;
  }
}

export function parseJetStreamSubject(subject: string): {
  stream: JetStreamStreamName | null;
  tenantId: string;
  flowId: string;
  revisionId: string;
  messageId: string;
} {
  const parts = subject.split(".");
  return {
    stream: parts[1] === "ingress" || parts[1] === "work" || parts[1] === "retry" || parts[1] === "dlq" || parts[1] === "audit" || parts[1] === "replay"
      ? (parts[1] as JetStreamStreamName)
      : null,
    tenantId: parts[2] ?? "",
    flowId: parts[3] ?? "",
    revisionId: parts[4] ?? "",
    messageId: parts.slice(5).join("."),
  };
}
