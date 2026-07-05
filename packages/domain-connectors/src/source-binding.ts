import type { FlowSpec } from "@rohrpost/shared-flow-spec";

export type JsonSourceKind = "http" | "nats" | "kafka";

export type SourceCapabilityId = "http_in" | "nats_in" | "kafka_in";

export interface ExistingSourceConnector {
  id: string;
  capabilityId: string;
  config: Record<string, unknown>;
}

export interface JsonSourceBinding {
  sourceKind: JsonSourceKind;
  capabilityId: SourceCapabilityId;
  executionMode: "native" | "adapter";
  connectorId: string;
  connectorName: string;
  ref: string;
  config: Record<string, unknown>;
  generated: boolean;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "flow";
}

function connectorSlug(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 64) || "source";
}

function capabilityIdForSourceKind(sourceKind: JsonSourceKind): SourceCapabilityId {
  switch (sourceKind) {
    case "kafka":
      return "kafka_in";
    case "nats":
      return "nats_in";
    default:
      return "http_in";
  }
}

function executionModeForSourceKind(sourceKind: JsonSourceKind): "native" | "adapter" {
  return sourceKind === "kafka" ? "adapter" : "native";
}

function buildBaseRef(sourceKind: JsonSourceKind, flowName: string): string {
  const slug = slugify(flowName);
  switch (sourceKind) {
    case "kafka":
      return `router.ingress.${slug}`;
    case "nats":
      return `events.source.${slug}`;
    default:
      return `/ingest/${slug}`;
  }
}

function withSuffix(sourceKind: JsonSourceKind, baseRef: string, suffix: number): string {
  if (suffix <= 1) {
    return baseRef;
  }

  switch (sourceKind) {
    case "kafka":
    case "nats":
      return `${baseRef}-${suffix}`;
    default:
      return `${baseRef}-${suffix}`;
  }
}

function extractRef(
  sourceKind: JsonSourceKind,
  config: Record<string, unknown>,
): string | null {
  switch (sourceKind) {
    case "kafka":
      return typeof config.topic === "string" ? config.topic : null;
    case "nats":
      return typeof config.subject === "string" ? config.subject : null;
    default:
      return typeof config.path === "string" ? config.path : null;
  }
}

function buildConfig(sourceKind: JsonSourceKind, ref: string): Record<string, unknown> {
  switch (sourceKind) {
    case "kafka":
      return {
        topic: ref,
        brokers: ["host.docker.internal:9092"],
      };
    case "nats":
      return { subject: ref };
    default:
      return { path: ref, method: "POST" };
  }
}

function buildConnectorId(capabilityId: SourceCapabilityId, ref: string): string {
  return `${capabilityId}_${connectorSlug(ref)}`;
}

function buildConnectorName(sourceKind: JsonSourceKind, flowName: string): string {
  switch (sourceKind) {
    case "kafka":
      return `${flowName} Kafka ingress`;
    case "nats":
      return `${flowName} NATS ingress`;
    default:
      return `${flowName} HTTP ingress`;
  }
}

export function isDefaultSourceConnectorId(connectorId: string | undefined): boolean {
  return connectorId === "http_in_default"
    || connectorId === "nats_in_default"
    || connectorId === "kafka_in_default";
}

export function buildAutoSourceBinding(input: {
  sourceKind: JsonSourceKind;
  flowName: string;
  existingConnectors: ExistingSourceConnector[];
}): JsonSourceBinding {
  const capabilityId = capabilityIdForSourceKind(input.sourceKind);
  const existingRefs = new Set(
    input.existingConnectors
      .filter((connector) => connector.capabilityId === capabilityId)
      .map((connector) => extractRef(input.sourceKind, connector.config))
      .filter((ref): ref is string => Boolean(ref)),
  );

  const baseRef = buildBaseRef(input.sourceKind, input.flowName);
  let suffix = 1;
  let ref = withSuffix(input.sourceKind, baseRef, suffix);

  while (existingRefs.has(ref)) {
    suffix += 1;
    ref = withSuffix(input.sourceKind, baseRef, suffix);
  }

  return {
    sourceKind: input.sourceKind,
    capabilityId,
    executionMode: executionModeForSourceKind(input.sourceKind),
    connectorId: buildConnectorId(capabilityId, ref),
    connectorName: buildConnectorName(input.sourceKind, input.flowName),
    ref,
    config: buildConfig(input.sourceKind, ref),
    generated: true,
  };
}

export function applySourceBindingToSpec(
  spec: FlowSpec,
  binding: JsonSourceBinding,
): FlowSpec {
  if (spec.sources.length === 0) {
    return spec;
  }

  return {
    ...spec,
    sources: spec.sources.map((source, index) =>
      index === 0
        ? {
            ...source,
            connector: {
              capabilityId: binding.capabilityId,
              connectorId: binding.connectorId,
              executionMode: binding.executionMode,
            },
          }
        : source,
    ),
  };
}
