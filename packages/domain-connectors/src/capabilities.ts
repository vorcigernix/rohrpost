import type { ConnectorCapability } from "@rohrpost/shared-flow-spec";

const ALL_GUARANTEES = ["idempotent", "append_only", "best_effort"] as const;

export const CONNECTOR_CAPABILITIES: ConnectorCapability[] = [
  {
    id: "http_in",
    name: "HTTP In",
    kind: "source",
    executionMode: "native",
    streamCompatibility: ["ingress"],
    deliveryGuarantees: [...ALL_GUARANTEES],
  },
  {
    id: "http_out",
    name: "HTTP Out",
    kind: "sink",
    executionMode: "native",
    streamCompatibility: ["work", "retry", "dlq", "replay"],
    deliveryGuarantees: ["idempotent", "best_effort"],
  },
  {
    id: "nats_in",
    name: "NATS In",
    kind: "source",
    executionMode: "native",
    streamCompatibility: ["ingress", "replay"],
    deliveryGuarantees: [...ALL_GUARANTEES],
  },
  {
    id: "nats_out",
    name: "NATS Out",
    kind: "sink",
    executionMode: "native",
    streamCompatibility: ["work", "retry", "replay"],
    deliveryGuarantees: ["idempotent", "append_only"],
  },
  {
    id: "snowflake_sink",
    name: "Snowflake Sink",
    kind: "sink",
    executionMode: "adapter",
    streamCompatibility: ["work", "retry", "dlq"],
    deliveryGuarantees: ["idempotent", "append_only"],
    adapterManaged: true,
  },
  {
    id: "bigquery_sink",
    name: "BigQuery Sink",
    kind: "sink",
    executionMode: "adapter",
    streamCompatibility: ["work", "retry", "dlq"],
    deliveryGuarantees: ["append_only", "best_effort"],
    adapterManaged: true,
  },
  {
    id: "s3_sink",
    name: "S3 Sink",
    kind: "sink",
    executionMode: "adapter",
    streamCompatibility: ["work", "retry", "dlq"],
    deliveryGuarantees: ["append_only", "best_effort"],
    adapterManaged: true,
  },
  {
    id: "kafka_in",
    name: "Kafka In",
    kind: "source",
    executionMode: "adapter",
    streamCompatibility: ["ingress"],
    deliveryGuarantees: [...ALL_GUARANTEES],
    adapterManaged: true,
  },
  {
    id: "kafka_out",
    name: "Kafka Out",
    kind: "sink",
    executionMode: "adapter",
    streamCompatibility: ["work", "retry", "replay"],
    deliveryGuarantees: ["append_only", "best_effort"],
    adapterManaged: true,
  },
];

export function findCapability(capabilityId: string): ConnectorCapability | undefined {
  return CONNECTOR_CAPABILITIES.find((capability) => capability.id === capabilityId);
}
