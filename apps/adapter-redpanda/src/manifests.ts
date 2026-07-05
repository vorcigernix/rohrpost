export type ConnectorExecutionMode = "adapter";

export interface AdapterCatalogCapability {
  connectorId: string;
  connectorType: string;
  executionMode: ConnectorExecutionMode;
  supportedDirections: Array<"source" | "sink" | "bidirectional">;
  protocol: string;
  notes: string;
}

export interface ConnectorManifest {
  id: string;
  connectorId: string;
  executionMode: ConnectorExecutionMode;
  image: string;
  description: string;
  defaultEnv: Record<string, string>;
}

export const ADAPTER_CONNECTOR_CAPABILITIES: AdapterCatalogCapability[] = [
  {
    connectorId: "kafka",
    connectorType: "commodity-stream",
    executionMode: "adapter",
    supportedDirections: ["source", "sink"],
    protocol: "Kafka",
    notes: "Kafka is routed through the Redpanda Connect adapter path in v1.",
  },
  {
    connectorId: "snowflake",
    connectorType: "warehouse",
    executionMode: "adapter",
    supportedDirections: ["sink"],
    protocol: "Snowflake",
    notes: "Warehouse rows are materialized by the adapter runtime into local Snowflake-shaped artifacts in v1.",
  },
  {
    connectorId: "bigquery",
    connectorType: "warehouse",
    executionMode: "adapter",
    supportedDirections: ["sink"],
    protocol: "BigQuery",
    notes: "Warehouse rows are materialized by the adapter runtime into local BigQuery-shaped artifacts in v1.",
  },
  {
    connectorId: "s3",
    connectorType: "object-storage",
    executionMode: "adapter",
    supportedDirections: ["sink"],
    protocol: "S3",
    notes: "Object outputs are materialized by the adapter runtime into local S3-shaped artifacts in v1.",
  },
];

export function buildConnectorManifests(image: string): ConnectorManifest[] {
  return [
    {
      id: "kafka-source",
      connectorId: "kafka",
      executionMode: "adapter",
      image,
      description: "Adapter-managed Kafka source workload.",
      defaultEnv: {
        REDPANDA_CONNECT_MODE: "source",
        CONNECTOR_KIND: "kafka",
      },
    },
    {
      id: "kafka-sink",
      connectorId: "kafka",
      executionMode: "adapter",
      image,
      description: "Adapter-managed Kafka sink workload.",
      defaultEnv: {
        REDPANDA_CONNECT_MODE: "sink",
        CONNECTOR_KIND: "kafka",
      },
    },
    {
      id: "snowflake-sink",
      connectorId: "snowflake",
      executionMode: "adapter",
      image,
      description: "Adapter-managed Snowflake sink workload.",
      defaultEnv: {
        REDPANDA_CONNECT_MODE: "sink",
        CONNECTOR_KIND: "snowflake",
      },
    },
    {
      id: "bigquery-sink",
      connectorId: "bigquery",
      executionMode: "adapter",
      image,
      description: "Adapter-managed BigQuery sink workload.",
      defaultEnv: {
        REDPANDA_CONNECT_MODE: "sink",
        CONNECTOR_KIND: "bigquery",
      },
    },
    {
      id: "s3-sink",
      connectorId: "s3",
      executionMode: "adapter",
      image,
      description: "Adapter-managed S3 sink workload.",
      defaultEnv: {
        REDPANDA_CONNECT_MODE: "sink",
        CONNECTOR_KIND: "s3",
      },
    },
  ];
}

export function findManifest(
  manifests: ConnectorManifest[],
  connectorId: string,
): ConnectorManifest | undefined {
  return manifests.find((manifest) => manifest.id === connectorId);
}

const CAPABILITY_MANIFEST_IDS: Record<string, string> = {
  kafka_in: "kafka-source",
  kafka_out: "kafka-sink",
  snowflake_sink: "snowflake-sink",
  bigquery_sink: "bigquery-sink",
  s3_sink: "s3-sink",
};

export function findManifestForConnectorRef(
  manifests: ConnectorManifest[],
  connectorRef: string,
): ConnectorManifest | undefined {
  return findManifest(manifests, connectorRef)
    ?? (connectorRef in CAPABILITY_MANIFEST_IDS
      ? findManifest(manifests, CAPABILITY_MANIFEST_IDS[connectorRef])
      : undefined);
}
