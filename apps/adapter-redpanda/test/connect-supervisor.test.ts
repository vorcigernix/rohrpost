import { describe, expect, it } from "bun:test";
import type { FlowSpec } from "@rohrpost/shared-flow-spec";
import { buildKubernetesWorkloadResources, collectDesiredConnectWorkloads } from "../src/connect-supervisor";
import type { AdapterRedpandaConfig } from "../src/config";

function buildConfig(): AdapterRedpandaConfig {
  return {
    host: "127.0.0.1",
    port: 3003,
    redpandaConnectImage: "docker.redpanda.com/redpandadata/connect:latest",
    manifestSource: "test",
    connectBackend: "docker",
    connectPollIntervalMs: 5_000,
    connectDockerBinary: "docker",
    connectWorkdir: "/tmp/redpanda-connect",
    connectKubernetesNamespace: "rohrpost",
    connectKubernetesServiceAccountName: "adapter-redpanda-connect",
    connectKubernetesConfigMapPrefix: "rpc-config",
    connectKubernetesDeploymentPrefix: "rpc-workload",
    connectKubernetesImagePullPolicy: "IfNotPresent",
    connectKubernetesArtifactVolumeClaimName: "adapter-artifacts-pvc",
    serviceName: "adapter-redpanda",
    natsUrl: "nats://127.0.0.1:4222",
    controlApiUrl: "http://127.0.0.1:3001",
    controlApiToken: "dev-admin-token",
    deliveryLogPath: "/tmp/adapter-deliveries.jsonl",
    deliveryLogEnabled: true,
    artifactRoot: "/tmp/adapter-artifacts",
  };
}

function buildDeployment(input: {
  deploymentId?: string;
  flowId?: string;
  revisionId?: string;
  sourceCapabilityId?: "http_in" | "kafka_in";
  sourceConnectorId?: string;
  sourceConfig?: Record<string, unknown>;
  sinkCapabilityId?: "s3_sink" | "kafka_out";
  sinkConnectorId?: string;
  sinkConfig?: Record<string, unknown>;
}) {
  const sourceCapabilityId = input.sourceCapabilityId ?? "http_in";
  const sourceConnectorId = input.sourceConnectorId ?? "http_in_default";
  const sinkCapabilityId = input.sinkCapabilityId;
  const sinkConnectorId = input.sinkConnectorId;
  const flowId = input.flowId ?? "flow_demo";
  const revisionId = input.revisionId ?? "rev_demo_v1";

  const spec: FlowSpec = {
    version: 1,
    metadata: {
      tenantId: "tenant_demo",
      flowId,
      revisionId,
      name: "Adapter Demo",
    },
    sources: [
      {
        id: "source-ingest",
        kind: sourceCapabilityId === "kafka_in" ? "kafka" : "http",
        connector: {
          capabilityId: sourceCapabilityId,
          connectorId: sourceConnectorId,
          executionMode: sourceCapabilityId === "kafka_in" ? "adapter" : "native",
        },
        stream: "ingress",
        nextNodeIds: [],
      },
    ],
    processors: [],
    routes: [],
    sinks: sinkCapabilityId && sinkConnectorId
      ? [
          {
            id: "sink-adapter",
            kind: sinkCapabilityId === "s3_sink" ? "s3" : "kafka",
            connector: {
              capabilityId: sinkCapabilityId,
              connectorId: sinkConnectorId,
              executionMode: "adapter",
            },
            deliveryGuarantee: "append_only",
            stream: "work",
          },
        ]
      : [],
    retryPolicy: {
      maxAttempts: 1,
      initialBackoffMs: 25,
      maxBackoffMs: 250,
      multiplier: 2,
    },
    dlqPolicy: {
      enabled: false,
    },
    idempotencyStrategy: "message_id",
  };

  const connectors: Record<string, {
    id: string;
    capabilityId: string;
    executionMode: "native" | "adapter";
    config: Record<string, unknown>;
  }> = {
    [sourceConnectorId]: {
      id: sourceConnectorId,
      capabilityId: sourceCapabilityId,
      executionMode: sourceCapabilityId === "kafka_in" ? "adapter" : "native",
      config: input.sourceConfig ?? {},
    },
  };

  if (sinkCapabilityId && sinkConnectorId) {
    connectors[sinkConnectorId] = {
      id: sinkConnectorId,
      capabilityId: sinkCapabilityId,
      executionMode: "adapter",
      config: input.sinkConfig ?? {},
    };
  }

  return {
    deployment: {
      id: input.deploymentId ?? "deploy-1",
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      status: "active",
      rolloutStatus: "activated",
    },
    revision: {
      id: spec.metadata.revisionId,
      spec,
    },
    connectors,
  };
}

describe("collectDesiredConnectWorkloads", () => {
  it("creates connect-managed workloads for kafka sources and s3 sinks, while skipping inline adapter sinks", () => {
    const workloads = collectDesiredConnectWorkloads(
      {
        generatedAt: "2026-04-07T08:00:00.000Z",
        deployments: [
          buildDeployment({
            deploymentId: "deploy-kafka-source",
            sourceCapabilityId: "kafka_in",
            sourceConnectorId: "kafka_in_orders",
            sourceConfig: {
              topic: "orders.raw",
              brokers: ["localhost:19092"],
            },
          }),
          buildDeployment({
            deploymentId: "deploy-s3-sink",
            sinkCapabilityId: "s3_sink",
            sinkConnectorId: "s3_sink_default",
            sinkConfig: {
              bucket: "event-router-v1",
              prefix: "exports/",
            },
          }),
          buildDeployment({
            deploymentId: "deploy-inline-kafka-out",
            sinkCapabilityId: "kafka_out",
            sinkConnectorId: "kafka_out_default",
            sinkConfig: {
              topic: "orders.created",
            },
          }),
        ],
      },
      buildConfig(),
    );

    expect(workloads).toHaveLength(2);

    const kafkaWorkload = workloads.find((workload) => workload.connectorId === "kafka_in_orders");
    expect(kafkaWorkload).toBeDefined();
    expect(kafkaWorkload?.runtimeRole).toBe("source");
    expect(kafkaWorkload?.manifestId).toBe("kafka-source");
    expect(kafkaWorkload?.deploymentIds).toEqual(["deploy-kafka-source"]);
    expect(kafkaWorkload?.flowIds).toEqual(["flow_demo"]);
    expect(kafkaWorkload?.revisionIds).toEqual(["rev_demo_v1"]);
    expect(kafkaWorkload?.targetKind).toBe("nats_jetstream");
    expect(kafkaWorkload?.inputRef).toBe("kafka://host.docker.internal:19092/orders.raw");
    expect(kafkaWorkload?.outputRef).toContain(
      "router.ingress.tenant_demo.flow_demo.rev_demo_v1.${!json(\"messageId\")}",
    );
    expect(kafkaWorkload?.consumerRef).toBe("rohrpost_deploy-kafka-source");
    expect(kafkaWorkload?.connectConfig.input).toEqual({
      kafka: expect.objectContaining({
        addresses: ["host.docker.internal:19092"],
        topics: ["orders.raw"],
        consumer_group: "rohrpost_deploy-kafka-source",
      }),
    });
    expect(kafkaWorkload?.connectConfig.output).toEqual({
      nats_jetstream: expect.objectContaining({
        subject: "router.ingress.tenant_demo.flow_demo.rev_demo_v1.${!json(\"messageId\")}",
      }),
    });

    const s3Workload = workloads.find((workload) => workload.connectorId === "s3_sink_default");
    expect(s3Workload?.runtimeRole).toBe("sink");
    expect(s3Workload?.deploymentIds).toEqual(["deploy-s3-sink"]);
    expect(s3Workload?.flowIds).toEqual(["flow_demo"]);
    expect(s3Workload?.revisionIds).toEqual(["rev_demo_v1"]);
    expect(s3Workload?.inputRef).toBe("router.work.connect.s3_sink_default.>");
    expect(s3Workload?.targetKind).toBe("file");
    expect(s3Workload?.outputRef).toContain("file://");
  });

  it("creates a kafka source workload per deployment even when the connector id is shared", () => {
    const workloads = collectDesiredConnectWorkloads(
      {
        generatedAt: "2026-04-07T08:00:00.000Z",
        deployments: [
          buildDeployment({
            deploymentId: "deploy-a",
            flowId: "flow_a",
            revisionId: "rev_a_v1",
            sourceCapabilityId: "kafka_in",
            sourceConnectorId: "kafka_in_shared",
            sourceConfig: { topic: "orders.shared" },
          }),
          buildDeployment({
            deploymentId: "deploy-b",
            flowId: "flow_b",
            revisionId: "rev_b_v1",
            sourceCapabilityId: "kafka_in",
            sourceConnectorId: "kafka_in_shared",
            sourceConfig: { topic: "orders.shared" },
          }),
        ],
      },
      buildConfig(),
    );

    expect(workloads).toHaveLength(2);
    expect(workloads.map((workload) => workload.key)).toEqual([
      "source:deploy-a:kafka_in_shared",
      "source:deploy-b:kafka_in_shared",
    ]);
    expect(workloads.map((workload) => workload.outputRef)).toEqual([
      "nats+js://nats://host.docker.internal:4222/router.ingress.tenant_demo.flow_a.rev_a_v1.${!json(\"messageId\")}",
      "nats+js://nats://host.docker.internal:4222/router.ingress.tenant_demo.flow_b.rev_b_v1.${!json(\"messageId\")}",
    ]);
  });

  it("tracks every active deployment that shares a connect-managed sink connector", () => {
    const workloads = collectDesiredConnectWorkloads(
      {
        generatedAt: "2026-04-07T08:00:00.000Z",
        deployments: [
          buildDeployment({
            deploymentId: "deploy-a",
            flowId: "flow_a",
            revisionId: "rev_a_v1",
            sinkCapabilityId: "s3_sink",
            sinkConnectorId: "s3_sink_shared",
          }),
          buildDeployment({
            deploymentId: "deploy-b",
            flowId: "flow_b",
            revisionId: "rev_b_v1",
            sinkCapabilityId: "s3_sink",
            sinkConnectorId: "s3_sink_shared",
          }),
        ],
      },
      buildConfig(),
    );

    expect(workloads).toHaveLength(1);
    expect(workloads[0]?.key).toBe("sink:s3_sink_shared");
    expect(workloads[0]?.deploymentIds).toEqual(["deploy-a", "deploy-b"]);
    expect(workloads[0]?.flowIds).toEqual(["flow_a", "flow_b"]);
    expect(workloads[0]?.revisionIds).toEqual(["rev_a_v1", "rev_b_v1"]);
  });

  it("uses aws_s3 output when an endpoint or credentials are configured", () => {
    const workloads = collectDesiredConnectWorkloads(
      {
        generatedAt: "2026-04-07T08:00:00.000Z",
        deployments: [
          buildDeployment({
            deploymentId: "deploy-r2",
            sinkCapabilityId: "s3_sink",
            sinkConnectorId: "s3_sink_r2",
            sinkConfig: {
              bucket: "router-r2",
              prefix: "events/",
              endpoint: "https://example.r2.cloudflarestorage.com",
              region: "auto",
              accessKeyId: "id",
              secretAccessKey: "secret",
              forcePathStyleUrls: true,
            },
          }),
        ],
      },
      buildConfig(),
    );

    expect(workloads).toHaveLength(1);
    expect(workloads[0]?.targetKind).toBe("aws_s3");
    expect(workloads[0]?.outputRef).toBe("s3://router-r2/events/");
    expect(workloads[0]?.connectConfig.output).toEqual({
      aws_s3: expect.objectContaining({
        bucket: "router-r2",
        endpoint: "https://example.r2.cloudflarestorage.com",
        region: "auto",
        force_path_style_urls: true,
      }),
    });
  });

  it("builds Kubernetes ConfigMap and Deployment resources for connect workloads", () => {
    const config = { ...buildConfig(), connectBackend: "kubernetes" as const };
    const [workload] = collectDesiredConnectWorkloads(
      {
        generatedAt: "2026-04-07T08:00:00.000Z",
        deployments: [
          buildDeployment({
            deploymentId: "deploy-s3-k8s",
            sinkCapabilityId: "s3_sink",
            sinkConnectorId: "s3_sink_default",
            sinkConfig: {
              bucket: "event-router-v1",
              prefix: "exports/",
            },
          }),
        ],
      },
      config,
    );

    expect(workload).toBeDefined();
    const resources = buildKubernetesWorkloadResources(workload!, config);
    expect(resources.configMapName).toStartWith("rpc-config-sink-s3-sink-default");
    expect(resources.deploymentName).toStartWith("rpc-workload-sink-s3-sink-default");
    expect(resources.configMap).toMatchObject({
      kind: "ConfigMap",
      metadata: {
        namespace: "rohrpost",
        annotations: {
          "rohrpost.dev/deployment-ids": "deploy-s3-k8s",
          "rohrpost.dev/flow-ids": "flow_demo",
          "rohrpost.dev/revision-ids": "rev_demo_v1",
        },
        labels: {
          "app.kubernetes.io/component": "adapter-workload",
          "rohrpost.dev/capability-id": "s3_sink",
        },
      },
    });
    expect(resources.deployment).toMatchObject({
      kind: "Deployment",
      metadata: {
        namespace: "rohrpost",
      },
      spec: {
        template: {
          spec: {
            serviceAccountName: "adapter-redpanda-connect",
            containers: [
              {
                name: "redpanda-connect",
                image: "docker.redpanda.com/redpandadata/connect:latest",
                command: ["/redpanda-connect", "run", "/etc/redpanda-connect/connect.json"],
              },
            ],
          },
        },
      },
    });
    const podSpec = resources.deployment.spec as {
      template?: {
        spec?: {
          volumes?: Array<{ name?: string; persistentVolumeClaim?: { claimName?: string } }>;
        };
      };
    };
    expect(podSpec.template?.spec?.volumes).toContainEqual(
      expect.objectContaining({
        name: "artifacts",
        persistentVolumeClaim: {
          claimName: "adapter-artifacts-pvc",
        },
      }),
    );
  });

  it("preserves cluster-local addresses when Kubernetes is the connect backend", () => {
    const config = {
      ...buildConfig(),
      connectBackend: "kubernetes" as const,
      natsUrl: "nats://nats:4222",
    };
    const workloads = collectDesiredConnectWorkloads(
      {
        generatedAt: "2026-04-07T08:00:00.000Z",
        deployments: [
          buildDeployment({
            deploymentId: "deploy-kafka-k8s",
            sourceCapabilityId: "kafka_in",
            sourceConnectorId: "kafka_in_orders",
            sourceConfig: {
              topic: "orders.raw",
              brokers: ["redpanda:9092"],
            },
          }),
        ],
      },
      config,
    );

    expect(workloads[0]?.inputRef).toBe("kafka://redpanda:9092/orders.raw");
    expect(workloads[0]?.outputRef).toBe(
      "nats+js://nats://nats:4222/router.ingress.tenant_demo.flow_demo.rev_demo_v1.${!json(\"messageId\")}",
    );
    expect(workloads[0]?.connectConfig.input).toEqual({
      kafka: expect.objectContaining({
        addresses: ["redpanda:9092"],
      }),
    });
  });
});
