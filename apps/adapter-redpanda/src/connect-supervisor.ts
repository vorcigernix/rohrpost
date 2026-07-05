import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import {
  buildAdapterWorkSubjectPattern,
  createControlApiClient,
  isConnectManagedAdapterCapability,
  type ControlApiConnectorRecord,
  type ControlApiRuntimeDeploymentRecord,
  type AdapterWorkloadStatusReport,
} from "@rohrpost/control-api-contracts";
import type { AdapterRedpandaConfig } from "./config";

type SupervisorBackend = "docker" | "kubernetes" | "disabled";
type KubernetesResource = Record<string, unknown>;
type ConnectSupervisorConnectorRecord = Pick<ControlApiConnectorRecord, "id" | "capabilityId" | "executionMode" | "config">;
type ConnectSupervisorDeploymentRecord = {
  deployment: Pick<ControlApiRuntimeDeploymentRecord["deployment"], "id" | "flowId" | "revisionId" | "status" | "rolloutStatus">;
  revision: Pick<ControlApiRuntimeDeploymentRecord["revision"], "id" | "spec">;
  connectors: Record<string, ConnectSupervisorConnectorRecord>;
};
type ConnectSupervisorDeploymentsResponse = {
  generatedAt: string;
  deployments: ConnectSupervisorDeploymentRecord[];
};

export interface ConnectWorkloadSpec {
  key: string;
  connectorId: string;
  capabilityId: string;
  manifestId: string;
  deploymentIds: string[];
  flowIds: string[];
  revisionIds: string[];
  runtimeRole: "source" | "sink";
  inputRef: string;
  outputRef: string;
  consumerRef?: string;
  configPath: string;
  configHash: string;
  targetKind: "aws_s3" | "file" | "nats_jetstream";
  artifactPath?: string;
  containerName: string;
  dockerCommand: string[];
  connectConfig: Record<string, unknown>;
}

export interface ConnectWorkloadStatus extends AdapterWorkloadStatusReport {
  key: string;
  connectorId: string;
  capabilityId: string;
  manifestId: string;
  deploymentIds: string[];
  flowIds: string[];
  revisionIds: string[];
  runtimeRole: "source" | "sink";
  inputRef: string;
  outputRef: string;
  status: "starting" | "running" | "stopped" | "degraded";
  backend: SupervisorBackend;
  consumerRef?: string;
  targetKind: "aws_s3" | "file" | "nats_jetstream";
  artifactPath?: string;
  configPath: string;
  containerName?: string;
  startedAt?: string;
  stoppedAt?: string;
  lastError?: string;
  restartCount: number;
  recentLogs: string[];
}

export interface ConnectSupervisorSummary {
  backend: SupervisorBackend;
  enabled: boolean;
  managedWorkloads: number;
  runningWorkloads: number;
  lastRefreshAt?: string;
  lastRefreshError?: string;
  lastReportAt?: string;
  lastReportError?: string;
}

interface RunningWorkload {
  spec: ConnectWorkloadSpec;
  status: ConnectWorkloadStatus;
  process?: ChildProcess;
  kubernetesResources?: KubernetesWorkloadResources;
}

interface KubernetesWorkloadResources {
  configMapName: string;
  deploymentName: string;
  configMap: KubernetesResource;
  deployment: KubernetesResource;
}

const KUBERNETES_SERVICE_ACCOUNT_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";
const KUBERNETES_TOKEN_PATH = `${KUBERNETES_SERVICE_ACCOUNT_DIR}/token`;
const KUBERNETES_CA_PATH = `${KUBERNETES_SERVICE_ACCOUNT_DIR}/ca.crt`;

function isoNow(): string {
  return new Date().toISOString();
}

function fileToken(value: string): string {
  return value
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 48) || "item";
}

function stableHash(value: unknown): string {
  return Bun.hash(JSON.stringify(value)).toString(16);
}

function shortHash(value: unknown): string {
  return stableHash(value).replace(/[^a-zA-Z0-9]+/g, "").toLowerCase().slice(0, 12) || "0";
}

function kubernetesName(...parts: string[]): string {
  const name = parts
    .join("-")
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");

  return name.slice(0, 63).replace(/-+$/g, "") || "redpanda-connect";
}

function kubernetesLabelValue(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9_.-]+/g, "-")
    .replace(/^[^a-z0-9]+|[^a-z0-9]+$/g, "")
    .slice(0, 63) || "unknown";
}

function connectReachableHost(runtimeConfig: AdapterRedpandaConfig, value: string): string {
  if (
    runtimeConfig.connectBackend === "docker" || runtimeConfig.connectBackend === "auto"
  ) {
    return dockerReachableHost(value);
  }

  return value;
}

function connectReachableNatsUrl(runtimeConfig: AdapterRedpandaConfig, natsUrl: string): string {
  if (
    runtimeConfig.connectBackend === "docker" || runtimeConfig.connectBackend === "auto"
  ) {
    return dockerReachableNatsUrl(natsUrl);
  }

  return natsUrl;
}

function connectReachableKafkaAddresses(runtimeConfig: AdapterRedpandaConfig, addresses: string[]): string[] {
  return addresses.map((address) => {
    const [host, ...rest] = address.split(":");
    if (!host || rest.length === 0) {
      return address;
    }

    return [connectReachableHost(runtimeConfig, host), ...rest].join(":");
  });
}

function pushLogLine(status: ConnectWorkloadStatus, line: string): void {
  status.recentLogs.unshift(line);
  status.recentLogs.splice(20);
}

function dockerReachableHost(value: string): string {
  return value === "127.0.0.1" || value === "localhost"
    ? "host.docker.internal"
    : value;
}

function dockerReachableNatsUrl(natsUrl: string): string {
  try {
    const parsed = new URL(natsUrl);
    if (parsed.hostname === "127.0.0.1" || parsed.hostname === "localhost") {
      parsed.hostname = dockerReachableHost(parsed.hostname);
      return parsed.toString();
    }
  } catch {
    return natsUrl;
  }

  return natsUrl;
}

function readString(config: Record<string, unknown>, key: string): string | undefined {
  const value = config[key];
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function readStringArray(config: Record<string, unknown>, ...keys: string[]): string[] | undefined {
  for (const key of keys) {
    const value = config[key];
    if (Array.isArray(value)) {
      const items = value.filter((item): item is string => typeof item === "string" && item.trim().length > 0);
      if (items.length > 0) {
        return items.map((item) => item.trim());
      }
    }

    if (typeof value === "string" && value.trim().length > 0) {
      const items = value
        .split(",")
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
      if (items.length > 0) {
        return items;
      }
    }
  }

  return undefined;
}

function readBoolean(config: Record<string, unknown>, key: string): boolean | undefined {
  const value = config[key];
  return typeof value === "boolean" ? value : undefined;
}

function appendUnique(values: string[], value: string): void {
  if (!values.includes(value)) {
    values.push(value);
  }
}

function deploymentIdentity(deploymentRecord: ConnectSupervisorDeploymentRecord): Pick<
  ConnectWorkloadSpec,
  "deploymentIds" | "flowIds" | "revisionIds"
> {
  return {
    deploymentIds: [deploymentRecord.deployment.id],
    flowIds: [deploymentRecord.deployment.flowId],
    revisionIds: [deploymentRecord.deployment.revisionId],
  };
}

function attachDeploymentIdentity(
  spec: Pick<ConnectWorkloadSpec, "deploymentIds" | "flowIds" | "revisionIds">,
  deploymentRecord: ConnectSupervisorDeploymentRecord,
): void {
  appendUnique(spec.deploymentIds, deploymentRecord.deployment.id);
  appendUnique(spec.flowIds, deploymentRecord.deployment.flowId);
  appendUnique(spec.revisionIds, deploymentRecord.deployment.revisionId);
}

function dockerReachableKafkaAddresses(addresses: string[]): string[] {
  return addresses.map((address) => {
    const [host, ...rest] = address.split(":");
    if (!host || rest.length === 0) {
      return address;
    }

    return [dockerReachableHost(host), ...rest].join(":");
  });
}

function buildDeploymentIngressSubjectPattern(deployment: ConnectSupervisorDeploymentRecord): string {
  return [
    "router",
    "ingress",
    deployment.revision.spec.metadata.tenantId,
    deployment.deployment.flowId,
    deployment.deployment.revisionId,
    "${!json(\"messageId\")}",
  ].join(".");
}

function buildS3ConnectConfig(
  runtimeConfig: AdapterRedpandaConfig,
  connector: ConnectSupervisorConnectorRecord,
  artifactRoot: string,
): Pick<ConnectWorkloadSpec, "targetKind" | "outputRef" | "consumerRef" | "artifactPath" | "connectConfig"> {
  const bucket = readString(connector.config, "bucket") ?? "event-router-v1";
  const prefix = readString(connector.config, "prefix") ?? "events/";
  const region = readString(connector.config, "region");
  const endpoint = readString(connector.config, "endpoint");
  const accessKeyId = readString(connector.config, "accessKeyId");
  const secretAccessKey = readString(connector.config, "secretAccessKey");
  const sessionToken = readString(connector.config, "sessionToken");
  const forcePathStyleUrls = readBoolean(connector.config, "forcePathStyleUrls");
  const objectPath = `${prefix}\${!json("tenantId")}/\${!json("flowId")}/\${!json("revisionId")}/\${!json("messageId")}.json`;
  const usesAwsS3 = Boolean(region || endpoint || accessKeyId || secretAccessKey || sessionToken);

  const input = {
    nats_jetstream: {
      urls: [connectReachableNatsUrl(runtimeConfig, runtimeConfig.natsUrl ?? "nats://127.0.0.1:4222")],
      stream: "work",
      subject: buildAdapterWorkSubjectPattern("connect", connector.id),
      durable: `rpc_${fileToken(connector.id)}`,
      deliver: "all",
      ack_wait: "30s",
      max_ack_pending: 1024,
    },
  };

  if (usesAwsS3) {
    return {
      targetKind: "aws_s3",
      outputRef: `s3://${bucket}/${prefix}`,
      consumerRef: `rpc_${fileToken(connector.id)}`,
      connectConfig: {
        input,
        output: {
          aws_s3: {
            bucket,
            path: objectPath,
            content_type: "application/json",
            ...(region ? { region } : {}),
            ...(endpoint ? { endpoint } : {}),
            ...(typeof forcePathStyleUrls === "boolean" ? { force_path_style_urls: forcePathStyleUrls } : {}),
            credentials: {
              ...(accessKeyId ? { id: accessKeyId } : {}),
              ...(secretAccessKey ? { secret: secretAccessKey } : {}),
              ...(sessionToken ? { token: sessionToken } : {}),
            },
          },
        },
      },
    };
  }

  const artifactPath = resolve(artifactRoot, "s3", fileToken(connector.id), "events.ndjson");
  const containerArtifactPath = `/artifacts/s3/${fileToken(connector.id)}/events.ndjson`;
  return {
    targetKind: "file",
    outputRef: `file://${artifactPath}`,
    artifactPath,
    consumerRef: `rpc_${fileToken(connector.id)}`,
    connectConfig: {
      input,
      output: {
        file: {
          path: containerArtifactPath,
          codec: "lines",
        },
      },
    },
  };
}

function buildKafkaSourceConnectConfig(
  runtimeConfig: AdapterRedpandaConfig,
  deploymentRecord: ConnectSupervisorDeploymentRecord,
  connector: ConnectSupervisorConnectorRecord,
): Pick<ConnectWorkloadSpec, "targetKind" | "outputRef" | "consumerRef" | "connectConfig"> | null {
  const topic = readString(connector.config, "topic");
  if (!topic) {
    return null;
  }

  const addresses = connectReachableKafkaAddresses(runtimeConfig,
    readStringArray(connector.config, "addresses", "brokers", "bootstrapServers", "bootstrap_servers")
      ?? ["host.docker.internal:9092"],
  );
  const consumerGroup = readString(connector.config, "consumerGroup")
    ?? `rohrpost_${fileToken(deploymentRecord.deployment.id)}`;
  const clientId = readString(connector.config, "clientId")
    ?? `rohrpost-${fileToken(deploymentRecord.deployment.id)}`;
  const ingressSubject = buildDeploymentIngressSubjectPattern(deploymentRecord);
  const natsUrl = connectReachableNatsUrl(runtimeConfig, runtimeConfig.natsUrl ?? "nats://127.0.0.1:4222");
  const tenantId = deploymentRecord.revision.spec.metadata.tenantId;
  const flowId = deploymentRecord.deployment.flowId;
  const revisionId = deploymentRecord.deployment.revisionId;

  return {
    targetKind: "nats_jetstream",
    outputRef: `nats+js://${natsUrl}/${ingressSubject}`,
    consumerRef: consumerGroup,
    connectConfig: {
      input: {
        kafka: {
          addresses,
          topics: [topic],
          consumer_group: consumerGroup,
          client_id: clientId,
        },
      },
      pipeline: {
        processors: [
          {
            mapping: [
              "root = {}",
              `root.tenantId = "${tenantId}"`,
              `root.flowId = "${flowId}"`,
              `root.revisionId = "${revisionId}"`,
              "root.messageId = uuid_v7()",
              "root.sourceRef = meta(\"kafka_topic\")",
              `root.partitionKey = meta(\"kafka_key\").catch("${tenantId}")`,
              "root.headers = {}",
              "root.payload = content().string().parse_json().catch(content().string())",
              "root.receivedAt = now()",
              `root.traceId = "${flowId}:" + root.messageId`,
            ].join("\n"),
          },
        ],
      },
      output: {
        nats_jetstream: {
          urls: [natsUrl],
          subject: ingressSubject,
        },
      },
    },
  };
}

export function collectDesiredConnectWorkloads(
  response: ConnectSupervisorDeploymentsResponse,
  runtimeConfig: AdapterRedpandaConfig,
): ConnectWorkloadSpec[] {
  const workdir = resolve(runtimeConfig.connectWorkdir);
  const specs = new Map<string, ConnectWorkloadSpec>();

  for (const deploymentRecord of response.deployments) {
    if (deploymentRecord.deployment.status !== "active") {
      continue;
    }

    for (const source of deploymentRecord.revision.spec.sources) {
      const connector = deploymentRecord.connectors[source.connector.connectorId];
      if (!connector || connector.executionMode !== "adapter") {
        continue;
      }

      if (!isConnectManagedAdapterCapability(connector.capabilityId)) {
        continue;
      }

      if (connector.capabilityId !== "kafka_in") {
        continue;
      }

      const manifestId = "kafka-source";
      const workloadKey = `source:${deploymentRecord.deployment.id}:${connector.id}`;
      if (specs.has(workloadKey)) {
        continue;
      }

      const configDir = resolve(
        workdir,
        "configs",
        fileToken(deploymentRecord.deployment.id),
        fileToken(connector.id),
      );
      const configPath = resolve(configDir, "connect.json");
      const workloadConfig = buildKafkaSourceConnectConfig(runtimeConfig, deploymentRecord, connector);
      if (!workloadConfig) {
        continue;
      }

      const containerName = `rohrpost-rpc-${fileToken(deploymentRecord.deployment.id)}-${fileToken(connector.id)}`.slice(0, 63);
      const dockerCommand = [
        runtimeConfig.connectDockerBinary,
        "run",
        "--rm",
        "--name",
        containerName,
        "-v",
        `${configDir}:/workspace/connect:ro`,
        runtimeConfig.redpandaConnectImage,
        "run",
        "/workspace/connect/connect.json",
      ];

      specs.set(workloadKey, {
        key: workloadKey,
        connectorId: connector.id,
        capabilityId: connector.capabilityId,
        manifestId,
        ...deploymentIdentity(deploymentRecord),
        runtimeRole: "source",
        inputRef: `kafka://${connectReachableKafkaAddresses(runtimeConfig,
          readStringArray(connector.config, "addresses", "brokers", "bootstrapServers", "bootstrap_servers")
            ?? ["host.docker.internal:9092"],
        ).join(",")}/${readString(connector.config, "topic")}`,
        outputRef: workloadConfig.outputRef,
        consumerRef: workloadConfig.consumerRef,
        configPath,
        configHash: stableHash(workloadConfig.connectConfig),
        targetKind: workloadConfig.targetKind,
        containerName,
        dockerCommand,
        connectConfig: workloadConfig.connectConfig,
      });
    }

    for (const sink of deploymentRecord.revision.spec.sinks) {
      const connector = deploymentRecord.connectors[sink.connector.connectorId];
      if (!connector || connector.executionMode !== "adapter") {
        continue;
      }

      if (!isConnectManagedAdapterCapability(connector.capabilityId)) {
        continue;
      }

      const workloadKey = `sink:${connector.id}`;
      const existingSpec = specs.get(workloadKey);
      if (existingSpec) {
        attachDeploymentIdentity(existingSpec, deploymentRecord);
        continue;
      }

      const manifestId = connector.capabilityId === "s3_sink" ? "s3-sink" : connector.capabilityId;
      const configDir = resolve(workdir, "configs", fileToken(connector.id));
      const configPath = resolve(configDir, "connect.json");
      const subjectPattern = buildAdapterWorkSubjectPattern("connect", connector.id);

      let workloadConfig: Pick<
        ConnectWorkloadSpec,
        "targetKind" | "outputRef" | "consumerRef" | "artifactPath" | "connectConfig"
      >;
      switch (connector.capabilityId) {
        case "s3_sink":
          workloadConfig = buildS3ConnectConfig(runtimeConfig, connector, resolve(runtimeConfig.artifactRoot));
          break;
        default:
          continue;
      }

      const containerName = `rohrpost-rpc-${fileToken(connector.id)}`.slice(0, 63);
      const dockerCommand = [
        runtimeConfig.connectDockerBinary,
        "run",
        "--rm",
        "--name",
        containerName,
        "-v",
        `${configDir}:/workspace/connect:ro`,
        "-v",
        `${resolve(runtimeConfig.artifactRoot)}:/artifacts`,
        runtimeConfig.redpandaConnectImage,
        "run",
        "/workspace/connect/connect.json",
      ];

      const connectConfig = workloadConfig.connectConfig;

      specs.set(workloadKey, {
        key: workloadKey,
        connectorId: connector.id,
        capabilityId: connector.capabilityId,
        manifestId,
        ...deploymentIdentity(deploymentRecord),
        runtimeRole: "sink",
        inputRef: subjectPattern,
        outputRef: workloadConfig.outputRef,
        consumerRef: workloadConfig.consumerRef,
        configPath,
        configHash: stableHash(connectConfig),
        targetKind: workloadConfig.targetKind,
        artifactPath: workloadConfig.artifactPath,
        containerName,
        dockerCommand,
        connectConfig,
      });
    }
  }

  return [...specs.values()];
}

export function buildKubernetesWorkloadResources(
  spec: ConnectWorkloadSpec,
  runtimeConfig: AdapterRedpandaConfig,
): KubernetesWorkloadResources {
  const hash = shortHash({ key: spec.key, configHash: spec.configHash });
  const configMapName = kubernetesName(runtimeConfig.connectKubernetesConfigMapPrefix, spec.runtimeRole, spec.connectorId, hash);
  const deploymentName = kubernetesName(runtimeConfig.connectKubernetesDeploymentPrefix, spec.runtimeRole, spec.connectorId, hash);
  const workloadHash = kubernetesLabelValue(hash);
  const commonLabels = {
    "app.kubernetes.io/name": "redpanda-connect",
    "app.kubernetes.io/component": "adapter-workload",
    "app.kubernetes.io/part-of": "rohrpost",
    "app.kubernetes.io/managed-by": runtimeConfig.serviceName,
    "rohrpost.dev/workload-hash": workloadHash,
    "rohrpost.dev/connector-id": kubernetesLabelValue(spec.connectorId),
    "rohrpost.dev/capability-id": kubernetesLabelValue(spec.capabilityId),
    "rohrpost.dev/runtime-role": spec.runtimeRole,
  };
  const commonAnnotations = {
    "rohrpost.dev/workload-key": spec.key,
    "rohrpost.dev/config-hash": spec.configHash,
    "rohrpost.dev/manifest-id": spec.manifestId,
    "rohrpost.dev/deployment-ids": spec.deploymentIds.join(","),
    "rohrpost.dev/flow-ids": spec.flowIds.join(","),
    "rohrpost.dev/revision-ids": spec.revisionIds.join(","),
    "rohrpost.dev/input-ref": spec.inputRef,
    "rohrpost.dev/output-ref": spec.outputRef,
  };
  const volumes: KubernetesResource[] = [
    {
      name: "connect-config",
      configMap: {
        name: configMapName,
        items: [
          {
            key: "connect.json",
            path: "connect.json",
          },
        ],
      },
    },
  ];
  const volumeMounts: KubernetesResource[] = [
    {
      name: "connect-config",
      mountPath: "/etc/redpanda-connect",
      readOnly: true,
    },
  ];

  if (spec.targetKind === "file") {
    volumes.push({
      name: "artifacts",
      persistentVolumeClaim: {
        claimName: runtimeConfig.connectKubernetesArtifactVolumeClaimName,
      },
    });
    volumeMounts.push({
      name: "artifacts",
      mountPath: "/artifacts",
    });
  }

  return {
    configMapName,
    deploymentName,
    configMap: {
      apiVersion: "v1",
      kind: "ConfigMap",
      metadata: {
        name: configMapName,
        namespace: runtimeConfig.connectKubernetesNamespace,
        labels: commonLabels,
        annotations: commonAnnotations,
      },
      data: {
        "connect.json": `${JSON.stringify(spec.connectConfig, null, 2)}\n`,
      },
    },
    deployment: {
      apiVersion: "apps/v1",
      kind: "Deployment",
      metadata: {
        name: deploymentName,
        namespace: runtimeConfig.connectKubernetesNamespace,
        labels: commonLabels,
        annotations: commonAnnotations,
      },
      spec: {
        replicas: 1,
        selector: {
          matchLabels: {
            "app.kubernetes.io/name": "redpanda-connect",
            "rohrpost.dev/workload-hash": workloadHash,
          },
        },
        template: {
          metadata: {
            labels: commonLabels,
            annotations: commonAnnotations,
          },
          spec: {
            serviceAccountName: runtimeConfig.connectKubernetesServiceAccountName,
            containers: [
              {
                name: "redpanda-connect",
                image: runtimeConfig.redpandaConnectImage,
                imagePullPolicy: runtimeConfig.connectKubernetesImagePullPolicy,
                command: ["/redpanda-connect", "run", "/etc/redpanda-connect/connect.json"],
                volumeMounts,
              },
            ],
            volumes,
          },
        },
      },
    },
  };
}

export class RedpandaConnectSupervisor {
  private readonly workloads = new Map<string, RunningWorkload>();
  private backend: SupervisorBackend = "disabled";
  private refreshTimer: ReturnType<typeof setTimeout> | undefined;
  private refreshInFlight: Promise<void> | undefined;
  private lastRefreshAt: string | undefined;
  private lastRefreshError: string | undefined;
  private lastReportAt: string | undefined;
  private lastReportError: string | undefined;

  public constructor(private readonly config: AdapterRedpandaConfig) {}

  private resolveBackend(): SupervisorBackend {
    if (this.config.connectBackend === "disabled") {
      return "disabled";
    }

    if (!this.config.controlApiUrl || !this.config.controlApiToken) {
      return "disabled";
    }

    if (!this.config.natsUrl) {
      return "disabled";
    }

    if (this.config.connectBackend === "kubernetes") {
      return this.isKubernetesAvailable() ? "kubernetes" : "disabled";
    }

    if (this.config.connectBackend === "auto" && this.isKubernetesAvailable()) {
      return "kubernetes";
    }

    try {
      execFileSync(this.config.connectDockerBinary, ["version"], { stdio: "ignore" });
      return "docker";
    } catch {
      return this.config.connectBackend === "docker" ? "docker" : "disabled";
    }
  }

  public async start(): Promise<void> {
    this.backend = this.resolveBackend();
    if (this.backend === "disabled") {
      this.lastRefreshError = this.config.connectBackend === "disabled"
        ? "Redpanda Connect supervision disabled by configuration"
        : "Redpanda Connect supervision backend is unavailable";
      return;
    }

    await this.refresh();
  }

  public async stop(): Promise<void> {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = undefined;
    }

    await Promise.all([...this.workloads.values()].map((workload) => this.stopWorkload(workload)));
    this.workloads.clear();
    await this.reportWorkloads();
  }

  public getSummary(): ConnectSupervisorSummary {
    const statuses = [...this.workloads.values()].map((entry) => entry.status);
    return {
      backend: this.backend,
      enabled: this.backend !== "disabled",
      managedWorkloads: statuses.length,
      runningWorkloads: statuses.filter((status) => status.status === "running").length,
      lastRefreshAt: this.lastRefreshAt,
      lastRefreshError: this.lastRefreshError,
      lastReportAt: this.lastReportAt,
      lastReportError: this.lastReportError,
    };
  }

  public getWorkloads(): ConnectWorkloadStatus[] {
    return [...this.workloads.values()].map((entry) => ({ ...entry.status, recentLogs: [...entry.status.recentLogs] }));
  }

  private scheduleRefresh(): void {
    if (this.backend === "disabled") {
      return;
    }

    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
    }

    this.refreshTimer = setTimeout(() => {
      this.refreshTimer = undefined;
      void this.refresh().catch(() => undefined);
    }, this.config.connectPollIntervalMs);
  }

  private async fetchActiveDeployments(): Promise<ConnectSupervisorDeploymentsResponse> {
    const client = createControlApiClient({
      baseUrl: this.config.controlApiUrl!,
      token: this.config.controlApiToken,
    });

    return {
      generatedAt: isoNow(),
      deployments: await client.fetchActiveDeployments(),
    };
  }

  private async reportWorkloads(): Promise<void> {
    if (!this.config.controlApiUrl || !this.config.controlApiToken) {
      return;
    }

    const reportedAt = isoNow();
    try {
      const client = createControlApiClient({
        baseUrl: this.config.controlApiUrl,
        token: this.config.controlApiToken,
      });
      await client.replaceAdapterWorkloadStatuses({
        reporterId: this.config.serviceName,
        reportedAt,
        workloads: this.getWorkloads(),
      });

      this.lastReportAt = reportedAt;
      this.lastReportError = undefined;
    } catch (error) {
      this.lastReportError = error instanceof Error ? error.message : String(error);
    }
  }

  private isKubernetesAvailable(): boolean {
    return Boolean(process.env.KUBERNETES_SERVICE_HOST) && existsSync(KUBERNETES_TOKEN_PATH);
  }

  private kubernetesApiBaseUrl(): string {
    const host = process.env.KUBERNETES_SERVICE_HOST;
    const port = process.env.KUBERNETES_SERVICE_PORT ?? "443";
    if (!host) {
      throw new Error("KUBERNETES_SERVICE_HOST is not set");
    }

    return `https://${host}:${port}`;
  }

  private kubernetesFetchOptions(init: RequestInit): RequestInit & { tls?: { ca?: string } } {
    const token = readFileSync(KUBERNETES_TOKEN_PATH, "utf8").trim();
    const ca = existsSync(KUBERNETES_CA_PATH) ? readFileSync(KUBERNETES_CA_PATH, "utf8") : undefined;
    return {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        ...(init.headers ?? {}),
      },
      tls: ca ? { ca } : undefined,
    };
  }

  private async kubernetesRequest(
    path: string,
    init: RequestInit,
    acceptedStatuses = new Set([200, 201, 202]),
  ): Promise<unknown> {
    const response = await fetch(
      `${this.kubernetesApiBaseUrl()}${path}`,
      this.kubernetesFetchOptions(init),
    );

    if (!acceptedStatuses.has(response.status)) {
      const body = await response.text().catch(() => "");
      throw new Error(`Kubernetes API ${path} failed with status ${response.status}${body ? `: ${body}` : ""}`);
    }

    if (response.status === 204) {
      return undefined;
    }

    return response.json();
  }

  private async applyKubernetesResource(path: string, resource: KubernetesResource): Promise<void> {
    await this.kubernetesRequest(
      `${path}?fieldManager=adapter-redpanda&force=true`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/apply-patch+yaml",
        },
        body: JSON.stringify(resource),
      },
    );
  }

  private async deleteKubernetesResource(path: string): Promise<void> {
    await this.kubernetesRequest(
      path,
      {
        method: "DELETE",
      },
      new Set([200, 202, 204, 404]),
    );
  }

  public async refresh(): Promise<void> {
    if (this.refreshInFlight) {
      return this.refreshInFlight;
    }

    this.refreshInFlight = (async () => {
      try {
        const activeDeployments = await this.fetchActiveDeployments();
        const desiredSpecs = collectDesiredConnectWorkloads(activeDeployments, this.config);
        const desiredByKey = new Map(desiredSpecs.map((spec) => [spec.key, spec]));

        for (const [key, running] of this.workloads.entries()) {
          if (!desiredByKey.has(key)) {
            await this.stopWorkload(running);
            this.workloads.delete(key);
          }
        }

        for (const spec of desiredSpecs) {
          await this.ensureWorkload(spec);
        }

        this.lastRefreshAt = isoNow();
        this.lastRefreshError = undefined;
        await this.reportWorkloads();
      } catch (error) {
        this.lastRefreshError = error instanceof Error ? error.message : String(error);
      } finally {
        this.scheduleRefresh();
      }
    })().finally(() => {
      this.refreshInFlight = undefined;
    });

    return this.refreshInFlight;
  }

  private async ensureWorkload(spec: ConnectWorkloadSpec): Promise<void> {
    if (this.backend === "kubernetes") {
      await this.ensureKubernetesWorkload(spec);
      return;
    }

    await this.ensureDockerWorkload(spec);
  }

  private syncWorkloadStatusIdentity(status: ConnectWorkloadStatus, spec: ConnectWorkloadSpec): void {
    status.deploymentIds = [...spec.deploymentIds];
    status.flowIds = [...spec.flowIds];
    status.revisionIds = [...spec.revisionIds];
  }

  private buildWorkloadStatus(
    spec: ConnectWorkloadSpec,
    existing: RunningWorkload | undefined,
  ): ConnectWorkloadStatus {
    return {
      key: spec.key,
      connectorId: spec.connectorId,
      capabilityId: spec.capabilityId,
      manifestId: spec.manifestId,
      deploymentIds: [...spec.deploymentIds],
      flowIds: [...spec.flowIds],
      revisionIds: [...spec.revisionIds],
      runtimeRole: spec.runtimeRole,
      inputRef: spec.inputRef,
      outputRef: spec.outputRef,
      status: "starting",
      backend: this.backend,
      consumerRef: spec.consumerRef,
      targetKind: spec.targetKind,
      artifactPath: spec.artifactPath,
      configPath: spec.configPath,
      containerName: spec.containerName,
      startedAt: isoNow(),
      restartCount: existing ? existing.status.restartCount + 1 : 0,
      recentLogs: existing ? [...existing.status.recentLogs] : [],
    };
  }

  private async ensureDockerWorkload(spec: ConnectWorkloadSpec): Promise<void> {
    const existing = this.workloads.get(spec.key);
    if (
      existing?.process
      && existing.process.exitCode !== null
      && existing.status.status === "running"
    ) {
      existing.status.status = "degraded";
      existing.status.stoppedAt = isoNow();
      existing.status.lastError = `Redpanda Connect workload exited with code ${existing.process.exitCode}`;
      existing.process = undefined;
    }

    if (
      existing
      && existing.spec.configHash === spec.configHash
      && existing.process
      && existing.process.exitCode === null
      && !existing.process.killed
      && existing.status.status !== "degraded"
    ) {
      existing.spec = spec;
      this.syncWorkloadStatusIdentity(existing.status, spec);
      existing.status.status = "running";
      return;
    }

    if (existing) {
      await this.stopWorkload(existing);
    }

    const status = this.buildWorkloadStatus(spec, existing);
    const running: RunningWorkload = { spec, status };
    this.workloads.set(spec.key, running);

    const configDir = dirname(spec.configPath);
    mkdirSync(configDir, { recursive: true });
    if (spec.artifactPath) {
      mkdirSync(dirname(spec.artifactPath), { recursive: true });
    }
    writeFileSync(spec.configPath, `${JSON.stringify(spec.connectConfig, null, 2)}\n`);
    try {
      execFileSync(this.config.connectDockerBinary, ["rm", "-f", spec.containerName], { stdio: "ignore" });
    } catch {
      // Ignore missing stale containers.
    }

    const [command, ...args] = spec.dockerCommand;
    const process = spawn(command, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });
    running.process = process as unknown as ChildProcess;
    status.status = "running";

    process.stdout?.on("data", (chunk) => {
      pushLogLine(status, chunk.toString("utf8").trim());
    });
    process.stderr?.on("data", (chunk) => {
      pushLogLine(status, chunk.toString("utf8").trim());
    });
  }

  private async ensureKubernetesWorkload(spec: ConnectWorkloadSpec): Promise<void> {
    const existing = this.workloads.get(spec.key);
    const resources = buildKubernetesWorkloadResources(spec, this.config);

    if (existing && existing.spec.configHash === spec.configHash) {
      existing.spec = spec;
      existing.kubernetesResources = resources;
      this.syncWorkloadStatusIdentity(existing.status, spec);
      existing.status.containerName = resources.deploymentName;
      await this.refreshKubernetesWorkloadStatus(existing);
      return;
    }

    if (existing) {
      await this.stopWorkload(existing);
    }

    const status = this.buildWorkloadStatus(spec, existing);
    status.containerName = resources.deploymentName;
    const running: RunningWorkload = { spec, status, kubernetesResources: resources };
    this.workloads.set(spec.key, running);

    try {
      const namespace = encodeURIComponent(this.config.connectKubernetesNamespace);
      const configMapName = encodeURIComponent(resources.configMapName);
      const deploymentName = encodeURIComponent(resources.deploymentName);
      await this.applyKubernetesResource(
        `/api/v1/namespaces/${namespace}/configmaps/${configMapName}`,
        resources.configMap,
      );
      await this.applyKubernetesResource(
        `/apis/apps/v1/namespaces/${namespace}/deployments/${deploymentName}`,
        resources.deployment,
      );
      pushLogLine(status, `Applied Kubernetes Redpanda Connect deployment ${resources.deploymentName}`);
      await this.refreshKubernetesWorkloadStatus(running);
    } catch (error) {
      status.status = "degraded";
      status.lastError = error instanceof Error ? error.message : String(error);
      pushLogLine(status, status.lastError);
    }
  }

  private async refreshKubernetesWorkloadStatus(workload: RunningWorkload): Promise<void> {
    const resources = workload.kubernetesResources;
    if (!resources) {
      return;
    }

    const namespace = encodeURIComponent(this.config.connectKubernetesNamespace);
    const deploymentName = encodeURIComponent(resources.deploymentName);
    try {
      const deployment = await this.kubernetesRequest(
        `/apis/apps/v1/namespaces/${namespace}/deployments/${deploymentName}`,
        { method: "GET" },
      ) as {
        status?: {
          availableReplicas?: number;
          readyReplicas?: number;
          replicas?: number;
          conditions?: Array<{ type?: string; status?: string; reason?: string; message?: string }>;
        };
      };
      const status = deployment.status ?? {};
      const failedCondition = status.conditions?.find(
        (condition) => condition.type === "Progressing" && condition.status === "False",
      );
      if (failedCondition) {
        workload.status.status = "degraded";
        workload.status.lastError = failedCondition.message ?? failedCondition.reason ?? "Kubernetes rollout is not progressing";
        pushLogLine(workload.status, workload.status.lastError);
        return;
      }

      if ((status.availableReplicas ?? 0) > 0 || (status.readyReplicas ?? 0) > 0) {
        workload.status.status = "running";
        workload.status.lastError = undefined;
        return;
      }

      workload.status.status = "starting";
    } catch (error) {
      workload.status.status = "degraded";
      workload.status.lastError = error instanceof Error ? error.message : String(error);
      pushLogLine(workload.status, workload.status.lastError);
    }
  }

  private async stopWorkload(workload: RunningWorkload): Promise<void> {
    if (workload.status.backend === "kubernetes" && workload.kubernetesResources) {
      const namespace = encodeURIComponent(this.config.connectKubernetesNamespace);
      await Promise.all([
        this.deleteKubernetesResource(
          `/apis/apps/v1/namespaces/${namespace}/deployments/${encodeURIComponent(workload.kubernetesResources.deploymentName)}`,
        ),
        this.deleteKubernetesResource(
          `/api/v1/namespaces/${namespace}/configmaps/${encodeURIComponent(workload.kubernetesResources.configMapName)}`,
        ),
      ]);
      workload.status.status = "stopped";
      workload.status.stoppedAt = isoNow();
      workload.process = undefined;
      return;
    }

    if (workload.process && !workload.process.killed && workload.status.containerName) {
      try {
        execFileSync(this.config.connectDockerBinary, ["rm", "-f", workload.status.containerName], { stdio: "ignore" });
      } catch {
        workload.process.kill("SIGTERM");
      }
    }

    workload.status.status = "stopped";
    workload.status.stoppedAt = isoNow();
    workload.process = undefined;
  }
}
