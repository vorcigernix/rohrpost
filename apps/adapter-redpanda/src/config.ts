export interface AdapterRedpandaConfig {
  host: string;
  port: number;
  redpandaConnectImage: string;
  manifestSource: string;
  connectBackend: "auto" | "docker" | "kubernetes" | "disabled";
  connectPollIntervalMs: number;
  connectDockerBinary: string;
  connectWorkdir: string;
  connectKubernetesNamespace: string;
  connectKubernetesServiceAccountName: string;
  connectKubernetesConfigMapPrefix: string;
  connectKubernetesDeploymentPrefix: string;
  connectKubernetesImagePullPolicy: "Always" | "IfNotPresent" | "Never";
  connectKubernetesArtifactVolumeClaimName: string;
  serviceName: string;
  natsUrl?: string;
  controlApiUrl?: string;
  controlApiToken?: string;
  deliveryLogPath: string;
  deliveryLogEnabled: boolean;
  artifactRoot: string;
}

const DEFAULTS: AdapterRedpandaConfig = {
  host: "0.0.0.0",
  port: 3003,
  redpandaConnectImage: "redpandadata/connect:latest",
  manifestSource: "local",
  connectBackend: "auto",
  connectPollIntervalMs: 10_000,
  connectDockerBinary: "docker",
  connectWorkdir: "data/redpanda-connect",
  connectKubernetesNamespace: "default",
  connectKubernetesServiceAccountName: "adapter-redpanda-connect",
  connectKubernetesConfigMapPrefix: "redpanda-connect",
  connectKubernetesDeploymentPrefix: "redpanda-connect",
  connectKubernetesImagePullPolicy: "IfNotPresent",
  connectKubernetesArtifactVolumeClaimName: "adapter-redpanda-artifacts",
  serviceName: "adapter-redpanda",
  natsUrl: "nats://127.0.0.1:4222",
  controlApiUrl: "http://127.0.0.1:3001",
  controlApiToken: "dev-admin-token",
  deliveryLogPath: "data/adapter-deliveries.jsonl",
  deliveryLogEnabled: true,
  artifactRoot: "data/adapter-artifacts",
};

function parsePort(value: string | undefined, fallback: number): number {
  if (!value) return fallback;

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new Error(`Invalid port value: ${value}`);
  }

  return parsed;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`Invalid positive integer value: ${value}`);
  }

  return parsed;
}

function parseBoolean(value: string | undefined, fallback: boolean): boolean {
  if (value == null || value === "") return fallback;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  throw new Error(`Invalid boolean value: ${value}`);
}

export function loadAdapterRedpandaConfig(
  env: Record<string, string | undefined> = process.env,
): AdapterRedpandaConfig {
  const connectBackend = env.ADAPTER_REDPANDA_CONNECT_BACKEND ?? DEFAULTS.connectBackend;
  if (
    connectBackend !== "auto"
    && connectBackend !== "docker"
    && connectBackend !== "kubernetes"
    && connectBackend !== "disabled"
  ) {
    throw new Error(`Invalid connect backend value: ${connectBackend}`);
  }
  const connectKubernetesImagePullPolicy =
    env.ADAPTER_REDPANDA_CONNECT_K8S_IMAGE_PULL_POLICY ?? DEFAULTS.connectKubernetesImagePullPolicy;
  if (
    connectKubernetesImagePullPolicy !== "Always"
    && connectKubernetesImagePullPolicy !== "IfNotPresent"
    && connectKubernetesImagePullPolicy !== "Never"
  ) {
    throw new Error(`Invalid Kubernetes image pull policy value: ${connectKubernetesImagePullPolicy}`);
  }

  return {
    host: env.ADAPTER_REDPANDA_HOST ?? env.HOST ?? DEFAULTS.host,
    port: parsePort(env.ADAPTER_REDPANDA_PORT ?? env.PORT, DEFAULTS.port),
    redpandaConnectImage: env.REDPANDA_CONNECT_IMAGE ?? DEFAULTS.redpandaConnectImage,
    manifestSource: env.MANIFEST_SOURCE ?? DEFAULTS.manifestSource,
    connectBackend,
    connectPollIntervalMs: parsePositiveInt(
      env.ADAPTER_REDPANDA_CONNECT_POLL_INTERVAL_MS,
      DEFAULTS.connectPollIntervalMs,
    ),
    connectDockerBinary:
      env.ADAPTER_REDPANDA_CONNECT_DOCKER_BINARY ?? DEFAULTS.connectDockerBinary,
    connectWorkdir: env.ADAPTER_REDPANDA_CONNECT_WORKDIR ?? DEFAULTS.connectWorkdir,
    connectKubernetesNamespace:
      env.ADAPTER_REDPANDA_CONNECT_K8S_NAMESPACE ?? DEFAULTS.connectKubernetesNamespace,
    connectKubernetesServiceAccountName:
      env.ADAPTER_REDPANDA_CONNECT_K8S_SERVICE_ACCOUNT_NAME
      ?? DEFAULTS.connectKubernetesServiceAccountName,
    connectKubernetesConfigMapPrefix:
      env.ADAPTER_REDPANDA_CONNECT_K8S_CONFIGMAP_PREFIX
      ?? DEFAULTS.connectKubernetesConfigMapPrefix,
    connectKubernetesDeploymentPrefix:
      env.ADAPTER_REDPANDA_CONNECT_K8S_DEPLOYMENT_PREFIX
      ?? DEFAULTS.connectKubernetesDeploymentPrefix,
    connectKubernetesImagePullPolicy,
    connectKubernetesArtifactVolumeClaimName:
      env.ADAPTER_REDPANDA_CONNECT_K8S_ARTIFACT_PVC_NAME
      ?? DEFAULTS.connectKubernetesArtifactVolumeClaimName,
    serviceName: DEFAULTS.serviceName,
    natsUrl: env.ADAPTER_REDPANDA_NATS_URL ?? env.NATS_URL ?? DEFAULTS.natsUrl,
    controlApiUrl: env.ADAPTER_REDPANDA_CONTROL_API_URL ?? env.CONTROL_API_URL ?? DEFAULTS.controlApiUrl,
    controlApiToken: env.ADAPTER_REDPANDA_CONTROL_API_TOKEN ?? env.CONTROL_API_TOKEN ?? DEFAULTS.controlApiToken,
    deliveryLogPath: env.ADAPTER_REDPANDA_DELIVERY_LOG_PATH ?? DEFAULTS.deliveryLogPath,
    deliveryLogEnabled: parseBoolean(
      env.ADAPTER_REDPANDA_DELIVERY_LOG_ENABLED,
      DEFAULTS.deliveryLogEnabled,
    ),
    artifactRoot: env.ADAPTER_REDPANDA_ARTIFACT_ROOT ?? DEFAULTS.artifactRoot,
  };
}
