import { describe, expect, it } from "bun:test";

import { loadAdapterRedpandaConfig } from "../src/config";

describe("loadAdapterRedpandaConfig", () => {
  it("applies defaults when env is empty", () => {
    const config = loadAdapterRedpandaConfig({});

    expect(config.host).toBe("0.0.0.0");
    expect(config.port).toBe(3003);
    expect(config.redpandaConnectImage).toBe("redpandadata/connect:latest");
    expect(config.manifestSource).toBe("local");
    expect(config.connectBackend).toBe("auto");
    expect(config.connectPollIntervalMs).toBe(10_000);
    expect(config.connectDockerBinary).toBe("docker");
    expect(config.connectWorkdir).toBe("data/redpanda-connect");
    expect(config.connectKubernetesNamespace).toBe("default");
    expect(config.connectKubernetesServiceAccountName).toBe("adapter-redpanda-connect");
    expect(config.connectKubernetesConfigMapPrefix).toBe("redpanda-connect");
    expect(config.connectKubernetesDeploymentPrefix).toBe("redpanda-connect");
    expect(config.connectKubernetesImagePullPolicy).toBe("IfNotPresent");
    expect(config.connectKubernetesArtifactVolumeClaimName).toBe("adapter-redpanda-artifacts");
    expect(config.natsUrl).toBe("nats://127.0.0.1:4222");
    expect(config.controlApiUrl).toBe("http://127.0.0.1:3001");
    expect(config.controlApiToken).toBe("dev-admin-token");
    expect(config.deliveryLogPath).toBe("data/adapter-deliveries.jsonl");
    expect(config.deliveryLogEnabled).toBe(true);
    expect(config.artifactRoot).toBe("data/adapter-artifacts");
  });

  it("reads explicit overrides", () => {
    const config = loadAdapterRedpandaConfig({
      ADAPTER_REDPANDA_HOST: "127.0.0.1",
      ADAPTER_REDPANDA_PORT: "8124",
      REDPANDA_CONNECT_IMAGE: "example/connect:dev",
      MANIFEST_SOURCE: "registry",
      ADAPTER_REDPANDA_CONNECT_BACKEND: "docker",
      ADAPTER_REDPANDA_CONNECT_POLL_INTERVAL_MS: "2500",
      ADAPTER_REDPANDA_CONNECT_DOCKER_BINARY: "/usr/local/bin/docker",
      ADAPTER_REDPANDA_CONNECT_WORKDIR: "/tmp/redpanda-connect",
      ADAPTER_REDPANDA_CONNECT_K8S_NAMESPACE: "rohrpost",
      ADAPTER_REDPANDA_CONNECT_K8S_SERVICE_ACCOUNT_NAME: "connect-runner",
      ADAPTER_REDPANDA_CONNECT_K8S_CONFIGMAP_PREFIX: "rpc-cfg",
      ADAPTER_REDPANDA_CONNECT_K8S_DEPLOYMENT_PREFIX: "rpc-workload",
      ADAPTER_REDPANDA_CONNECT_K8S_IMAGE_PULL_POLICY: "Always",
      ADAPTER_REDPANDA_CONNECT_K8S_ARTIFACT_PVC_NAME: "adapter-artifacts-pvc",
      ADAPTER_REDPANDA_NATS_URL: "nats://adapter.test:4222",
      ADAPTER_REDPANDA_CONTROL_API_URL: "http://control-api.test:3001",
      ADAPTER_REDPANDA_CONTROL_API_TOKEN: "adapter-token",
      ADAPTER_REDPANDA_DELIVERY_LOG_PATH: "/tmp/adapter-deliveries.jsonl",
      ADAPTER_REDPANDA_DELIVERY_LOG_ENABLED: "false",
      ADAPTER_REDPANDA_ARTIFACT_ROOT: "/tmp/adapter-artifacts",
    });

    expect(config.host).toBe("127.0.0.1");
    expect(config.port).toBe(8124);
    expect(config.redpandaConnectImage).toBe("example/connect:dev");
    expect(config.manifestSource).toBe("registry");
    expect(config.connectBackend).toBe("docker");
    expect(config.connectPollIntervalMs).toBe(2500);
    expect(config.connectDockerBinary).toBe("/usr/local/bin/docker");
    expect(config.connectWorkdir).toBe("/tmp/redpanda-connect");
    expect(config.connectKubernetesNamespace).toBe("rohrpost");
    expect(config.connectKubernetesServiceAccountName).toBe("connect-runner");
    expect(config.connectKubernetesConfigMapPrefix).toBe("rpc-cfg");
    expect(config.connectKubernetesDeploymentPrefix).toBe("rpc-workload");
    expect(config.connectKubernetesImagePullPolicy).toBe("Always");
    expect(config.connectKubernetesArtifactVolumeClaimName).toBe("adapter-artifacts-pvc");
    expect(config.natsUrl).toBe("nats://adapter.test:4222");
    expect(config.controlApiUrl).toBe("http://control-api.test:3001");
    expect(config.controlApiToken).toBe("adapter-token");
    expect(config.deliveryLogPath).toBe("/tmp/adapter-deliveries.jsonl");
    expect(config.deliveryLogEnabled).toBe(false);
    expect(config.artifactRoot).toBe("/tmp/adapter-artifacts");
  });
});
