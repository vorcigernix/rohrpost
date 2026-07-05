import {
  ControlApiHttpError,
  createControlApiClient,
  type ControlApiConnectorRecord,
  type ControlApiFlowRecord,
  type ControlApiRuntimeDeploymentRecord,
} from "@rohrpost/control-api-contracts";
import { compileFlowSpec, FlowSpecValidationError, type FlowSpec } from "@rohrpost/shared-flow-spec";
import {
  buildDeploymentIngressPattern,
  buildDeploymentReplayPattern,
  buildDeploymentRetryPattern,
} from "./jetstream";
import type {
  DeploymentLoadError,
  DeploymentSource,
  DeploymentTargetMap,
  RouterDeployment,
  SinkTarget,
} from "./phase2-types";

export interface ControlApiDeploymentSourceOptions {
  controlApiUrl: string;
  controlApiToken?: string;
  sinkTargets?: DeploymentTargetMap;
  fetchImpl?: typeof fetch;
}

function pickFlowRevision(record: ControlApiFlowRecord): string {
  return record.activeRevisionId ?? record.revisionId ?? record.latestRevisionId ?? "latest";
}

function resolveSinkTarget(
  sinkId: string,
  sinkConnectorId: string,
  sinkTargets: Record<string, SinkTarget>,
): SinkTarget | undefined {
  return sinkTargets[sinkConnectorId] ?? sinkTargets[sinkId];
}

function sinkTargetFromConnector(
  connector: ControlApiConnectorRecord | undefined,
): SinkTarget | undefined {
  if (!connector) {
    return undefined;
  }

  if (
    connector.capabilityId === "http_out" &&
    typeof connector.config.url === "string"
  ) {
    return {
      kind: "http",
      connectorId: connector.id,
      url: connector.config.url,
      method:
        connector.config.method === "PUT" || connector.config.method === "PATCH"
          ? connector.config.method
          : "POST",
      headers:
        typeof connector.config.headers === "object" && connector.config.headers
          ? (connector.config.headers as Record<string, string>)
          : undefined,
      timeoutMs:
        typeof connector.config.timeoutMs === "number" ? connector.config.timeoutMs : undefined,
    };
  }

  if (
    connector.capabilityId === "nats_out" &&
    typeof connector.config.subject === "string"
  ) {
    return {
      kind: "nats",
      connectorId: connector.id,
      subject: connector.config.subject,
      headers:
        typeof connector.config.headers === "object" && connector.config.headers
          ? (connector.config.headers as Record<string, string>)
          : undefined,
    };
  }

  if (connector.executionMode === "adapter") {
    return {
      kind: "adapter",
      connectorId: connector.id,
      capabilityId: connector.capabilityId,
      workStream: "work",
    };
  }

  return undefined;
}

function natsSourceSubjectsFromConnectors(
  spec: FlowSpec,
  connectors: Record<string, ControlApiConnectorRecord>,
): string[] {
  return spec.sources.flatMap((source) => {
    if (source.kind !== "nats") {
      return [];
    }

    const connector = connectors[source.connector.connectorId];
    const subject = connector?.config.subject;
    return typeof subject === "string" ? [subject] : [];
  });
}

function httpSourcePathsFromConnectors(
  spec: FlowSpec,
  connectors: Record<string, ControlApiConnectorRecord>,
): string[] {
  return spec.sources.flatMap((source) => {
    if (source.kind !== "http") {
      return [];
    }

    const connector = connectors[source.connector.connectorId];
    const path = connector?.config.path;
    return typeof path === "string" && path.startsWith("/") ? [path] : [];
  });
}

export function mapControlApiRecordToDeployment(
  record: ControlApiFlowRecord,
  sinkTargets: DeploymentTargetMap = { sinks: {} },
): RouterDeployment | null {
  if (record.status !== "active" || !record.spec) {
    return null;
  }

  const compiled = compileFlowSpec(record.spec);
  const revisionId = pickFlowRevision(record);

  return {
    id: record.id,
    tenantId: record.tenantId,
    flowId: record.id,
    revisionId,
    active: true,
    spec: record.spec,
    compiled,
    sourceSubjects: [
      buildDeploymentIngressPattern(record.tenantId, record.id, revisionId),
      buildDeploymentRetryPattern(record.tenantId, record.id, revisionId),
      buildDeploymentReplayPattern(record.tenantId, record.id, revisionId),
    ],
    natsSourceSubjects: [],
    httpSourcePaths: [],
    sinkTargets: Object.fromEntries(
      record.spec.sinks
        .map((sink) => [
          sink.id,
          resolveSinkTarget(sink.id, sink.connector.connectorId, sinkTargets.sinks),
        ])
        .filter((entry): entry is [string, SinkTarget] => Boolean(entry[1])),
    ),
    connectors: {},
  };
}

function mapRuntimeDeploymentRecord(
  record: ControlApiRuntimeDeploymentRecord,
  sinkTargets: DeploymentTargetMap = { sinks: {} },
): RouterDeployment {
  const { deployment, revision, connectors } = record;
  const compiled = compileFlowSpec(revision.spec);
  const resolvedSinkTargets: Record<string, SinkTarget> = {};

  for (const sink of revision.spec.sinks) {
    const explicit = resolveSinkTarget(sink.id, sink.connector.connectorId, sinkTargets.sinks);
    const fromConnector = sinkTargetFromConnector(connectors[sink.connector.connectorId]);
    const target = explicit ?? fromConnector;
    if (target) {
      resolvedSinkTargets[sink.id] = target;
      resolvedSinkTargets[sink.connector.connectorId] = target;
    }
  }

  return {
    id: deployment.id,
    tenantId: revision.spec.metadata.tenantId,
    flowId: deployment.flowId,
    revisionId: deployment.revisionId,
    active: deployment.status === "active",
    spec: revision.spec,
    compiled,
    sourceSubjects: [
      buildDeploymentIngressPattern(revision.spec.metadata.tenantId, deployment.flowId, deployment.revisionId),
      buildDeploymentRetryPattern(revision.spec.metadata.tenantId, deployment.flowId, deployment.revisionId),
      buildDeploymentReplayPattern(revision.spec.metadata.tenantId, deployment.flowId, deployment.revisionId),
    ],
    natsSourceSubjects: natsSourceSubjectsFromConnectors(revision.spec, connectors),
    httpSourcePaths: httpSourcePathsFromConnectors(revision.spec, connectors),
    sinkTargets: resolvedSinkTargets,
    connectors,
  };
}

export class ControlApiDeploymentSource implements DeploymentSource {
  private readonly loadErrors: DeploymentLoadError[] = [];
  private readonly controlApiClient;

  public constructor(
    private readonly options: ControlApiDeploymentSourceOptions,
  ) {
    this.controlApiClient = createControlApiClient({
      baseUrl: this.options.controlApiUrl,
      token: this.options.controlApiToken,
      fetchImpl: this.options.fetchImpl,
    });
  }

  public getLoadErrors(): DeploymentLoadError[] {
    return [...this.loadErrors];
  }

  public async loadDeployments(): Promise<RouterDeployment[]> {
    this.loadErrors.length = 0;
    try {
      const deployments = await this.controlApiClient.fetchActiveDeployments();
      return deployments.flatMap((record) => {
        try {
          return [mapRuntimeDeploymentRecord(record, this.options.sinkTargets)];
        } catch (error) {
          this.loadErrors.push({
            deploymentId: record.deployment.id,
            tenantId: record.revision.spec.metadata.tenantId,
            flowId: record.deployment.flowId,
            revisionId: record.deployment.revisionId,
            reason: error instanceof Error ? error.message : String(error),
            issues: error instanceof FlowSpecValidationError ? error.issues : undefined,
          });
          return [];
        }
      });
    } catch (error) {
      if (!(error instanceof ControlApiHttpError) || error.status !== 404) {
        throw error;
      }
    }

    const data = await this.controlApiClient.fetchFlows();
    return data
      .map((record) => {
        try {
          return mapControlApiRecordToDeployment(record, this.options.sinkTargets);
        } catch (error) {
          this.loadErrors.push({
            deploymentId: record.id,
            tenantId: record.tenantId,
            flowId: record.id,
            revisionId: pickFlowRevision(record),
            reason: error instanceof Error ? error.message : String(error),
            issues: error instanceof FlowSpecValidationError ? error.issues : undefined,
          });
          return null;
        }
      })
      .filter((deployment): deployment is RouterDeployment => deployment !== null);
  }
}

export class StaticDeploymentSource implements DeploymentSource {
  public constructor(private readonly deployments: RouterDeployment[]) {}

  public async loadDeployments(): Promise<RouterDeployment[]> {
    return this.deployments;
  }

  public getLoadErrors(): DeploymentLoadError[] {
    return [];
  }
}
