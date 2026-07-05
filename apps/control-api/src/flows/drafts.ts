import {
  compileFlowSpec,
  simulateFlowSpec,
  type FlowSpec,
  type SimulationSample,
} from "@rohrpost/shared-flow-spec";

export interface DraftResponse {
  draft: FlowSpec;
  compiler: ReturnType<typeof compileFlowSpec>;
  simulation: ReturnType<typeof simulateFlowSpec>;
}

interface DraftOptions {
  prompt: string;
  samplePayload?: unknown;
  tenantId: string;
  flowId?: string;
  revisionId?: string;
  name?: string;
}

function normalizePrompt(prompt: string): string {
  return prompt.trim().toLowerCase();
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "flow";
}

function detectSource(prompt: string): { kind: FlowSpec["sources"][number]["kind"]; capabilityId: string; executionMode: "native" | "adapter" } {
  if (prompt.includes("kafka")) {
    return { kind: "kafka", capabilityId: "kafka_in", executionMode: "adapter" };
  }
  if (prompt.includes("nats")) {
    return { kind: "nats", capabilityId: "nats_in", executionMode: "native" };
  }
  return { kind: "http", capabilityId: "http_in", executionMode: "native" };
}

function detectSink(prompt: string): { kind: FlowSpec["sinks"][number]["kind"]; capabilityId: string; executionMode: "native" | "adapter"; deliveryGuarantee: FlowSpec["sinks"][number]["deliveryGuarantee"] } {
  if (prompt.includes("snowflake")) {
    return { kind: "snowflake", capabilityId: "snowflake_sink", executionMode: "adapter", deliveryGuarantee: "idempotent" };
  }
  if (prompt.includes("bigquery")) {
    return { kind: "bigquery", capabilityId: "bigquery_sink", executionMode: "adapter", deliveryGuarantee: "append_only" };
  }
  if (prompt.includes("s3")) {
    return { kind: "s3", capabilityId: "s3_sink", executionMode: "adapter", deliveryGuarantee: "append_only" };
  }
  if (prompt.includes("kafka")) {
    return { kind: "kafka", capabilityId: "kafka_out", executionMode: "adapter", deliveryGuarantee: "append_only" };
  }
  if (prompt.includes("nats")) {
    return { kind: "nats", capabilityId: "nats_out", executionMode: "native", deliveryGuarantee: "idempotent" };
  }
  return { kind: "http", capabilityId: "http_out", executionMode: "native", deliveryGuarantee: "best_effort" };
}

export function buildDraftFromPrompt(options: DraftOptions): DraftResponse {
  const prompt = normalizePrompt(options.prompt);
  const source = detectSource(prompt);
  const sink = detectSink(prompt);
  const flowId = options.flowId ?? `flow_${slugify(options.name ?? options.prompt)}`;
  const revisionId = options.revisionId ?? `rev_${slugify(options.name ?? options.prompt)}_v1`;
  const name = options.name ?? (options.prompt.trim() || "Untitled flow");
  const samplePayload =
    options.samplePayload ??
    ({
      eventType: "event",
      pii: {
        email: "customer@example.com",
      },
      amount: 42,
    } satisfies Record<string, unknown>);

  const processors: FlowSpec["processors"] = [
    {
      id: "processor_enrich",
      kind: "enrich_static",
      values: {
        routedBy: "control-api",
        mode: sink.executionMode,
      },
      nextNodeIds: ["route_terminal"],
    },
  ];

  if (prompt.includes("redact") || prompt.includes("pii")) {
    processors.unshift({
      id: "processor_redact",
      kind: "redact",
      paths: ["pii.email"],
      mask: "[redacted]",
      nextNodeIds: ["processor_enrich"],
    });
  }

  if (prompt.includes("filter")) {
    processors.unshift({
      id: "processor_filter",
      kind: "filter",
      predicate: {
        type: "field_gte",
        path: "amount",
        value: 100,
      },
      nextNodeIds: [processors[0]?.id ?? "route_terminal"],
    });
  }

  const firstNodeId = processors[0]?.id ?? "route_terminal";
  const draft: FlowSpec = {
    version: 1,
    metadata: {
      tenantId: options.tenantId,
      flowId,
      revisionId,
      name,
      description: `Drafted from prompt: ${options.prompt}`,
      tags: [source.executionMode, sink.executionMode],
    },
    sources: [
      {
        id: "source_primary",
        kind: source.kind,
        connector: {
          capabilityId: source.capabilityId,
          connectorId: `${source.capabilityId}_default`,
          executionMode: source.executionMode,
        },
        stream: "ingress",
        nextNodeIds: [firstNodeId],
      },
    ],
    processors,
    routes: [
      {
        id: "route_terminal",
        fromNodeId: processors.at(-1)?.id ?? "source_primary",
        predicate: { type: "always" },
        toSinkIds: ["sink_primary"],
        priority: 100,
      },
    ],
    sinks: [
      {
        id: "sink_primary",
        kind: sink.kind,
        connector: {
          capabilityId: sink.capabilityId,
          connectorId: `${sink.capabilityId}_default`,
          executionMode: sink.executionMode,
        },
        deliveryGuarantee: sink.deliveryGuarantee,
        stream: sink.executionMode === "adapter" ? "work" : "retry",
      },
    ],
    retryPolicy: {
      maxAttempts: sink.deliveryGuarantee === "idempotent" ? 3 : 1,
      initialBackoffMs: 250,
      maxBackoffMs: 5_000,
      multiplier: 2,
      retryableStatusCodes: [408, 429, 500, 502, 503, 504],
    },
    dlqPolicy: {
      enabled: true,
      sinkId: "sink_primary",
      reasonFormat: "json",
    },
    batchingPolicy: {
      enabled: sink.kind === "snowflake" || sink.kind === "bigquery" || sink.kind === "s3",
      batchSize: 100,
      flushIntervalMs: 5_000,
      keyPath: "tenantId",
    },
    idempotencyStrategy: sink.deliveryGuarantee === "best_effort" ? "partition_key" : "message_id",
  };

  const samples: SimulationSample[] = [
    {
      envelope: {},
      payload: samplePayload,
      sourceId: "source_primary",
    },
  ];

  return {
    draft,
    compiler: compileFlowSpec(draft),
    simulation: simulateFlowSpec(draft, samples),
  };
}
