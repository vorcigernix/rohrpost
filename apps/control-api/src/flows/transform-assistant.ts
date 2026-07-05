import {
  CONNECTOR_CAPABILITIES,
  type JsonSourceBinding,
  type JsonSourceKind,
} from "@rohrpost/domain-connectors";
import {
  compileFlowSpec,
  evaluatePredicate,
  getPath,
  setPath,
  simulateFlowSpec,
  validateFlowSpec,
  type ConnectorCapability,
  type FlowSpec,
  type FlowSpecValidationResult,
  type PredicateExpr,
} from "@rohrpost/shared-flow-spec";
import type { ControlApiConfig } from "../config";
type SinkCapabilityId =
  | "http_out"
  | "nats_out"
  | "snowflake_sink"
  | "bigquery_sink"
  | "s3_sink"
  | "kafka_out";

type AssistantProvider = "gemini" | "heuristic";

interface JsonLeafPath {
  path: string;
  leaf: string;
  type: string;
  example: string;
}

interface ResolvedFieldPath {
  path: string;
  preservePath: boolean;
}

type SemanticFieldGroup =
  | "ecommerce"
  | "identity"
  | "product"
  | "cart"
  | "revenue"
  | "attribution";

export interface JsonTransformFieldMapping {
  from: string;
  to: string;
}

export interface JsonTransformPlan {
  suggestedName: string;
  summary: string;
  fieldMappings: JsonTransformFieldMapping[];
  filter?: PredicateExpr;
  filterSummary?: string;
  explanation: string[];
  recommendedSinkCapabilityIds: SinkCapabilityId[];
}

export interface JsonTransformPreview {
  accepted: boolean;
  output?: unknown;
  droppedReason?: string;
  notes: string[];
}

export interface JsonTransformDraftResponse {
  assistant: {
    provider: AssistantProvider;
    model: string;
    note?: string;
  };
  plan: JsonTransformPlan;
  preview: JsonTransformPreview;
  exportOptions: ConnectorCapability[];
  sourceBinding?: JsonSourceBinding;
  draft?: FlowSpec;
  validation?: FlowSpecValidationResult;
  compiler?: ReturnType<typeof compileFlowSpec>;
  simulation?: ReturnType<typeof simulateFlowSpec>;
}

interface JsonTransformRequest {
  prompt: string;
  samplePayload: unknown;
  tenantId: string;
  config: ControlApiConfig;
  fetchImpl?: typeof fetch;
  name?: string;
  sourceKind?: JsonSourceKind;
  sinkCapabilityId?: SinkCapabilityId;
  sinkConnectorId?: string;
}

interface RawGeminiPlan {
  suggestedName?: unknown;
  summary?: unknown;
  fieldMappings?: unknown;
  filter?: unknown;
  filterSummary?: unknown;
  explanation?: unknown;
  recommendedSinkCapabilityIds?: unknown;
}

interface ResolvedSource {
  kind: "http" | "nats" | "kafka";
  capabilityId: string;
  connectorId: string;
  executionMode: "native" | "adapter";
}

interface ResolvedSink {
  kind: FlowSpec["sinks"][number]["kind"];
  capabilityId: SinkCapabilityId;
  connectorId: string;
  executionMode: "native" | "adapter";
  deliveryGuarantee: FlowSpec["sinks"][number]["deliveryGuarantee"];
}

const SINK_CAPABILITY_IDS: SinkCapabilityId[] = [
  "http_out",
  "nats_out",
  "snowflake_sink",
  "bigquery_sink",
  "s3_sink",
  "kafka_out",
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "flow";
}

function truncateExample(value: unknown): string {
  const raw =
    typeof value === "string" ? value : JSON.stringify(value);
  return raw.length > 48 ? `${raw.slice(0, 45)}...` : raw;
}

function describeJsonType(value: unknown): string {
  if (Array.isArray(value)) return "array";
  if (value === null) return "null";
  return typeof value;
}

function collectLeafPaths(value: unknown, prefix = ""): JsonLeafPath[] {
  if (Array.isArray(value)) {
    return [
      {
        path: prefix || "$",
        leaf: prefix.split(".").at(-1) || "$",
        type: "array",
        example: truncateExample(value.slice(0, 2)),
      },
    ];
  }

  if (!isRecord(value)) {
    return [
      {
        path: prefix || "$",
        leaf: prefix.split(".").at(-1) || "$",
        type: describeJsonType(value),
        example: truncateExample(value),
      },
    ];
  }

  const entries = Object.entries(value);
  if (entries.length === 0) {
    return [
      {
        path: prefix || "$",
        leaf: prefix.split(".").at(-1) || "$",
        type: "object",
        example: "{}",
      },
    ];
  }

  return entries.flatMap(([key, nestedValue]) =>
    collectLeafPaths(nestedValue, prefix ? `${prefix}.${key}` : key),
  );
}

function normalizeToken(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function resolveFieldPath(token: string, leafPaths: JsonLeafPath[]): string | undefined {
  const normalized = normalizeToken(token);
  if (!normalized) return undefined;

  const exact = leafPaths.find((entry) => normalizeToken(entry.path) === normalized);
  if (exact) return exact.path;

  const exactLeaf = leafPaths.find((entry) => normalizeToken(entry.leaf) === normalized);
  if (exactLeaf) return exactLeaf.path;

  const containsLeaf = leafPaths.find((entry) => normalizeToken(entry.leaf).includes(normalized));
  if (containsLeaf) return containsLeaf.path;

  const containsPath = leafPaths.find((entry) => normalizeToken(entry.path).includes(normalized));
  return containsPath?.path;
}

function splitFieldList(raw: string): string[] {
  return raw
    .replace(/\b(the|a|an|only|fields?)\b/gi, "")
    .split(/,| and /i)
    .map((part) => part.trim())
    .filter(Boolean);
}

function createOutputPath(inputPath: string, seen: Set<string>): string {
  const leaf = inputPath.split(".").at(-1) || inputPath;
  if (!seen.has(leaf)) {
    seen.add(leaf);
    return leaf;
  }

  const fallback = inputPath.replace(/\./g, "_");
  seen.add(fallback);
  return fallback;
}

function createPreservedOutputPath(inputPath: string, seen: Set<string>): string {
  if (inputPath !== "$" && !seen.has(inputPath)) {
    seen.add(inputPath);
    return inputPath;
  }

  return createOutputPath(inputPath, seen);
}

function dedupeMappings(mappings: JsonTransformFieldMapping[]): JsonTransformFieldMapping[] {
  const seen = new Set<string>();
  return mappings.filter((mapping) => {
    const key = `${mapping.from}->${mapping.to}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function defaultMappings(samplePayload: unknown): JsonTransformFieldMapping[] {
  if (!isRecord(samplePayload)) {
    return [{ from: "$", to: "value" }];
  }

  return Object.keys(samplePayload).map((key) => ({ from: key, to: key }));
}

function fieldSearchText(entry: JsonLeafPath): string {
  return normalizeToken(`${entry.path} ${entry.leaf}`);
}

function semanticGroupsForToken(token: string): SemanticFieldGroup[] {
  const normalized = normalizeToken(token);
  const groups: SemanticFieldGroup[] = [];
  const push = (group: SemanticFieldGroup) => {
    if (!groups.includes(group)) groups.push(group);
  };

  if (/\becommerce\b|\bcommerce\b|\bshop\b|\bstore\b|\border\b|\bpurchase\b/.test(normalized)) push("ecommerce");
  if (/\bidentity\b|\bidentit|\bcustomer\b|\buser\b|\bvisitor\b|\bperson\b|\bprofile\b|\baccount\b|\bemail\b/.test(normalized)) push("identity");
  if (/\bproduct\b|\bsku\b|\bitem\b|\bvariant\b|\bcatalog\b/.test(normalized)) push("product");
  if (/\bcart\b|\bbasket\b|\bcheckout\b|\bline item\b/.test(normalized)) push("cart");
  if (/\brevenue\b|\bamount\b|\bprice\b|\bvalue\b|\btotal\b|\bcurrency\b|\bpayment\b|\btransaction\b/.test(normalized)) push("revenue");
  if (/\battribution\b|\butm\b|\bcampaign\b|\bmedium\b|\bsource\b|\breferrer\b|\bchannel\b|\bgclid\b|\bfbclid\b|\bad\b|\baffiliate\b/.test(normalized)) push("attribution");

  return groups;
}

function fieldMatchesSemanticGroup(entry: JsonLeafPath, group: SemanticFieldGroup): boolean {
  const text = fieldSearchText(entry);
  switch (group) {
    case "ecommerce":
      return /\bevent\b|\border\b|\bpurchase\b|\btransaction\b|\bcheckout\b|\bcart\b|\bbasket\b|\bproduct\b|\bsku\b|\bitem\b|\bamount\b|\brevenue\b|\bprice\b|\btotal\b|\bcurrency\b|\butm\b|\bcampaign\b|\breferrer\b/.test(text);
    case "identity":
      return /\buser\b|\bcustomer\b|\bclient\b|\bvisitor\b|\banonymous\b|\bperson\b|\bprofile\b|\baccount\b|\bemail\b|\bphone\b|\bname\b|\bloyalty\b/.test(text);
    case "product":
      return /\bproduct\b|\bsku\b|\bitem\b|\bvariant\b|\bcatalog\b|\bcategory\b|\bbrand\b/.test(text);
    case "cart":
      return /\bcart\b|\bbasket\b|\bcheckout\b|\bline item\b|\bitems\b|\bquantity\b|\bqty\b/.test(text);
    case "revenue":
      return /\brevenue\b|\bamount\b|\bprice\b|\bvalue\b|\btotal\b|\bsubtotal\b|\bcurrency\b|\btax\b|\bdiscount\b|\border\b|\btransaction\b|\bpayment\b/.test(text);
    case "attribution":
      return /\butm\b|\bcampaign\b|\bmedium\b|\bsource\b|\breferrer\b|\breferer\b|\bchannel\b|\bgclid\b|\bfbclid\b|\bad\b|\badgroup\b|\bcreative\b|\baffiliate\b/.test(text);
  }
}

function shouldDropFieldForPrompt(prompt: string, path: string): boolean {
  const normalizedPrompt = normalizeToken(prompt);
  if (!/\bdrop\b|\bremove\b|\bexclude\b|\bomit\b/.test(normalizedPrompt)) {
    return false;
  }

  const normalizedPath = normalizeToken(path);
  if (normalizedPrompt.includes("ui") && /\bui\b|\binterface\b|\bscreen\b|\bcomponent\b|\btheme\b/.test(normalizedPath)) {
    return true;
  }
  if (normalizedPrompt.includes("demo") && /\bdemo\b|\bsample\b|\bmock\b|\bfixture\b/.test(normalizedPath)) {
    return true;
  }
  if (normalizedPrompt.includes("metadata") && /\bmetadata\b|\bmeta\b|\bdebug\b|\binternal\b/.test(normalizedPath)) {
    return true;
  }

  return false;
}

function resolveFieldPathsForToken(token: string, leafPaths: JsonLeafPath[]): ResolvedFieldPath[] {
  const groups = semanticGroupsForToken(token);
  if (groups.length > 0) {
    const semanticPaths = leafPaths
      .filter((entry) => groups.some((group) => fieldMatchesSemanticGroup(entry, group)))
      .map((entry) => ({ path: entry.path, preservePath: true }));
    if (semanticPaths.length > 0) {
      const seen = new Set<string>();
      return semanticPaths.filter((entry) => {
        if (seen.has(entry.path)) return false;
        seen.add(entry.path);
        return true;
      });
    }
  }

  const directPath = resolveFieldPath(token, leafPaths);
  return directPath ? [{ path: directPath, preservePath: false }] : [];
}

function preferredSinkIds(prompt: string): SinkCapabilityId[] {
  const normalized = prompt.toLowerCase();
  const ordered: SinkCapabilityId[] = [];

  const push = (value: SinkCapabilityId) => {
    if (!ordered.includes(value)) {
      ordered.push(value);
    }
  };

  if (normalized.includes("snowflake")) push("snowflake_sink");
  if (normalized.includes("bigquery")) push("bigquery_sink");
  if (normalized.includes("s3")) push("s3_sink");
  if (normalized.includes("kafka")) push("kafka_out");
  if (normalized.includes("nats")) push("nats_out");
  if (normalized.includes("http") || normalized.includes("webhook")) push("http_out");

  push("http_out");
  push("nats_out");
  push("snowflake_sink");

  return ordered;
}

function describePredicate(predicate: PredicateExpr | undefined): string | undefined {
  if (!predicate) return undefined;

  switch (predicate.type) {
    case "always":
      return "Keep every event.";
    case "field_exists":
      return `${predicate.path} must exist.`;
    case "field_equals":
      return `${predicate.path} must equal ${JSON.stringify(predicate.value)}.`;
    case "field_contains":
      return `${predicate.path} must contain ${JSON.stringify(predicate.value)}.`;
    case "field_gt":
      return `${predicate.path} must be greater than ${predicate.value}.`;
    case "field_gte":
      return `${predicate.path} must be at least ${predicate.value}.`;
    case "field_lt":
      return `${predicate.path} must be less than ${predicate.value}.`;
    case "field_lte":
      return `${predicate.path} must be at most ${predicate.value}.`;
    case "and":
      return predicate.all.map(describePredicate).filter(Boolean).join(" And ");
    case "or":
      return predicate.any.map(describePredicate).filter(Boolean).join(" Or ");
    case "not": {
      const nested = describePredicate(predicate.predicate);
      return nested ? `Exclude events where ${nested.charAt(0).toLowerCase()}${nested.slice(1)}` : undefined;
    }
  }
}

function normalizePredicate(
  input: unknown,
  availablePaths: Set<string>,
): PredicateExpr | undefined {
  if (!isRecord(input) || typeof input.type !== "string") {
    return undefined;
  }

  const predicateType = input.type;
  switch (predicateType) {
    case "always":
      return { type: "always" };
    case "field_exists":
      return typeof input.path === "string" && availablePaths.has(input.path)
        ? { type: "field_exists", path: input.path }
        : undefined;
    case "field_equals":
      return typeof input.path === "string" && availablePaths.has(input.path)
        ? { type: "field_equals", path: input.path, value: input.value }
        : undefined;
    case "field_contains":
      return typeof input.path === "string" &&
        availablePaths.has(input.path) &&
        typeof input.value === "string"
        ? { type: "field_contains", path: input.path, value: input.value }
        : undefined;
    case "field_gt":
    case "field_gte":
    case "field_lt":
    case "field_lte":
      return typeof input.path === "string" &&
        availablePaths.has(input.path) &&
        typeof input.value === "number"
        ? { type: predicateType, path: input.path, value: input.value }
        : undefined;
    case "not": {
      const predicate = normalizePredicate(input.predicate, availablePaths);
      return predicate ? { type: "not", predicate } : undefined;
    }
    case "and": {
      const predicates = Array.isArray(input.all)
        ? input.all
            .map((entry) => normalizePredicate(entry, availablePaths))
            .filter((entry): entry is PredicateExpr => Boolean(entry))
        : [];
      return predicates.length > 0 ? { type: "and", all: predicates } : undefined;
    }
    case "or": {
      const predicates = Array.isArray(input.any)
        ? input.any
            .map((entry) => normalizePredicate(entry, availablePaths))
            .filter((entry): entry is PredicateExpr => Boolean(entry))
        : [];
      return predicates.length > 0 ? { type: "or", any: predicates } : undefined;
    }
    default:
      return undefined;
  }
}

function heuristicFilter(prompt: string, leafPaths: JsonLeafPath[]): PredicateExpr | undefined {
  const countryPath = leafPaths.find((entry) =>
    ["country", "countrycode", "countryname"].includes(normalizeToken(entry.leaf).replace(/\s+/g, "")),
  )?.path;

  const locationMatch = prompt.match(/filter out .*?(?:living|located|based) in ([\p{L}\s-]+)/iu);
  if (locationMatch?.[1] && countryPath) {
    return {
      type: "not",
      predicate: {
        type: "field_equals",
        path: countryPath,
        value: locationMatch[1].trim(),
      },
    };
  }

  const equalsMatch = prompt.match(/where ([a-z0-9_.\s]+?) equals ([^.,]+)/i);
  if (equalsMatch) {
    const path = resolveFieldPath(equalsMatch[1], leafPaths);
    if (path) {
      return {
        type: "field_equals",
        path,
        value: equalsMatch[2].trim().replace(/^["']|["']$/g, ""),
      };
    }
  }

  return undefined;
}

function buildHeuristicPlan(
  prompt: string,
  samplePayload: unknown,
  explicitName?: string,
): JsonTransformPlan {
  const leafPaths = collectLeafPaths(samplePayload);
  const keepMatch = prompt.match(
    /(?:keep|only keep|preserve|select)\s+(.+?)(?:\s+and\s+filter|\s+where|\s+then|\s+before|\.$|$)/i,
  );
  const mappedFields = new Set<string>();
  const seenOutputs = new Set<string>();

  const fieldMappings = keepMatch
    ? dedupeMappings(
        splitFieldList(keepMatch[1])
          .flatMap((token) => resolveFieldPathsForToken(token, leafPaths))
          .filter((entry) => !shouldDropFieldForPrompt(prompt, entry.path))
          .filter((entry) => {
            if (mappedFields.has(entry.path)) return false;
            mappedFields.add(entry.path);
            return true;
          })
          .map((entry) => {
            return {
              from: entry.path,
              to: entry.preservePath
                ? createPreservedOutputPath(entry.path, seenOutputs)
                : createOutputPath(entry.path, seenOutputs),
            };
          }),
      )
    : defaultMappings(samplePayload);

  const filter = heuristicFilter(prompt, leafPaths);
  const filterSummary = describePredicate(filter);
  const summary = [
    fieldMappings.length > 0
      ? `Keep ${fieldMappings.map((field) => field.to).join(", ")}.`
      : "Pass the payload through unchanged.",
    filterSummary,
  ]
    .filter(Boolean)
    .join(" ");

  return {
    suggestedName: explicitName?.trim() || titleCase(summary || prompt),
    summary: summary || "Transform JSON events based on the sample payload.",
    fieldMappings,
    filter,
    filterSummary,
    explanation: [
      fieldMappings.length > 0
        ? `Projected ${fieldMappings.length} field${fieldMappings.length === 1 ? "" : "s"} into a new JSON object.`
        : "No field projection was requested.",
      filterSummary ?? "No filter rule was inferred from the request.",
    ],
    recommendedSinkCapabilityIds: preferredSinkIds(prompt),
  };
}

function titleCase(value: string): string {
  return value
    .replace(/[.]/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
    .slice(0, 80) || "JSON Transform";
}

function extractJsonDocument(raw: string): string {
  const fencedMatch = raw.match(/```json\s*([\s\S]*?)```/i);
  if (fencedMatch?.[1]) {
    return fencedMatch[1].trim();
  }

  const firstBrace = raw.indexOf("{");
  const lastBrace = raw.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    return raw.slice(firstBrace, lastBrace + 1);
  }

  return raw.trim();
}

async function requestGeminiPlan(
  config: ControlApiConfig,
  fetchImpl: typeof fetch,
  prompt: string,
  samplePayload: unknown,
): Promise<RawGeminiPlan> {
  if (!config.geminiApiKey) {
    throw new Error("Gemini API key is not configured");
  }

  const leafPaths = collectLeafPaths(samplePayload)
    .map((entry) => `- ${entry.path} (${entry.type}) example ${entry.example}`)
    .join("\n");

  const model = config.geminiModel ?? "gemini-2.5-flash";
  const apiBaseUrl = config.geminiApiBaseUrl ?? "https://generativelanguage.googleapis.com/v1beta";
  const response = await fetchImpl(
    `${apiBaseUrl}/models/${model}:generateContent?key=${config.geminiApiKey}`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              {
                text: [
                  "You are building a JSON event transformation for a streaming product.",
                  "Return only JSON with this shape:",
                  JSON.stringify({
                    suggestedName: "string",
                    summary: "string",
                    fieldMappings: [{ from: "source.path", to: "output.path" }],
                    filter: {
                      type: "field_equals|field_contains|field_exists|field_gt|field_gte|field_lt|field_lte|and|or|not|always",
                    },
                    filterSummary: "string",
                    explanation: ["string"],
                    recommendedSinkCapabilityIds: ["http_out"],
                  }),
                  "Rules:",
                  "- Use only field paths from the provided sample.",
                  "- Prefer a projection that keeps only the requested fields.",
                  "- When the user says to filter out events, express that with a not(...) predicate.",
                  `- Allowed sink ids: ${SINK_CAPABILITY_IDS.join(", ")}.`,
                  "Sample JSON:",
                  JSON.stringify(samplePayload, null, 2),
                  "Available field paths:",
                  leafPaths || "- $ (unknown)",
                  "User request:",
                  prompt,
                ].join("\n"),
              },
            ],
          },
        ],
        generationConfig: {
          temperature: 0.2,
        },
      }),
    },
  );

  if (!response.ok) {
    throw new Error(`Gemini request failed with status ${response.status}`);
  }

  const data = (await response.json()) as {
    candidates?: Array<{
      content?: {
        parts?: Array<{ text?: string }>;
      };
    }>;
  };

  const text = data.candidates?.[0]?.content?.parts
    ?.map((part) => part.text ?? "")
    .join("\n")
    .trim();

  if (!text) {
    throw new Error("Gemini returned an empty response");
  }

  return JSON.parse(extractJsonDocument(text)) as RawGeminiPlan;
}

function sanitizeGeminiPlan(
  rawPlan: RawGeminiPlan,
  prompt: string,
  samplePayload: unknown,
  explicitName?: string,
): JsonTransformPlan {
  const leafPaths = collectLeafPaths(samplePayload);
  const availablePaths = new Set(leafPaths.map((entry) => entry.path));
  const seenOutputs = new Set<string>();

  const rawMappings = Array.isArray(rawPlan.fieldMappings) ? rawPlan.fieldMappings : [];
  const fieldMappings = dedupeMappings(
    rawMappings
      .map((entry) => (isRecord(entry) ? entry : null))
      .filter((entry): entry is Record<string, unknown> => Boolean(entry))
      .map((entry) => {
        const fromPath =
          typeof entry.from === "string" && availablePaths.has(entry.from)
            ? entry.from
            : resolveFieldPath(String(entry.from ?? ""), leafPaths);
        if (!fromPath) return null;

        const desiredTo = typeof entry.to === "string" && entry.to.trim().length > 0
          ? entry.to.trim()
          : createOutputPath(fromPath, seenOutputs);

        return {
          from: fromPath,
          to: desiredTo,
        };
      })
      .filter((entry): entry is JsonTransformFieldMapping => Boolean(entry)),
  );

  const fallbackPlan = buildHeuristicPlan(prompt, samplePayload, explicitName);
  const filter = normalizePredicate(rawPlan.filter, availablePaths) ?? fallbackPlan.filter;
  const recommendedSinkCapabilityIds = Array.isArray(rawPlan.recommendedSinkCapabilityIds)
    ? rawPlan.recommendedSinkCapabilityIds
        .filter((entry): entry is SinkCapabilityId => typeof entry === "string" && SINK_CAPABILITY_IDS.includes(entry as SinkCapabilityId))
    : fallbackPlan.recommendedSinkCapabilityIds;

  return {
    suggestedName:
      explicitName?.trim() ||
      (typeof rawPlan.suggestedName === "string" && rawPlan.suggestedName.trim().length > 0
        ? rawPlan.suggestedName.trim()
        : fallbackPlan.suggestedName),
    summary:
      typeof rawPlan.summary === "string" && rawPlan.summary.trim().length > 0
        ? rawPlan.summary.trim()
        : fallbackPlan.summary,
    fieldMappings: fieldMappings.length > 0 ? fieldMappings : fallbackPlan.fieldMappings,
    filter,
    filterSummary:
      typeof rawPlan.filterSummary === "string" && rawPlan.filterSummary.trim().length > 0
        ? rawPlan.filterSummary.trim()
        : describePredicate(filter),
    explanation: Array.isArray(rawPlan.explanation)
      ? rawPlan.explanation.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
      : fallbackPlan.explanation,
    recommendedSinkCapabilityIds:
      recommendedSinkCapabilityIds.length > 0 ? recommendedSinkCapabilityIds : fallbackPlan.recommendedSinkCapabilityIds,
  };
}

function resolveSource(kind: JsonSourceKind): ResolvedSource {
  switch (kind) {
    case "kafka":
      return {
        kind: "kafka",
        capabilityId: "kafka_in",
        connectorId: "kafka_in_default",
        executionMode: "adapter",
      };
    case "nats":
      return {
        kind: "nats",
        capabilityId: "nats_in",
        connectorId: "nats_in_default",
        executionMode: "native",
      };
    default:
      return {
        kind: "http",
        capabilityId: "http_in",
        connectorId: "http_in_default",
        executionMode: "native",
      };
  }
}

function resolveSink(capabilityId: SinkCapabilityId): ResolvedSink {
  switch (capabilityId) {
    case "snowflake_sink":
      return {
        kind: "snowflake",
        capabilityId,
        connectorId: "snowflake_sink_default",
        executionMode: "adapter",
        deliveryGuarantee: "idempotent",
      };
    case "bigquery_sink":
      return {
        kind: "bigquery",
        capabilityId,
        connectorId: "bigquery_sink_default",
        executionMode: "adapter",
        deliveryGuarantee: "append_only",
      };
    case "s3_sink":
      return {
        kind: "s3",
        capabilityId,
        connectorId: "s3_sink_default",
        executionMode: "adapter",
        deliveryGuarantee: "append_only",
      };
    case "kafka_out":
      return {
        kind: "kafka",
        capabilityId,
        connectorId: "kafka_out_default",
        executionMode: "adapter",
        deliveryGuarantee: "append_only",
      };
    case "nats_out":
      return {
        kind: "nats",
        capabilityId,
        connectorId: "nats_out_default",
        executionMode: "native",
        deliveryGuarantee: "idempotent",
      };
    default:
      return {
        kind: "http",
        capabilityId: "http_out",
        connectorId: "http_out_default",
        executionMode: "native",
        deliveryGuarantee: "best_effort",
      };
  }
}

function applyProjection(
  payload: unknown,
  fieldMappings: JsonTransformFieldMapping[],
): unknown {
  if (fieldMappings.length === 0) {
    return payload;
  }

  let projected: unknown = {};
  for (const mapping of fieldMappings) {
    const value = getPath(payload, mapping.from);
    if (value !== undefined) {
      projected = setPath(projected, mapping.to, value);
    }
  }

  return projected;
}

function previewTransform(
  samplePayload: unknown,
  plan: JsonTransformPlan,
): JsonTransformPreview {
  if (plan.filter && !evaluatePredicate(plan.filter, samplePayload)) {
    return {
      accepted: false,
      droppedReason: plan.filterSummary ?? "The sample event would be filtered out.",
      notes: [
        "The sample does not pass the current filter rule.",
      ],
    };
  }

  const output = applyProjection(samplePayload, plan.fieldMappings);
  return {
    accepted: true,
    output,
    notes: [
      plan.fieldMappings.length > 0
        ? `Projected ${plan.fieldMappings.length} field${plan.fieldMappings.length === 1 ? "" : "s"} into the preview output.`
        : "No projection was applied.",
      plan.filterSummary ?? "No filter rule was applied.",
    ],
  };
}

function orderExportOptions(recommended: SinkCapabilityId[]): ConnectorCapability[] {
  const sinks = CONNECTOR_CAPABILITIES.filter(
    (capability) => capability.kind === "sink",
  );

  return [...sinks].sort((left, right) => {
    const leftIndex = recommended.indexOf(left.id as SinkCapabilityId);
    const rightIndex = recommended.indexOf(right.id as SinkCapabilityId);
    const resolvedLeft = leftIndex === -1 ? Number.MAX_SAFE_INTEGER : leftIndex;
    const resolvedRight = rightIndex === -1 ? Number.MAX_SAFE_INTEGER : rightIndex;
    return resolvedLeft - resolvedRight || left.name.localeCompare(right.name);
  });
}

function buildDraftFromPlan(input: {
  tenantId: string;
  name: string;
  sourceKind: JsonSourceKind;
  sinkCapabilityId: SinkCapabilityId;
  sinkConnectorId?: string;
  plan: JsonTransformPlan;
}): FlowSpec {
  const source = resolveSource(input.sourceKind);
  const sink = resolveSink(input.sinkCapabilityId);
  const flowId = `flow_${slugify(input.name)}`;
  const revisionId = `rev_${slugify(input.name)}_v1`;

  const processors: FlowSpec["processors"] = [];
  let sourceNextNodeIds = ["route_terminal"];
  let routeFromNodeId = "route_terminal";

  if (input.plan.fieldMappings.length > 0) {
    processors.push({
      id: "processor_project",
      kind: "map",
      mode: "project",
      mappings: input.plan.fieldMappings,
      nextNodeIds: ["route_terminal"],
    });
    sourceNextNodeIds = ["processor_project"];
    routeFromNodeId = "processor_project";
  }

  if (input.plan.filter) {
    processors.unshift({
      id: "processor_filter",
      kind: "filter",
      predicate: input.plan.filter,
      nextNodeIds: [processors[0]?.id ?? "route_terminal"],
    });
    sourceNextNodeIds = ["processor_filter"];
    routeFromNodeId = processors.at(-1)?.id ?? "route_terminal";
  }

  return {
    version: 1,
    metadata: {
      tenantId: input.tenantId,
      flowId,
      revisionId,
      name: input.name,
      description: input.plan.summary,
      tags: ["json", "assistant-authored", source.executionMode, sink.executionMode],
    },
    sources: [
      {
        id: "source_primary",
        kind: source.kind,
        connector: {
          capabilityId: source.capabilityId,
          connectorId: source.connectorId,
          executionMode: source.executionMode,
        },
        stream: "ingress",
        nextNodeIds: sourceNextNodeIds,
      },
    ],
    processors,
    routes: [
      {
        id: "route_terminal",
        fromNodeId: routeFromNodeId,
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
          connectorId: input.sinkConnectorId?.trim() || sink.connectorId,
          executionMode: sink.executionMode,
        },
        deliveryGuarantee: sink.deliveryGuarantee,
        stream: "work",
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
}

export async function buildJsonTransformAssistantResult(
  input: JsonTransformRequest,
): Promise<JsonTransformDraftResponse> {
  const prompt = input.prompt.trim();
  const fetchImpl = input.fetchImpl ?? fetch;
  const sourceKind = input.sourceKind ?? "http";
  const fallbackPlan = buildHeuristicPlan(prompt, input.samplePayload, input.name);

  let provider: AssistantProvider = "heuristic";
  let model = "heuristic-local";
  let note: string | undefined;
  let plan = fallbackPlan;

  if (input.config.geminiApiKey) {
    try {
      const rawPlan = await requestGeminiPlan(input.config, fetchImpl, prompt, input.samplePayload);
      plan = sanitizeGeminiPlan(rawPlan, prompt, input.samplePayload, input.name);
      provider = "gemini";
      model = input.config.geminiModel ?? "gemini-2.5-flash";
    } catch (error) {
      note = error instanceof Error ? error.message : "Gemini planning failed; using heuristic fallback.";
    }
  }

  const preview = previewTransform(input.samplePayload, plan);
  const exportOptions = orderExportOptions(plan.recommendedSinkCapabilityIds);
  const response: JsonTransformDraftResponse = {
    assistant: {
      provider,
      model,
      note,
    },
    plan,
    preview,
    exportOptions,
  };

  if (!input.sinkCapabilityId) {
    return response;
  }

  const draft = buildDraftFromPlan({
    tenantId: input.tenantId,
    name: input.name?.trim() || plan.suggestedName,
    sourceKind,
    sinkCapabilityId: input.sinkCapabilityId,
    sinkConnectorId: input.sinkConnectorId,
    plan,
  });
  const validation = validateFlowSpec(draft);
  const compiler = compileFlowSpec(draft);
  const simulation = simulateFlowSpec(draft, [
    {
      envelope: {},
      payload: input.samplePayload,
      sourceId: draft.sources[0]?.id,
    },
  ]);

  return {
    ...response,
    draft,
    validation,
    compiler,
    simulation,
  };
}
