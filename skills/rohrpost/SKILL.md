---
name: rohrpost
description: Create, validate, publish, and inspect Rohrpost event-routing flows from plain-language requests. Use when the user asks to send website, webhook, HTTP, NATS, or Kafka event data to S3, BigQuery, Snowflake, Kafka, NATS, or HTTP destinations with Rohrpost, especially when they want to avoid configuring flows in the UI.
compatibility: Requires network access to a running Rohrpost control-api and an API token.
---

# Rohrpost Flow Authoring

Use this skill when the user wants Rohrpost to route event data without hand-building the flow in the UI.

## Inputs to collect

Ask only for missing information:

- Control API base URL. Default to `ROHRPOST_API_BASE_URL`, then `http://localhost:3001`.
- API token. Use `ROHRPOST_API_TOKEN` if set; otherwise ask the user.
- Source type: `http`, `nats`, or `kafka`. Default to `http` for website/webhook/app events.
- Destination: one of `s3_sink`, `bigquery_sink`, `snowflake_sink`, `kafka_out`, `nats_out`, or `http_out`.
- A small sample JSON payload. If unavailable, ask for one before publishing.
- Destination connector details if the sink is not already configured.

Do not imply Rohrpost can collect data from websites the user does not control. For website data, create an HTTP ingress flow and tell the user their site or tag manager must POST authorized event payloads to the generated ingress endpoint.

## API calls

Use bearer auth for every request:

```bash
BASE_URL="${ROHRPOST_API_BASE_URL:-http://localhost:3001}"
TOKEN="${ROHRPOST_API_TOKEN:?set ROHRPOST_API_TOKEN or ask the user}"
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
```

Check the backend first:

```bash
curl -fsS "$BASE_URL/health"
curl -fsS "${AUTH[@]}" "$BASE_URL/api/capabilities"
```

For prompt-based JSON routing, prefer `compose-json-transform` because it returns a draft, preview, validation details, and source binding:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/flows/compose-json-transform" \
  -d '{
    "name": "Website events to S3",
    "prompt": "Send website analytics events to S3, preserving useful ecommerce fields.",
    "sourceKind": "http",
    "sinkCapabilityId": "s3_sink",
    "samplePayload": {
      "event": "purchase",
      "orderId": "ord_123",
      "email": "customer@example.com",
      "total": 42
    }
  }'
```

Before publishing, make sure the destination connector exists when a specific connector is needed:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/api/connectors?capabilityId=s3_sink"
```

Create or update a sink connector only when the user gave the destination details:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/connectors" \
  -d '{
    "name": "Production S3",
    "capabilityId": "s3_sink",
    "config": {
      "bucket": "example-bucket",
      "prefix": "events/"
    }
  }'
```

If a connector id is supplied or created, either pass it to `compose-json-transform` as `sinkConnectorId` or patch the returned draft sink connector id before saving.

Save and publish the validated draft:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/flows" \
  -d '{
    "name": "Website events to S3",
    "samplePayload": {"event":"purchase","orderId":"ord_123","total":42},
    "sourceBinding": { "...": "use the sourceBinding returned by compose-json-transform" },
    "spec": { "...": "use the draft returned by compose-json-transform" }
  }'
```

Then publish the returned revision:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/flows/<flowId>/publish" \
  -d '{"revisionId":"<revisionId>"}'
```

## Response to the user

Keep the result short:

- Flow name, flow id, revision id, and deployment id.
- Generated source binding, such as `/ingest/website-events-to-s3`, NATS subject, or Kafka topic.
- Destination connector used.
- One test command for sending a sample event to the generated ingress.
- Any remaining manual step, usually adding credentials to the connector config or wiring the website to POST events.

If the backend is unreachable, stop and report the failed URL and status. Do not invent a flow id or endpoint.
