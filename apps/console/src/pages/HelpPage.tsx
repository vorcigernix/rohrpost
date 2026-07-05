import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { Divider } from '@astryxdesign/core/Divider';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { List, ListItem } from '@astryxdesign/core/List';
import { Section } from '@astryxdesign/core/Section';
import { StackItem } from '@astryxdesign/core/Stack';
import { Table, pixel, proportional, type TableColumn } from '@astryxdesign/core/Table';
import { Text } from '@astryxdesign/core/Text';
import { Token } from '@astryxdesign/core/Token';
import { VStack } from '@astryxdesign/core/VStack';
import {
  BeakerIcon,
  BoltIcon,
  CircleStackIcon,
  Cog6ToothIcon,
  InboxIcon,
  PlayCircleIcon,
  RectangleGroupIcon,
  SparklesIcon,
} from '@heroicons/react/24/outline';
import { Icon } from '@astryxdesign/core/Icon';
import { ActionLink, PageHeader, Panel, PanelHeader, Pill, Pipeline } from '../components/ui';

const EXAMPLE_FLOW_SPEC = `{
  "name": "storefront-to-warehouse",
  "sources": [
    { "id": "http_in", "kind": "http", "path": "/ingest/storefront" }
  ],
  "processors": [
    { "id": "drop_ui_events", "kind": "filter",
      "predicate": { "op": "field_exists", "path": "event_name" } },
    { "id": "normalize", "kind": "map", "mode": "merge",
      "mappings": [{ "from": "eventName", "to": "event_name" }] },
    { "id": "attach_segment", "kind": "enrich_lookup",
      "keyPath": "customer_id", "targetPath": "customer.enrichment" }
  ],
  "sinks": [
    { "id": "warehouse", "kind": "snowflake",
      "table": "ANALYTICS.EVENTS_INGEST", "guarantee": "idempotent" }
  ]
}`;

interface PageGuideRow extends Record<string, unknown> {
  id: string;
  page: string;
  route: string;
  icon: typeof BoltIcon;
  purpose: string;
}

const PAGE_GUIDE: PageGuideRow[] = [
  { id: 'demo', page: 'Demo storefront', route: '/demo', icon: BeakerIcon, purpose: 'Generate realistic ecommerce events (view, cart, checkout, purchase) to use as design samples for a flow.' },
  { id: 'flows', page: 'Flows', route: '/flows', icon: RectangleGroupIcon, purpose: 'Every flow with its pipeline, state, throughput, and backlog. Click through for the workflow graph, runs, revisions, and DLQ.' },
  { id: 'compose', page: 'Compose flow', route: '/compose', icon: SparklesIcon, purpose: 'The visual builder: pick an ingest source, choose a sample event, generate or hand-tune the transform, select a destination, publish.' },
  { id: 'pulse', page: 'Pulse', route: '/pulse', icon: BoltIcon, purpose: 'Live operations dashboard: throughput, delivered, backlog, error rate, DLQ totals, and the flows that need attention.' },
  { id: 'inbox', page: 'Inbox', route: '/inbox', icon: InboxIcon, purpose: 'Incident feed: DLQ events, degraded flows, schema rejects, and deploys, with replay and diagnosis actions.' },
  { id: 'runs', page: 'Runs', route: '/runs', icon: PlayCircleIcon, purpose: 'Runtime health per deployment: accepted, delivered, backlog, inflight counts, plus adapter-managed workloads.' },
  { id: 'settings', page: 'Settings', route: '/setup', icon: Cog6ToothIcon, purpose: 'Connect the AI provider used by Compose. Without a token, Compose falls back to the deterministic local planner.' },
  { id: 'capabilities', page: 'Capabilities', route: '/capabilities', icon: CircleStackIcon, purpose: 'Reference matrix of native and adapter connectors, and the delivery guarantees that shape retry policy.' },
];

const pageGuideColumns: TableColumn<PageGuideRow>[] = [
  {
    key: 'page',
    header: 'Page',
    width: pixel(220),
    renderCell: (row) => (
      <HStack gap={2} align="center">
        <Icon icon={row.icon} size="sm" />
        <Text type="body" weight="semibold">{row.page}</Text>
      </HStack>
    ),
  },
  {
    key: 'purpose',
    header: 'What it does',
    width: proportional(1),
    renderCell: (row) => (
      <Text type="supporting" color="secondary">{row.purpose}</Text>
    ),
  },
];

export function HelpPage() {
  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Help"
        title="How Rohrpost works"
        sub="A five-minute tour of the concepts behind flows, the runtime, and the console."
        actions={
          <ActionLink to="/capabilities" variant="secondary">
            Capabilities reference
          </ActionLink>
        }
      />

      <Section variant="muted" padding={6}>
        <VStack gap={3}>
          <Heading level={2}>What is a flow?</Heading>
          <Text type="body" color="secondary" display="block" maxLines={0}>
            A flow is the unit of work in Rohrpost: events enter through a source, pass through
            an ordered chain of processors, and land in a sink. You author the flow once; the
            platform handles delivery, retries, ordering, and observability.
          </Text>
          <Pipeline
            variant="hero"
            nodes={[
              { role: 'source', kind: 'http', label: 'Source', meta: 'HTTP · NATS · Kafka' },
              { role: 'processor', kind: 'filter', label: 'Filter', meta: 'drop what you don’t need' },
              { role: 'processor', kind: 'map', label: 'Transform', meta: 'reshape fields' },
              { role: 'processor', kind: 'enrich_lookup', label: 'Enrich', meta: 'attach reference data' },
              { role: 'sink', kind: 'snowflake', label: 'Sink', meta: 'warehouse · queue · API' },
            ]}
          />
        </VStack>
      </Section>

      <HStack gap={3} align="start" wrap="wrap">
        <StackItem size="fill">
          <Panel>
            <VStack gap={3}>
              <PanelHeader eyebrow="Lifecycle" title="From event to insight" />
              <List listStyle="decimal" density="balanced">
                <ListItem
                  label="Connect the AI provider"
                  description={<Text type="supporting" color="secondary" display="block">Settings stores the Gemini token on the control plane. Compose uses it to draft transforms; without it, a deterministic local planner takes over.</Text>}
                />
                <ListItem
                  label="Capture sample events"
                  description={<Text type="supporting" color="secondary" display="block">Use the Demo storefront (or a live source) to capture real payloads. One event becomes the design sample for the transform.</Text>}
                />
                <ListItem
                  label="Compose the flow"
                  description={<Text type="supporting" color="secondary" display="block">Pick the ingest kind, describe the desired output in plain language, review the generated processing steps, choose a destination, and publish.</Text>}
                />
                <ListItem
                  label="Publish a revision"
                  description={<Text type="supporting" color="secondary" display="block">Publishing creates an immutable revision and deploys it to the runtime. Edits create new revisions; rollback re-activates an old one.</Text>}
                />
                <ListItem
                  label="Operate"
                  description={<Text type="supporting" color="secondary" display="block">Pulse shows throughput and backlog, Inbox surfaces incidents with replay actions, Runs details every deployment and adapter workload.</Text>}
                />
              </List>
            </VStack>
          </Panel>
        </StackItem>

        <StackItem>
          <VStack gap={3} width={380}>
            <Panel>
              <VStack gap={3}>
                <PanelHeader eyebrow="Execution" title="Native and adapter" />
                <Text type="supporting" color="secondary" display="block">
                  Some flows run natively, in-process on the Rohrpost runtime with per-partition-key
                  ordering. Others proxy through adapters — Redpanda Connect workloads supervised by
                  the adapter plane — to reach systems like Kafka, Snowflake, or BigQuery. Both kinds
                  are authored and observed the same way.
                </Text>
                <HStack gap={1} wrap="wrap">
                  <Token label="native" color="blue" />
                  <Token label="adapter" />
                </HStack>
              </VStack>
            </Panel>

            <Panel>
              <VStack gap={3}>
                <PanelHeader eyebrow="Delivery" title="Guarantees and the DLQ" />
                <Text type="supporting" color="secondary" display="block">
                  Every sink declares a delivery guarantee, and the retry policy follows from it:
                  idempotent sinks retry aggressively, append-only sinks retry cautiously to avoid
                  duplicates, best-effort sinks drop on failure. Events that exhaust their retries
                  land in the flow’s dead-letter queue, where the Inbox lets you inspect and replay
                  them.
                </Text>
                <HStack gap={1} wrap="wrap">
                  <Pill tone="good">idempotent</Pill>
                  <Pill tone="warn">append-only</Pill>
                  <Pill tone="info">best-effort</Pill>
                </HStack>
              </VStack>
            </Panel>
          </VStack>
        </StackItem>
      </HStack>

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Under the hood" title="The flow spec" />
          <Text type="supporting" color="secondary" display="block">
            Everything the builder produces is a declarative spec. You can inspect it on any flow’s
            Configuration tab, or edit individual nodes as JSON from the workflow graph.
          </Text>
          <CodeBlock
            title="storefront-to-warehouse.json"
            code={EXAMPLE_FLOW_SPEC}
            language="json"
            size="sm"
            width="100%"
          />
        </VStack>
      </Panel>

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Console guide" title="Where to find things" />
          <Table<PageGuideRow>
            data={PAGE_GUIDE}
            columns={pageGuideColumns}
            idKey="id"
            density="compact"
            hasHover
          />
        </VStack>
      </Panel>

      <Divider />

      <HStack gap={2} wrap="wrap">
        <ActionLink to="/demo" variant="primary">Start with demo events</ActionLink>
        <ActionLink to="/compose" variant="secondary">Open the flow builder</ActionLink>
        <ActionLink to="/capabilities" variant="ghost">Browse capabilities</ActionLink>
      </HStack>
    </VStack>
  );
}
