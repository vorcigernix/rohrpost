import { Card } from '@astryxdesign/core/Card';
import { Grid } from '@astryxdesign/core/Grid';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { Section } from '@astryxdesign/core/Section';
import { StackItem } from '@astryxdesign/core/Stack';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { BookOpenIcon, BuildingStorefrontIcon, Cog6ToothIcon } from '@heroicons/react/24/outline';
import { ActionLink, Panel, Pill, Pipeline } from '../components/ui';

const STEPS = [
  {
    num: '01 · first',
    title: 'Configure AI',
    copy: 'Add the provider token used to draft transforms and explain event shapes.',
    isCurrent: true,
  },
  {
    num: '02 · events',
    title: 'Generate source events',
    copy: 'Use the demo storefront to capture cart, checkout, and purchase payloads.',
  },
  {
    num: '03 · build',
    title: 'Compose a flow',
    copy: 'Turn the payloads into source, transform, and sink logic before publishing.',
  },
  {
    num: '04 · operate',
    title: 'Monitor runtime',
    copy: 'Use Pulse, Inbox, and Runs to watch throughput, failures, and replays.',
  },
];

export function WelcomePage() {
  return (
    <VStack gap={5}>
      <Section variant="muted" padding={8}>
        <VStack gap={3} maxWidth={760}>
          <Text type="supporting" color="secondary" weight="semibold" display="block">
            ROHRPOST · PREVIEW
          </Text>
          <Heading level={1} type="display-2">Move every event in milliseconds.</Heading>
          <Text type="large" color="secondary" display="block">
            Start with provider setup, generate sample storefront events, compose the transform,
            publish the flow, then watch the runtime from Pulse.
          </Text>
          <HStack gap={2} wrap="wrap">
            <ActionLink to="/setup" variant="primary" size="lg" icon={Cog6ToothIcon}>
              Configure AI provider
            </ActionLink>
            <ActionLink to="/demo" variant="ghost" size="lg" icon={BuildingStorefrontIcon}>
              Generate demo events
            </ActionLink>
            <ActionLink to="/help" variant="ghost" size="lg" icon={BookOpenIcon}>
              How it works
            </ActionLink>
          </HStack>
        </VStack>
      </Section>

      <Grid columns={{ minWidth: 220, max: 4 }} gap={3}>
        {STEPS.map((step) => (
          <Card key={step.num} variant={step.isCurrent ? 'default' : 'muted'}>
            <VStack gap={1.5}>
              <Text type="code" color="secondary" display="block">{step.num}</Text>
              <Heading level={3}>{step.title}</Heading>
              <Text type="supporting" color="secondary" display="block">{step.copy}</Text>
            </VStack>
          </Card>
        ))}
      </Grid>

      <Panel>
        <HStack gap={5} align="center" wrap="wrap">
          <VStack gap={2} maxWidth={420}>
            <Text type="supporting" color="secondary" weight="semibold" display="block">
              UNDER THE HOOD
            </Text>
            <Heading level={2}>Mixed execution, one abstraction.</Heading>
            <Text type="supporting" color="secondary" display="block">
              Some flows run natively on the Rohrpost runtime. Others proxy through adapters to systems
              you already trust — Kafka, Snowflake, BigQuery. You author and observe them the same
              way.
            </Text>
            <HStack gap={1} wrap="wrap">
              <Pill tone="info">native</Pill>
              <Pill tone="neutral">adapter</Pill>
              <Pill tone="good">idempotent</Pill>
              <Pill tone="warn">append-only</Pill>
            </HStack>
          </VStack>
          <StackItem size="fill">
            <Pipeline
              variant="hero"
              nodes={[
                { role: 'source', kind: 'http', label: 'HTTP /ingest/storefront', meta: 'tenant_demo' },
                { role: 'processor', kind: 'filter', label: 'drop demo UI events' },
                { role: 'processor', kind: 'map', label: 'rename → snake_case' },
                { role: 'processor', kind: 'enrich_static', label: 'attach tenant_id' },
                { role: 'sink', kind: 'snowflake', label: 'ANALYTICS.EVENTS_INGEST', meta: 'idempotent' },
              ]}
            />
          </StackItem>
        </HStack>
      </Panel>
    </VStack>
  );
}
