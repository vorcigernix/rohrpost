import { useQuery } from '@tanstack/react-query';
import { Banner } from '@astryxdesign/core/Banner';
import { HStack } from '@astryxdesign/core/HStack';
import { Table, proportional, type TableColumn } from '@astryxdesign/core/Table';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { api } from '../lib/api';
import { describeConsoleError } from '../lib/error-state';
import { LoadingBlock, PageHeader, Panel, PanelHeader, Pill, StatusDot } from '../components/ui';

function guaranteeTone(guarantee: string): 'good' | 'info' | 'warn' {
  switch (guarantee) {
    case 'idempotent': return 'good';
    case 'append_only': return 'warn';
    default: return 'info';
  }
}

function capabilityTone(status: string): 'good' | 'warn' {
  return status === 'ready' ? 'good' : 'warn';
}

interface CapabilityRow extends Record<string, unknown> {
  id: string;
  label: string;
  mode: string;
  status: string;
  notes: string[];
}

interface GuaranteeRow extends Record<string, unknown> {
  guarantee: string;
  label: string;
  detail: string;
  retryPolicy: string;
}

const capabilityColumns: TableColumn<CapabilityRow>[] = [
  {
    key: 'capability',
    header: 'Capability',
    width: proportional(1.4),
    renderCell: (c) => (
      <VStack gap={0.5}>
        <Text type="body" weight="semibold" display="block" maxLines={1}>{c.label}</Text>
        <Text type="supporting" color="secondary" display="block" maxLines={1}>{c.id}</Text>
      </VStack>
    ),
  },
  {
    key: 'mode',
    header: 'Mode',
    width: proportional(0.6),
    renderCell: (c) => <Text type="body" color="secondary">{c.mode}</Text>,
  },
  {
    key: 'status',
    header: 'Status',
    width: proportional(0.7),
    renderCell: (c) => (
      <HStack gap={1.5} align="center">
        <StatusDot tone={capabilityTone(c.status)} />
        <Text type="body">{c.status}</Text>
      </HStack>
    ),
  },
  {
    key: 'notes',
    header: 'Notes',
    width: proportional(1.3),
    renderCell: (c) => (
      <Text type="supporting" color="secondary" maxLines={2}>{c.notes.join(' · ')}</Text>
    ),
  },
];

const guaranteeColumns: TableColumn<GuaranteeRow>[] = [
  {
    key: 'guarantee',
    header: 'Guarantee',
    width: proportional(0.7),
    renderCell: (row) => <Pill tone={guaranteeTone(row.guarantee)}>{row.guarantee}</Pill>,
  },
  {
    key: 'meaning',
    header: 'Meaning',
    width: proportional(1.4),
    renderCell: (row) => (
      <VStack gap={0.5}>
        <Text type="body" weight="semibold" display="block" maxLines={1}>{row.label}</Text>
        <Text type="supporting" color="secondary" display="block" maxLines={2}>{row.detail}</Text>
      </VStack>
    ),
  },
  {
    key: 'retryPolicy',
    header: 'Retry policy',
    width: proportional(1.2),
    renderCell: (row) => <Text type="supporting" color="secondary">{row.retryPolicy}</Text>,
  },
];

export function CapabilitiesPage() {
  const capabilitiesQuery = useQuery({ queryKey: ['capabilities'], queryFn: api.fetchCapabilities });

  if (capabilitiesQuery.isPending) {
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Capabilities" title="Execution matrix" sub="Loading connectors and sink policies." />
        <Panel><LoadingBlock lines={4} /></Panel>
      </VStack>
    );
  }

  if (capabilitiesQuery.isError) {
    const errorState = describeConsoleError(capabilitiesQuery.error);
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Capabilities" title="Execution matrix" />
        <Banner status="error" title={errorState.message} description={errorState.hint} />
      </VStack>
    );
  }

  const capabilities = capabilitiesQuery.data;

  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Capabilities"
        title="Execution matrix"
        sub="Native connectors run in-process. Adapter connectors execute externally. Delivery guarantees shape retry policy."
        actions={
          <>
            <Pill tone="good">{capabilities.native.length} native</Pill>
            <Pill tone="info">{capabilities.adapter.length} adapter</Pill>
          </>
        }
      />

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Native" title="Runtime connectors" />
          <Text type="supporting" color="secondary" display="block">First-party, in-process execution.</Text>
          <Table<CapabilityRow>
            data={capabilities.native.map((c) => ({ ...c }))}
            columns={capabilityColumns}
            idKey="id"
            density="compact"
          />
        </VStack>
      </Panel>

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Adapter" title="External connectors" />
          <Text type="supporting" color="secondary" display="block">Redpanda Connect-backed, out-of-process.</Text>
          <Table<CapabilityRow>
            data={capabilities.adapter.map((c) => ({ ...c }))}
            columns={capabilityColumns}
            idKey="id"
            density="compact"
          />
        </VStack>
      </Panel>

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Sink policy" title="Delivery guarantees" />
          <Text type="supporting" color="secondary" display="block">Retry behavior must match the sink contract.</Text>
          <Table<GuaranteeRow>
            data={capabilities.sinkGuarantees.map((s) => ({ ...s }))}
            columns={guaranteeColumns}
            idKey="guarantee"
            density="compact"
          />
        </VStack>
      </Panel>
    </VStack>
  );
}
