import {
  controlApiPaths,
  type ControlApiCapabilitiesResponse,
} from '@rohrpost/control-api-contracts';
import type { CapabilityData, CapabilityRecord } from '../../lib/api-types';
import { requestJson } from '../../lib/api-base';

export type LiveCapability = ControlApiCapabilitiesResponse['native'][number];

export function mapCapability(capability: LiveCapability): CapabilityRecord {
  return {
    id: capability.id,
    label: capability.name,
    execution: capability.executionMode,
    mode: capability.kind === 'processor' ? 'both' : capability.kind,
    status: 'ready',
    notes: [
      `Streams: ${capability.streamCompatibility.join(', ')}`,
      capability.adapterManaged ? 'Managed by adapter runtime' : 'Managed by native runtime',
    ],
  };
}

function mapCapabilities(data: ControlApiCapabilitiesResponse): CapabilityData {
  return {
    native: data.native.map(mapCapability),
    adapter: data.adapter.map(mapCapability),
    sinkGuarantees: [
      {
        guarantee: 'idempotent',
        label: 'Idempotent',
        detail: 'Retries are allowed because repeated writes are safe.',
        retryPolicy: 'Default for deterministic sinks.',
      },
      {
        guarantee: 'append_only',
        label: 'Append only',
        detail: 'Retries should remain conservative to avoid duplicate output records.',
        retryPolicy: 'Prefer one attempt or upstream dedupe.',
      },
      {
        guarantee: 'best_effort',
        label: 'Best effort',
        detail: 'Unsafe to blindly retry because sink writes may duplicate or drift.',
        retryPolicy: 'Validation blocks aggressive retry policies.',
      },
    ],
  };
}

export async function fetchCapabilities(): Promise<CapabilityData> {
  return mapCapabilities(await requestJson<ControlApiCapabilitiesResponse>(controlApiPaths.capabilities()));
}
