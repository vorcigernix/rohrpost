import type { ControlApiClient } from "./control-api";
import type { RuntimeTarget } from "./runtime-targets";
import { buildDesiredStateFromControlApi, buildRuntimeSnapshot } from "./snapshots";

export async function loadRuntimeSnapshot(
  client: ControlApiClient,
  tenantId: string,
  targets: RuntimeTarget[],
) {
  const [overview, flows] = await Promise.all([
    client.fetchOverview(),
    client.fetchFlows(),
  ]);

  return buildRuntimeSnapshot({
    tenantId,
    targets,
    controlApi: overview,
    flows,
  });
}

export async function loadDesiredState(
  client: ControlApiClient,
  tenantId: string,
) {
  const flows = await client.fetchFlows();
  return buildDesiredStateFromControlApi(flows, tenantId);
}
