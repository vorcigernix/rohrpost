# Monorepo Domain Layout

This document defines the target package/app layout for `rohrpost` without introducing any new platform technology.

## Principles

- Keep **apps** as composition roots and deployable/runtime units.
- Extract only **pure domain code** and **shared contracts** into `packages/*`.
- Keep persistence, framework wiring, transport clients, and probes app-local until their boundaries are proven.
- Prefer a few meaningful domain packages over many tiny ones.

## Target Shape

```text
apps/
  control-api          # Elysia + SQLite control-plane composition root
  router-workers       # NATS / JetStream runtime composition root
  runtime-manager      # rollout reconciliation composition root
  adapter-redpanda     # adapter runtime composition root
  console              # UI composition root

packages/
  shared-flow-spec     # FlowSpec DSL, compiler, simulator, validation
  control-api-contracts# typed HTTP contract surface for control-api
  domain-connectors    # connector capability catalog + source binding
  domain-flows         # future: pure flow/deployment/replay domain types and helpers
  domain-runtime       # future: pure runtime summary/replay domain helpers
```

## What Belongs In Packages

Good package candidates:

- immutable domain types
- pure mapping/validation logic
- capability registries
- shared request/response contracts
- code reused by more than one app

What stays app-local:

- Elysia routes and app setup
- SQLite schema creation and repositories
- NATS clients and JetStream wiring
- runtime health probes
- adapter process supervision
- UI feature wiring

## First Extraction Done

The first domain extraction after contracts is:

- `packages/domain-connectors`

It owns:

- connector capability catalog
- authored source-binding generation

Files moved conceptually:

- `apps/control-api/src/catalog.ts`
- `apps/control-api/src/source-binding.ts`

Those concerns are pure domain code and do not need to live inside `control-api`.

## Next Recommended Moves

### 1. Keep `control-api` split app-local before packaging more code

Do this inside `apps/control-api/src/` first:

- `auth/`
- `catalog/`
- `authoring/`
- `flows/`
- `runtime/`
- `operations/`

This should come before extracting a `domain-flows` package because [repository.ts](/Users/vorcigernix/Dev/rohrpost/apps/control-api/src/repository.ts:1) still mixes persistence and domain logic too heavily.

### 2. Extract `domain-flows` only after the app-local split

Likely future contents:

- flow list/revision/deployment record types
- replay request types
- pure deployment selection helpers
- publish/rollback validation helpers

Do not move:

- SQLite queries
- route handlers
- Elysia schemas

### 3. Keep `runtime-manager` and `router-workers` thin

Those apps should mostly consume:

- `@rohrpost/control-api-contracts`
- `@rohrpost/shared-flow-spec`
- future pure domain packages

They should not re-own control-plane domain shapes unless those shapes are runtime-specific.

## Ownership Map

| Area | Owner | Package/App |
| --- | --- | --- |
| FlowSpec DSL and compiler | shared runtime contract | `packages/shared-flow-spec` |
| Control-plane HTTP surface | control-plane contract | `packages/control-api-contracts` |
| Connector catalog and source binding | connector domain | `packages/domain-connectors` |
| SQLite metadata persistence | control plane only | `apps/control-api` |
| Runtime execution loop | router runtime only | `apps/router-workers` |
| Rollout reconciliation | runtime manager only | `apps/runtime-manager` |
| Adapter supervision | adapter runtime only | `apps/adapter-redpanda` |
| Console feature composition | UI only | `apps/console` |

## File Move Sequence

Recommended incremental order:

1. `control-api-contracts` — already started.
2. `domain-connectors` — extract pure connector catalog and source binding.
3. Split `apps/control-api/src/app.ts` into route modules.
4. Split `apps/control-api/src/repository.ts` by bounded area.
5. Extract `domain-flows` only after those boundaries are visible.
6. Reorganize the console into feature folders and consume contracts/packages from there.

## Non-Goals

- No new monorepo tool.
- No service split.
- No ORM rewrite.
- No package-per-table or package-per-endpoint structure.

The goal is a cleaner monorepo, not a more complicated one.
