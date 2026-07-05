# Domain Connectors

Pure connector-domain code shared across apps.

This package is the first non-contract domain package in the monorepo. It is intentionally narrow:

- connector capability catalog
- source-binding generation for authored flows

What does not belong here:

- Elysia route wiring
- SQLite repositories
- adapter runtime HTTP clients
- runtime-specific delivery code

Those stay in app composition roots until their boundaries are proven.
