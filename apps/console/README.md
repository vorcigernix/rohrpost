# Console

React console for the Event Router Platform. The current beta UI covers overview metrics, flow inventory, runs, capabilities, and a prompt-driven authoring workspace.

## Scripts

```bash
bun install
bun run dev
bun run build
bun run test
bun run typecheck
```

## Notes

- The UI points at the real `/api/*` endpoints by default.
- In local development, Vite proxies `/api/*` to `http://127.0.0.1:3001`.
- When opened on `localhost:5173` or `localhost:4173`, the console also auto-targets `http://127.0.0.1:3001` unless `VITE_API_BASE_URL` is set.
- The `/authoring` route provides prompt -> draft -> validation -> simulation -> publish against the live control API.
- The authoring flow uses recent runtime samples only to infer JSON shape. When you publish, the control plane automatically provisions a dedicated source connector and concrete ingress target such as `/ingest/<flow>`, `events.source.<flow>`, or `router.ingress.<flow>`.

## UI Writing

- Keep interface copy business-facing and action-oriented. Do not explain internal product limitations in labels, badges, or empty states; reserve that context for docs, sales notes, or technical detail views.
