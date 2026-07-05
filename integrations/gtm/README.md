# Rohrpost GTM Catcher

Paste `rohrpost-gtm-catcher.html` into a Google Tag Manager Custom HTML tag and fire it on Initialization / All Pages.

Set this before the snippet:

```html
<script>
window.rohrpostGtmCatcher = {
  endpoint: "https://router.example.com/ingest/website-events",
  dataLayerName: "dataLayer",
  includeExisting: true
};
</script>
```

The catcher mirrors existing and future `dataLayer.push(...)` items to the configured HTTP endpoint:

```json
{
  "source": "gtm.dataLayer",
  "page": {"url": "https://example.com/"},
  "events": [
    {
      "origin": "push",
      "item": {"event": "purchase", "value": 42}
    }
  ]
}
```

Limits:

- This captures `dataLayer` messages, not arbitrary network requests sent by every third-party tag.
- It uses `sendBeacon` or `fetch` with `keepalive` and `no-cors`, so do not rely on custom auth headers. Put any shared token in the endpoint URL or in the payload if needed.
- It is meant for sites you control and where consent/privacy rules allow mirroring analytics payloads.
