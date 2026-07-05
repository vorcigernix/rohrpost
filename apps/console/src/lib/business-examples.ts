export type DemoBusinessExampleId =
  | 'warehouse-cleanup'
  | 'purchase-dedupe'
  | 'pii-redaction'
  | 'high-value-routing'
  | 'risk-enrichment'
  | 'cart-normalization'
  | 'vendor-payload';

export interface DemoBusinessExample {
  id: DemoBusinessExampleId;
  title: string;
  outcome: string;
  prompt: string;
}

const STORAGE_KEY = 'rohrpost.demo.selected-example';

export const demoBusinessExamples: DemoBusinessExample[] = [
  {
    id: 'warehouse-cleanup',
    title: 'Clean ecommerce events for BI',
    outcome: 'Send consistent product, cart, revenue, campaign, and source fields to the warehouse.',
    prompt: 'Keep ecommerce product, cart, revenue, currency, source, campaign, and attribution fields. Drop UI debug metadata and empty checkout fields.',
  },
  {
    id: 'purchase-dedupe',
    title: 'Protect purchase metrics',
    outcome: 'Shape purchase events around order_id so downstream systems can deduplicate retries.',
    prompt: 'For purchase events, keep order_id, event_name, event_time, customer email, cart total, currency, line items, and source. Drop browser-only metadata.',
  },
  {
    id: 'pii-redaction',
    title: 'Govern data before vendors',
    outcome: 'Mask direct identifiers before events are exported to analytics or partner tools.',
    prompt: 'Keep ecommerce event, product, cart, revenue, and attribution fields, but redact email, phone, and address-like fields before delivery.',
  },
  {
    id: 'high-value-routing',
    title: 'Route high-value orders',
    outcome: 'Prepare a compact order payload for operations or CRM workflows.',
    prompt: 'Create a compact order operations payload with order_id, customer email, revenue total, currency, item count, product names, and campaign fields.',
  },
  {
    id: 'risk-enrichment',
    title: 'Add customer risk context',
    outcome: 'Attach risk band or loyalty context before the event reaches a warehouse or operations queue.',
    prompt: 'Keep ecommerce event, customer, order value, currency, product, and source fields, then enrich the payload with a customer risk band keyed by customer_id or anonymous_id.',
  },
  {
    id: 'cart-normalization',
    title: 'Normalize checkout funnel',
    outcome: 'Make cart and checkout events comparable across web, mobile, and server sources.',
    prompt: 'Normalize this checkout event into event_name, event_time, checkout_step, cart_value, currency, item_count, product_ids, product_names, and source.',
  },
  {
    id: 'vendor-payload',
    title: 'Shape a vendor payload',
    outcome: 'Build the JSON a destination expects without changing the storefront code.',
    prompt: 'Project this storefront event into a vendor payload with customer, event, ecommerce, revenue, products, and attribution objects.',
  },
];

export function readSelectedDemoBusinessExample(): DemoBusinessExample | null {
  if (typeof window === 'undefined') return null;
  const id = window.localStorage.getItem(STORAGE_KEY);
  return demoBusinessExamples.find((example) => example.id === id) ?? null;
}

export function rememberSelectedDemoBusinessExample(id: DemoBusinessExampleId): void {
  if (typeof window === 'undefined') return;
  window.localStorage.setItem(STORAGE_KEY, id);
}
