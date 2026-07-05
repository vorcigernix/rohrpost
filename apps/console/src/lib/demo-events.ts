export type DemoEventName =
  | 'page_view'
  | 'view_item_list'
  | 'search'
  | 'view_item'
  | 'add_to_cart'
  | 'remove_from_cart'
  | 'view_cart'
  | 'begin_checkout'
  | 'add_shipping_info'
  | 'add_payment_info'
  | 'purchase';

export type DemoProduct = {
  id: string;
  sku: string;
  name: string;
  category: string;
  price: number;
  description: string;
};

export type DemoCartLine = {
  product: DemoProduct;
  quantity: number;
};

export type DemoEventRecord = {
  id: string;
  name: DemoEventName;
  summary: string;
  createdAt: string;
  payload: Record<string, unknown>;
};

type DemoEventInput = {
  product?: DemoProduct;
  products?: DemoProduct[];
  cart?: DemoCartLine[];
  query?: string;
  checkoutStep?: string;
  orderId?: string;
};

export const DEMO_PRODUCTS: DemoProduct[] = [
  {
    id: 'prod_olive_oil',
    sku: 'PANTRY-OIL-001',
    name: 'Cold Press Olive Oil',
    category: 'Pantry',
    price: 18,
    description: 'Single-origin oil for salads, pasta, and roasted vegetables.',
  },
  {
    id: 'prod_granola',
    sku: 'PANTRY-GRA-002',
    name: 'Hazelnut Granola',
    category: 'Breakfast',
    price: 9,
    description: 'Small-batch oats, hazelnuts, cacao nibs, and sea salt.',
  },
  {
    id: 'prod_coffee',
    sku: 'PANTRY-COF-003',
    name: 'Morning Filter Coffee',
    category: 'Drinks',
    price: 15,
    description: 'Bright washed beans with citrus and brown sugar notes.',
  },
  {
    id: 'prod_citrus',
    sku: 'PANTRY-CIT-004',
    name: 'Seasonal Citrus Box',
    category: 'Fresh',
    price: 24,
    description: 'A mixed case of mandarins, blood oranges, and grapefruit.',
  },
  {
    id: 'prod_chocolate',
    sku: 'PANTRY-CHO-005',
    name: 'Dark Chocolate Bar',
    category: 'Snacks',
    price: 7,
    description: 'Seventy percent cacao with toasted almond pieces.',
  },
  {
    id: 'prod_tea',
    sku: 'PANTRY-TEA-006',
    name: 'Jasmine Green Tea',
    category: 'Drinks',
    price: 12,
    description: 'Loose-leaf green tea scented with jasmine blossoms.',
  },
];

export const DEMO_EVENT_HISTORY_KEY = 'rohrpost.demo.events';
export const DEMO_LATEST_EVENT_KEY = 'rohrpost.demo.latest-event';

export function formatDemoCurrency(value: number): string {
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(value);
}

export function cartQuantity(cart: DemoCartLine[]): number {
  return cart.reduce((sum, line) => sum + line.quantity, 0);
}

export function cartValue(cart: DemoCartLine[]): number {
  return cart.reduce((sum, line) => sum + line.product.price * line.quantity, 0);
}

export function createDemoEvent(name: DemoEventName, input: DemoEventInput = {}): DemoEventRecord {
  const id = createId();
  const createdAt = new Date().toISOString();
  const cart = input.cart ?? [];
  const product = input.product;
  const products = input.products ?? [];
  const value = cart.length > 0 ? cartValue(cart) : product ? product.price : 0;

  const payload: Record<string, unknown> = {
    event_name: name,
    event_id: id,
    occurred_at: createdAt,
    anonymous_id: readOrCreateIdentity(),
    session_id: readOrCreateSessionId(),
    source: {
      type: 'demo_store',
      name: 'The Pantry',
      surface: 'console_demo',
    },
    page: {
      path: '/demo',
      title: 'The Pantry',
      referrer: 'rohrpost-console',
    },
    context: {
      locale: 'en-US',
      currency: 'USD',
      user_agent: typeof navigator === 'undefined' ? 'server' : navigator.userAgent,
    },
    ecommerce: {
      currency: 'USD',
      value,
      items: cart.length > 0
        ? cart.map((line) => toEventItem(line.product, line.quantity))
        : product
          ? [toEventItem(product, 1)]
          : products.map((item) => toEventItem(item, 1)),
    },
  };

  if (product) {
    payload.product = {
      item_id: product.id,
      sku: product.sku,
      item_name: product.name,
      item_category: product.category,
      price: product.price,
    };
  }

  if (input.query) {
    payload.search = {
      query: input.query,
      result_count: DEMO_PRODUCTS.filter((item) => matchesQuery(item, input.query ?? '')).length,
    };
  }

  if (input.checkoutStep) {
    payload.checkout = {
      step: input.checkoutStep,
      shipping_tier: input.checkoutStep === 'shipping' ? 'standard' : undefined,
      payment_type: input.checkoutStep === 'payment' ? 'card' : undefined,
    };
  }

  if (input.orderId) {
    payload.order = {
      order_id: input.orderId,
      revenue: cartValue(cart),
      tax: Math.round(cartValue(cart) * 0.08 * 100) / 100,
      shipping: cartValue(cart) > 50 ? 0 : 6,
    };
  }

  return {
    id,
    name,
    summary: summarizeEvent(name, input),
    createdAt,
    payload,
  };
}

export function matchesQuery(product: DemoProduct, query: string): boolean {
  const normalized = query.trim().toLowerCase();
  if (!normalized) return true;
  return [
    product.name,
    product.category,
    product.description,
    product.sku,
  ].some((value) => value.toLowerCase().includes(normalized));
}

export function addCartProduct(cart: DemoCartLine[], product: DemoProduct): DemoCartLine[] {
  const existing = cart.find((line) => line.product.id === product.id);
  if (existing) {
    return cart.map((line) =>
      line.product.id === product.id ? { ...line, quantity: line.quantity + 1 } : line,
    );
  }
  return [...cart, { product, quantity: 1 }];
}

export function removeCartProduct(cart: DemoCartLine[], productId: string): DemoCartLine[] {
  return cart.flatMap((line) => {
    if (line.product.id !== productId) return [line];
    if (line.quantity <= 1) return [];
    return [{ ...line, quantity: line.quantity - 1 }];
  });
}

export function rememberDemoEvent(event: DemoEventRecord): DemoEventRecord[] {
  const next = [event, ...readDemoEventHistory().filter((item) => item.id !== event.id)].slice(0, 40);
  writeStorage(DEMO_LATEST_EVENT_KEY, JSON.stringify(event));
  writeStorage(DEMO_EVENT_HISTORY_KEY, JSON.stringify(next));
  return next;
}

export function rememberLatestDemoEvent(event: DemoEventRecord): void {
  writeStorage(DEMO_LATEST_EVENT_KEY, JSON.stringify(event));
}

export function readLatestDemoEvent(): DemoEventRecord | null {
  return parseDemoEvent(readStorage(DEMO_LATEST_EVENT_KEY));
}

export function readDemoEventHistory(): DemoEventRecord[] {
  const value = readStorage(DEMO_EVENT_HISTORY_KEY);
  if (!value) return [];
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.flatMap((item) => {
      const event = parseDemoEventFromValue(item);
      return event ? [event] : [];
    });
  } catch {
    return [];
  }
}

export function clearDemoEventHistory(): void {
  removeStorage(DEMO_EVENT_HISTORY_KEY);
  removeStorage(DEMO_LATEST_EVENT_KEY);
}

function summarizeEvent(name: DemoEventName, input: DemoEventInput): string {
  switch (name) {
    case 'search':
      return `Search for "${input.query ?? ''}"`;
    case 'view_item':
      return input.product ? `Viewed ${input.product.name}` : 'Viewed product';
    case 'add_to_cart':
      return input.product ? `Added ${input.product.name}` : 'Added product';
    case 'remove_from_cart':
      return input.product ? `Removed ${input.product.name}` : 'Removed product';
    case 'view_cart':
      return `${cartQuantity(input.cart ?? [])} items in cart`;
    case 'begin_checkout':
      return `Checkout started for ${formatDemoCurrency(cartValue(input.cart ?? []))}`;
    case 'add_shipping_info':
      return 'Shipping info added';
    case 'add_payment_info':
      return 'Payment info added';
    case 'purchase':
      return `Purchase ${input.orderId ?? ''}`.trim();
    case 'view_item_list':
      return 'Viewed product list';
    default:
      return 'Storefront page view';
  }
}

function toEventItem(product: DemoProduct, quantity: number): Record<string, unknown> {
  return {
    item_id: product.id,
    sku: product.sku,
    item_name: product.name,
    item_category: product.category,
    price: product.price,
    quantity,
  };
}

function parseDemoEvent(value: string | null): DemoEventRecord | null {
  if (!value) return null;
  try {
    return parseDemoEventFromValue(JSON.parse(value));
  } catch {
    return null;
  }
}

function parseDemoEventFromValue(value: unknown): DemoEventRecord | null {
  if (!isRecord(value)) return null;
  if (
    typeof value.id !== 'string' ||
    typeof value.name !== 'string' ||
    typeof value.summary !== 'string' ||
    typeof value.createdAt !== 'string' ||
    !isRecord(value.payload)
  ) {
    return null;
  }

  return {
    id: value.id,
    name: value.name as DemoEventName,
    summary: value.summary,
    createdAt: value.createdAt,
    payload: value.payload,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readOrCreateIdentity(): string {
  const key = 'rohrpost.demo.identity';
  const existing = readStorage(key);
  if (existing) return existing;
  const next = `anon_${createId()}`;
  writeStorage(key, next);
  return next;
}

function readOrCreateSessionId(): string {
  const key = 'rohrpost.demo.session';
  const existing = readStorage(key);
  if (existing) return existing;
  const next = `session_${createId()}`;
  writeStorage(key, next);
  return next;
}

function createId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function readStorage(key: string): string | null {
  const storage = getStorage();
  if (!storage) return null;
  try {
    return storage.getItem(key);
  } catch {
    return null;
  }
}

function writeStorage(key: string, value: string): void {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(key, value);
  } catch {
    // Ignore storage failures; the demo should still work in memory.
  }
}

function removeStorage(key: string): void {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.removeItem(key);
  } catch {
    // Ignore storage failures; the demo should still work in memory.
  }
}

function getStorage(): Storage | null {
  if (typeof window === 'undefined') return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}
