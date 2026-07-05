import { useMemo, useState } from 'react';
import { useNavigate } from '@tanstack/react-router';
import {
  demoBusinessExamples,
  rememberSelectedDemoBusinessExample,
} from '../lib/business-examples';
import {
  DEMO_PRODUCTS,
  addCartProduct,
  cartQuantity,
  cartValue,
  clearDemoEventHistory,
  createDemoEvent,
  formatDemoCurrency,
  matchesQuery,
  readDemoEventHistory,
  rememberDemoEvent,
  removeCartProduct,
  type DemoCartLine,
  type DemoEventName,
  type DemoEventRecord,
  type DemoProduct,
} from '../lib/demo-events';
import { ClickableCard } from '@astryxdesign/core/ClickableCard';
import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Grid } from '@astryxdesign/core/Grid';
import { HStack } from '@astryxdesign/core/HStack';
import { List, ListItem } from '@astryxdesign/core/List';
import { StackItem } from '@astryxdesign/core/Stack';
import { Text } from '@astryxdesign/core/Text';
import { TextInput } from '@astryxdesign/core/TextInput';
import { VStack } from '@astryxdesign/core/VStack';
import { ActionButton, ActionLink, Panel, PanelHeader, Pill, SectionHeader, StatusDot } from '../components/ui';

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2) ?? 'null';
}

function formatEventTime(value: string): string {
  return new Date(value).toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function eventLabel(name: DemoEventName): string {
  return name.replaceAll('_', ' ');
}

export function DemoPage() {
  const [cart, setCart] = useState<DemoCartLine[]>([]);
  const [query, setQuery] = useState('');
  const [selectedProduct, setSelectedProduct] = useState<DemoProduct>(DEMO_PRODUCTS[0]);
  const [eventHistory, setEventHistory] = useState<DemoEventRecord[]>(() => readDemoEventHistory());
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [checkoutStep, setCheckoutStep] = useState<'cart' | 'shipping' | 'payment' | 'review'>('cart');
  const navigate = useNavigate();

  const filteredProducts = useMemo(
    () => DEMO_PRODUCTS.filter((product) => matchesQuery(product, query)),
    [query],
  );

  const selectedEvent = eventHistory.find((event) => event.id === selectedEventId) ?? eventHistory[0] ?? null;

  const recordEvent = (name: DemoEventName, input: {
    product?: DemoProduct;
    products?: DemoProduct[];
    cart?: DemoCartLine[];
    query?: string;
    checkoutStep?: string;
    orderId?: string;
  } = {}) => {
    const event = createDemoEvent(name, {
      ...input,
      cart: input.cart ?? cart,
    });
    const next = rememberDemoEvent(event);
    setEventHistory(next);
    setSelectedEventId(event.id);
    return event;
  };

  const viewProduct = (product: DemoProduct) => {
    setSelectedProduct(product);
    recordEvent('view_item', { product });
  };

  const addProduct = (product: DemoProduct) => {
    const nextCart = addCartProduct(cart, product);
    setCart(nextCart);
    setCheckoutStep('cart');
    recordEvent('add_to_cart', { product, cart: nextCart });
  };

  const removeProduct = (product: DemoProduct) => {
    const nextCart = removeCartProduct(cart, product.id);
    setCart(nextCart);
    setCheckoutStep('cart');
    recordEvent('remove_from_cart', { product, cart: nextCart });
  };

  const runSearch = () => {
    recordEvent(query.trim() ? 'search' : 'view_item_list', {
      query: query.trim() || undefined,
      products: filteredProducts,
    });
  };

  const beginCheckout = () => {
    if (cart.length === 0) return;
    setCheckoutStep('shipping');
    recordEvent('begin_checkout', { cart, checkoutStep: 'checkout' });
  };

  const addShipping = () => {
    if (cart.length === 0) return;
    setCheckoutStep('payment');
    recordEvent('add_shipping_info', { cart, checkoutStep: 'shipping' });
  };

  const addPayment = () => {
    if (cart.length === 0) return;
    setCheckoutStep('review');
    recordEvent('add_payment_info', { cart, checkoutStep: 'payment' });
  };

  const completePurchase = () => {
    if (cart.length === 0) return;
    const orderId = `order_${Date.now().toString(36)}`;
    recordEvent('purchase', { cart, checkoutStep: 'purchase', orderId });
    setCart([]);
    setCheckoutStep('cart');
  };

  const resetDemo = () => {
    clearDemoEventHistory();
    setEventHistory([]);
    setSelectedEventId(null);
    setCart([]);
    setCheckoutStep('cart');
  };

  return (
    <VStack gap={4}>
      <SectionHeader
        eyebrow="Step 2"
        title="Generate storefront events"
        description="Browse The Pantry to create realistic ecommerce payloads. Continue to Compose when the batch has the funnel moments you want."
        actions={
          eventHistory.length > 0 ? (
            <ActionLink to="/authoring" variant="primary" label={`Compose from ${eventHistory.length} events`}>
              {`Compose from ${eventHistory.length} events →`}
            </ActionLink>
          ) : (
            <Pill tone="info">{eventHistory.length} events captured</Pill>
          )
        }
      />

      <HStack gap={3} align="start">
        <StackItem size="fill">
          <VStack gap={3}>
            <Panel>
              <VStack gap={4}>
                <PanelHeader
                  eyebrow="Demo store"
                  title="The Pantry"
                  actions={
                    <ActionButton variant="ghost" type="button" onClick={() => recordEvent('view_cart')}>
                      {`${cartQuantity(cart)} items · ${formatDemoCurrency(cartValue(cart))}`}
                    </ActionButton>
                  }
                />

                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    runSearch();
                  }}
                >
                  <HStack gap={2} align="end" wrap="wrap">
                    <StackItem size="fill">
                      <TextInput
                        label="Search products"
                        isLabelHidden
                        value={query}
                        onChange={(value) => setQuery(value)}
                        placeholder="Search coffee, citrus, chocolate..."
                      />
                    </StackItem>
                    <ActionButton variant="secondary" type="submit">
                      Search
                    </ActionButton>
                  </HStack>
                </form>

                <Grid columns={{ minWidth: 220 }} gap={3}>
                  {filteredProducts.map((product) => (
                    <ClickableCard
                      key={product.id}
                      label={`View ${product.name}`}
                      variant={selectedProduct.id === product.id ? 'default' : 'muted'}
                      padding={3}
                      onClick={() => viewProduct(product)}
                    >
                      <VStack gap={1.5}>
                        <Text type="supporting" color="secondary" weight="semibold" display="block">
                          {product.category.toUpperCase()}
                        </Text>
                        <Text type="body" weight="semibold" display="block">{product.name}</Text>
                        <Text type="supporting" color="secondary" display="block">{product.description}</Text>
                        <HStack justify="between" align="center" gap={2}>
                          <Text type="body" hasTabularNumbers>{formatDemoCurrency(product.price)}</Text>
                          <ActionButton
                            variant="primary"
                            size="sm"
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              addProduct(product);
                            }}
                          >
                            Add
                          </ActionButton>
                        </HStack>
                      </VStack>
                    </ClickableCard>
                  ))}
                </Grid>
              </VStack>
            </Panel>

            {cart.length > 0 ? (
              <Panel>
                <VStack gap={3}>
                  <List density="compact" hasDividers>
                    {cart.map((line) => (
                      <ListItem
                        key={line.product.id}
                        label={line.product.name}
                        description={`${line.quantity} x ${formatDemoCurrency(line.product.price)}`}
                        endContent={
                          <ActionButton variant="secondary" size="sm" type="button" onClick={() => removeProduct(line.product)}>
                            Remove
                          </ActionButton>
                        }
                      />
                    ))}
                  </List>
                  <HStack gap={2} wrap="wrap">
                    <ActionButton variant="secondary" type="button" onClick={beginCheckout} disabled={checkoutStep !== 'cart'}>
                      Begin checkout
                    </ActionButton>
                    <ActionButton variant="secondary" type="button" onClick={addShipping} disabled={checkoutStep !== 'shipping'}>
                      Add shipping
                    </ActionButton>
                    <ActionButton variant="secondary" type="button" onClick={addPayment} disabled={checkoutStep !== 'payment'}>
                      Add payment
                    </ActionButton>
                    <ActionButton variant="primary" type="button" onClick={completePurchase} disabled={checkoutStep !== 'review'}>
                      Purchase
                    </ActionButton>
                  </HStack>
                </VStack>
              </Panel>
            ) : null}

            <Panel>
              <VStack gap={3}>
                <PanelHeader eyebrow="Business examples" title="Starting points" />
                <Grid columns={{ minWidth: 220 }} gap={3}>
                  {demoBusinessExamples.map((example) => (
                    <ClickableCard
                      key={example.id}
                      label={example.title}
                      variant="muted"
                      padding={3}
                      onClick={() => {
                        rememberSelectedDemoBusinessExample(example.id);
                        void navigate({ to: '/authoring' });
                      }}
                    >
                      <VStack gap={1}>
                        <Text type="body" weight="semibold" display="block">{example.title}</Text>
                        <Text type="supporting" color="secondary" display="block">{example.outcome}</Text>
                      </VStack>
                    </ClickableCard>
                  ))}
                </Grid>
              </VStack>
            </Panel>
          </VStack>
        </StackItem>

        <StackItem>
          <VStack gap={3} width={400}>
            <Panel>
              <VStack gap={3}>
                <PanelHeader
                  eyebrow="Captured events"
                  title={`${eventHistory.length} events`}
                  actions={
                    <ActionButton variant="secondary" type="button" onClick={resetDemo} disabled={eventHistory.length === 0}>
                      Clear
                    </ActionButton>
                  }
                />

                {eventHistory.length === 0 ? (
                  <EmptyState
                    isCompact
                    title="No events captured"
                    description="Open a product, add to cart, or search to capture events."
                  />
                ) : (
                  <VStack gap={3}>
                    <List density="compact" hasDividers>
                      {eventHistory.map((event) => (
                        <ListItem
                          key={event.id}
                          label={eventLabel(event.name)}
                          description={event.summary}
                          isSelected={selectedEvent?.id === event.id}
                          startContent={<StatusDot tone={selectedEvent?.id === event.id ? 'good' : 'info'} />}
                          endContent={
                            <Text type="supporting" color="secondary" hasTabularNumbers>
                              {formatEventTime(event.createdAt)}
                            </Text>
                          }
                          onClick={() => setSelectedEventId(event.id)}
                        />
                      ))}
                    </List>

                    {selectedEvent ? (
                      <CodeBlock
                        title={eventLabel(selectedEvent.name)}
                        code={formatJson(selectedEvent.payload)}
                        language="json"
                        size="sm"
                        width="100%"
                        maxHeight={360}
                      />
                    ) : null}
                  </VStack>
                )}
              </VStack>
            </Panel>
          </VStack>
        </StackItem>
      </HStack>
    </VStack>
  );
}
