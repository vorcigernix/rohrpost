import { Outlet, createRootRouteWithContext, createRoute, createRouter, useRouterState } from '@tanstack/react-router';
import type { QueryClient } from '@tanstack/react-query';
import { AppShell } from './components/AppShell';
import { AuthGate } from './components/AuthGate';
import { AuthCallbackPage } from './pages/AuthCallbackPage';
import { AuthoringPage } from './pages/AuthoringPage';
import { CapabilitiesPage } from './pages/CapabilitiesPage';
import { DemoPage } from './pages/DemoPage';
import { FlowDetailPage } from './pages/FlowDetailPage';
import { FlowsPage } from './pages/FlowsPage';
import { HelpPage } from './pages/HelpPage';
import { InboxPage } from './pages/InboxPage';
import { PulsePage } from './pages/PulsePage';
import { RunsPage } from './pages/RunsPage';
import { SetupPage } from './pages/SetupPage';
import { WelcomePage } from './pages/WelcomePage';
import { queryClient } from './lib/query-client';

interface RouterContext {
  queryClient: QueryClient;
}

const rootRoute = createRootRouteWithContext<RouterContext>()({
  component: RootRoute,
});

function RootRoute() {
  const { location } = useRouterState();
  if (location.pathname === '/auth/callback') {
    return <Outlet />;
  }
  return (
    <AuthGate>
      <AppShell />
    </AuthGate>
  );
}

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: WelcomePage,
});

const welcomeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'welcome',
  component: WelcomePage,
});

const inboxRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'inbox',
  component: InboxPage,
});

const pulseRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'pulse',
  component: PulsePage,
});

const flowsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'flows',
  component: FlowsPage,
});

const flowDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'flows/$flowId',
  component: FlowDetailPage,
});

const composeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'compose',
  component: AuthoringPage,
});

const authoringRedirectRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'authoring',
  component: AuthoringPage,
});

const demoRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'demo',
  component: DemoPage,
});

const runsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'runs',
  component: RunsPage,
});

const capabilitiesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'capabilities',
  component: CapabilitiesPage,
});

const helpRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'help',
  component: HelpPage,
});

const setupRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'setup',
  component: SetupPage,
});

const authCallbackRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: 'auth/callback',
  component: AuthCallbackPage,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  welcomeRoute,
  pulseRoute,
  inboxRoute,
  demoRoute,
  composeRoute,
  authoringRedirectRoute,
  flowsRoute,
  flowDetailRoute,
  runsRoute,
  setupRoute,
  authCallbackRoute,
  capabilitiesRoute,
  helpRoute,
]);

export const router = createRouter({
  routeTree,
  context: { queryClient },
  defaultPreload: 'intent',
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
