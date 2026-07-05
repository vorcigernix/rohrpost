import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ComponentType, MouseEvent, SVGProps } from 'react';
import { Outlet, useNavigate, useRouterState } from '@tanstack/react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AppShell as AstryxAppShell } from '@astryxdesign/core/AppShell';
import { Badge } from '@astryxdesign/core/Badge';
import { Layout, LayoutContent } from '@astryxdesign/core/Layout';
import { Divider } from '@astryxdesign/core/Divider';
import {
  SideNav,
  SideNavHeading,
  SideNavItem,
  SideNavSection,
} from '@astryxdesign/core/SideNav';
import { StatusDot } from '@astryxdesign/core/StatusDot';
import {
  ArrowPathIcon,
  BoltIcon,
  BookOpenIcon,
  CircleStackIcon,
  Cog6ToothIcon,
  InboxIcon,
  ListBulletIcon,
  PlayCircleIcon,
  RectangleGroupIcon,
  RocketLaunchIcon,
} from '@heroicons/react/24/outline';
import { api, type ConsoleEventMessage } from '../lib/api';
import { RohrpostMark } from './RohrpostMark';
import { deriveInboxItems, inboxUnreadCount, isFreshRuntimeSignal } from '../lib/inbox';
import {
  ComposeStepNavigationContext,
  composeWizardSteps,
  defaultComposeStepState,
  type ComposeStepNavigation,
  type ComposeStepState,
  type ComposeWizardStep,
} from '../lib/compose-navigation';

type HeroIcon = ComponentType<SVGProps<SVGSVGElement>>;

interface NavItem {
  to: string;
  label: string;
  icon: HeroIcon;
  badge?: number | null;
}

function routeMatches(pathname: string, to: string): boolean {
  if (to === '/') return pathname === '/' || pathname === '/welcome';
  return pathname === to || pathname.startsWith(`${to}/`);
}

function useRouterNav() {
  const navigate = useNavigate();
  return useCallback((to: string, event?: MouseEvent) => {
    if (event?.defaultPrevented) return;
    if (event && (event.button !== 0 || event.metaKey || event.altKey || event.ctrlKey || event.shiftKey)) {
      return;
    }
    event?.preventDefault();
    void navigate({ to });
  }, [navigate]);
}

function SideNavLink({
  item,
  pathname,
}: {
  item: NavItem;
  pathname: string;
}) {
  const open = useRouterNav();
  return (
    <SideNavItem
      label={item.label}
      icon={item.icon}
      href={item.to}
      isSelected={routeMatches(pathname, item.to)}
      endContent={item.badge ? <Badge variant="error" label={item.badge} /> : undefined}
      onClick={(event) => open(item.to, event)}
    />
  );
}

function ComposeSteps({
  composeNavigation,
}: {
  composeNavigation: ComposeStepNavigation;
}) {
  return (
    <>
      {composeWizardSteps.map((step) => {
        const isDone = composeNavigation.completedSteps.includes(step.id);
        const isAvailable = composeNavigation.availableSteps.includes(step.id);
        return (
          <SideNavItem
            key={step.id}
            label={`${composeWizardSteps.indexOf(step) + 1}. ${step.label}`}
            isSelected={composeNavigation.currentStep === step.id}
            isDisabled={!isAvailable}
            endContent={isDone ? <StatusDot variant="success" label="Complete" /> : undefined}
            onClick={() => composeNavigation.setCurrentStep(step.id)}
          />
        );
      })}
    </>
  );
}

function AppSideNav({
  unread,
  runtimeStats,
  composeNavigation,
}: {
  unread: number;
  runtimeStats: { throughput: number; inflight: number; healthy: number; incidents: number };
  composeNavigation: ComposeStepNavigation;
}) {
  const { location } = useRouterState();
  const pathname = location.pathname;
  const open = useRouterNav();
  const isComposeRoute = pathname === '/compose' || pathname === '/authoring';
  const isFlowsArea = routeMatches(pathname, '/flows') || isComposeRoute;
  const isStartArea = routeMatches(pathname, '/') || routeMatches(pathname, '/demo');
  const operateItems: NavItem[] = [
    { to: '/pulse', label: 'Pulse', icon: BoltIcon },
    { to: '/inbox', label: 'Inbox', icon: InboxIcon, badge: unread || null },
    { to: '/runs', label: 'Runs', icon: PlayCircleIcon },
  ];
  const helpItems: NavItem[] = [
    { to: '/help', label: 'Documentation', icon: BookOpenIcon },
    { to: '/capabilities', label: 'Capabilities', icon: CircleStackIcon },
  ];

  return (
    <SideNav
      collapsible
      resizable={{ defaultWidth: 300, minWidth: 232, maxWidth: 420 }}
      header={
        <SideNavHeading
          heading="Rohrpost"
          subheading="tenant_demo · eu-1"
          icon={<RohrpostMark size={20} />}
          headingHref="/"
          onClick={(event) => open('/', event)}
        />
      }
      topContent={<Divider />}
      footer={
        <>
          <SideNavSection title="Settings" isHeaderHidden>
            <SideNavLink
              item={{ to: '/setup', label: 'Settings', icon: Cog6ToothIcon }}
              pathname={pathname}
            />
          </SideNavSection>
          <SideNavSection title="Runtime">
            <SideNavItem
              label={`${runtimeStats.throughput.toLocaleString()}/s throughput`}
              icon={ArrowPathIcon}
              endContent={<StatusDot variant={runtimeStats.incidents ? 'warning' : 'success'} label="Runtime status" />}
            />
            <SideNavItem
              label={`${runtimeStats.healthy} healthy · ${runtimeStats.inflight} inflight`}
              icon={ListBulletIcon}
            />
          </SideNavSection>
        </>
      }>
      <SideNavSection title="Create">
        <SideNavItem
          label="Start here"
          icon={RocketLaunchIcon}
          href="/"
          isSelected={routeMatches(pathname, '/')}
          collapsible={{ defaultIsCollapsed: !isStartArea }}
          onClick={(event) => open('/', event)}>
          <SideNavItem
            label="Demo storefront"
            href="/demo"
            isSelected={routeMatches(pathname, '/demo')}
            onClick={(event) => open('/demo', event)}
          />
        </SideNavItem>
        <SideNavItem
          label="Flows"
          icon={RectangleGroupIcon}
          href="/flows"
          isSelected={routeMatches(pathname, '/flows')}
          collapsible={{ defaultIsCollapsed: !isFlowsArea }}
          onClick={(event) => open('/flows', event)}>
          <SideNavItem
            label="Compose flow"
            href="/compose"
            isSelected={isComposeRoute}
            endContent={<Badge variant="blue" label="New" />}
            onClick={(event) => open('/compose', event)}
          />
          {isComposeRoute ? <ComposeSteps composeNavigation={composeNavigation} /> : null}
        </SideNavItem>
      </SideNavSection>
      <Divider />
      <SideNavSection title="Operate">
        {operateItems.map((item) => (
          <SideNavLink key={item.to} item={item} pathname={pathname} />
        ))}
      </SideNavSection>
      <Divider />
      <SideNavSection title="Help">
        {helpItems.map((item) => (
          <SideNavLink key={item.to} item={item} pathname={pathname} />
        ))}
      </SideNavSection>
    </SideNav>
  );
}

export function AppShell() {
  const queryClient = useQueryClient();
  const [composeStepState, setComposeStepState] = useState<ComposeStepState>(defaultComposeStepState);
  const pendingKindsRef = useRef<Set<ConsoleEventMessage['kind']>>(new Set());
  const flushTimerRef = useRef<number | null>(null);
  const runtimeStatsQuery = useQuery({ queryKey: ['runtime-stats'], queryFn: api.fetchRuntimeStats });
  const adapterWorkloadsQuery = useQuery({ queryKey: ['adapter-workloads'], queryFn: api.fetchAdapterWorkloads });
  const unread = useMemo(() => inboxUnreadCount(deriveInboxItems({
    runtime: runtimeStatsQuery.data,
    adapterWorkloads: adapterWorkloadsQuery.data,
  })), [adapterWorkloadsQuery.data, runtimeStatsQuery.data]);
  const sidebarRuntimeStats = useMemo(() => {
    const freshDeployments = runtimeStatsQuery.data?.deployments.filter((deployment) =>
      isFreshRuntimeSignal(deployment.updatedAt, deployment.lastProcessedAt, deployment.lastAcceptedAt),
    ) ?? [];
    return {
      throughput: Math.round(freshDeployments.reduce((sum, deployment) => sum + deployment.deliveredCount, 0) / 3600),
      inflight: freshDeployments.reduce((sum, deployment) => sum + deployment.inflightCount, 0),
      healthy: freshDeployments.filter((deployment) => deployment.state === 'healthy').length,
      incidents: unread,
    };
  }, [runtimeStatsQuery.data, unread]);
  const setCurrentComposeStep = useCallback((step: ComposeWizardStep) => {
    setComposeStepState((current) => ({ ...current, currentStep: step }));
  }, []);
  const updateComposeStepNavigation = useCallback((state: Partial<ComposeStepState>) => {
    setComposeStepState((current) => ({ ...current, ...state }));
  }, []);
  const composeNavigation = useMemo<ComposeStepNavigation>(
    () => ({
      ...composeStepState,
      setCurrentStep: setCurrentComposeStep,
      updateStepNavigation: updateComposeStepNavigation,
    }),
    [composeStepState, setCurrentComposeStep, updateComposeStepNavigation],
  );

  useEffect(() => {
    const flush = () => {
      flushTimerRef.current = null;
      const kinds = Array.from(pendingKindsRef.current);
      pendingKindsRef.current.clear();

      if (kinds.includes('runtime')) {
        void queryClient.invalidateQueries({ queryKey: ['overview'] });
        void queryClient.invalidateQueries({ queryKey: ['runtime-stats'] });
        void queryClient.invalidateQueries({ queryKey: ['adapter-workloads'] });
        void queryClient.invalidateQueries({ queryKey: ['runtime-samples'] });
        void queryClient.invalidateQueries({ queryKey: ['runs'] });
      }

      if (kinds.includes('flows')) {
        void queryClient.invalidateQueries({ queryKey: ['flows'] });
        void queryClient.invalidateQueries({ queryKey: ['overview'] });
      }

      if (kinds.includes('connectors')) {
        void queryClient.invalidateQueries({ queryKey: ['connectors'] });
      }
    };

    const scheduleFlush = (kind: ConsoleEventMessage['kind']) => {
      pendingKindsRef.current.add(kind);
      if (flushTimerRef.current !== null) return;
      flushTimerRef.current = window.setTimeout(flush, 350);
    };

    const unsubscribe = api.subscribeToConsoleEvents(
      (event) => scheduleFlush(event.kind),
      () => {},
    );

    return () => {
      unsubscribe();
      if (flushTimerRef.current !== null) {
        window.clearTimeout(flushTimerRef.current);
      }
    };
  }, [queryClient]);

  return (
    <ComposeStepNavigationContext.Provider value={composeNavigation}>
      <AstryxAppShell
        contentPadding={0}
        sideNav={
          <AppSideNav
            unread={unread}
            runtimeStats={sidebarRuntimeStats}
            composeNavigation={composeNavigation}
          />
        }>
        <Layout
          height="fill"
          contentWidth={1400}
          content={
            <LayoutContent label="Page content" padding={5}>
              <Outlet />
            </LayoutContent>
          }
        />
      </AstryxAppShell>
    </ComposeStepNavigationContext.Provider>
  );
}
