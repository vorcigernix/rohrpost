import type { AnchorHTMLAttributes, MouseEventHandler, ReactNode } from 'react';
import { Fragment } from 'react';
import { Link } from '@tanstack/react-router';
import { Badge, type BadgeVariant } from '@astryxdesign/core/Badge';
import { Button, type ButtonSize, type ButtonVariant } from '@astryxdesign/core/Button';
import { Card, type CardVariant } from '@astryxdesign/core/Card';
import { Icon, type IconType } from '@astryxdesign/core/Icon';
import { HStack } from '@astryxdesign/core/HStack';
import { Heading } from '@astryxdesign/core/Heading';
import { Skeleton } from '@astryxdesign/core/Skeleton';
import { StatusDot as AstryxStatusDot, type StatusDotVariant } from '@astryxdesign/core/StatusDot';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { cn } from '../lib/utils';

type PipeNodeRole = 'source' | 'processor' | 'sink';
type Tone = 'neutral' | 'good' | 'info' | 'warn' | 'danger';

const badgeVariant: Record<Tone, BadgeVariant> = {
  neutral: 'neutral',
  good: 'success',
  info: 'info',
  warn: 'warning',
  danger: 'error',
};

const dotVariant: Record<Tone, StatusDotVariant> = {
  neutral: 'neutral',
  good: 'success',
  info: 'accent',
  warn: 'warning',
  danger: 'error',
};

const cardVariant: Record<Tone, CardVariant> = {
  neutral: 'default',
  good: 'green',
  info: 'blue',
  warn: 'yellow',
  danger: 'red',
};

function resolveLabel(children: ReactNode, label?: string): string {
  return label ?? (typeof children === 'string' ? children : 'Action');
}

export function ActionButton({
  children,
  label,
  icon,
  endIcon,
  variant = 'secondary',
  size = 'md',
  type = 'button',
  disabled = false,
  isDisabled = false,
  isLoading = false,
  onClick,
}: {
  children: ReactNode;
  label?: string;
  icon?: IconType;
  endIcon?: IconType;
  variant?: ButtonVariant;
  size?: ButtonSize;
  type?: 'button' | 'submit' | 'reset';
  disabled?: boolean;
  isDisabled?: boolean;
  isLoading?: boolean;
  onClick?: MouseEventHandler<HTMLButtonElement>;
}) {
  return (
    <Button
      label={resolveLabel(children, label)}
      icon={icon ? <Icon icon={icon} /> : undefined}
      endContent={endIcon ? <Icon icon={endIcon} /> : undefined}
      variant={variant}
      size={size}
      type={type}
      isDisabled={disabled || isDisabled}
      isLoading={isLoading}
      onClick={onClick}
    >
      {typeof children === 'string' ? undefined : children}
    </Button>
  );
}

export function ActionLink({
  children,
  label,
  icon,
  endIcon,
  to,
  params,
  search,
  variant = 'secondary',
  size = 'md',
}: {
  children: ReactNode;
  label?: string;
  icon?: IconType;
  endIcon?: IconType;
  to: string;
  params?: Record<string, string>;
  search?: Record<string, unknown>;
  variant?: ButtonVariant;
  size?: ButtonSize;
}) {
  function RouterButtonLink({
    href: _href,
    ...props
  }: AnchorHTMLAttributes<HTMLAnchorElement> & { href?: string }) {
    return (
      <Link
        to={to as never}
        params={params as never}
        search={search as never}
        {...props}
      />
    );
  }

  return (
    <Button
      label={resolveLabel(children, label)}
      icon={icon ? <Icon icon={icon} /> : undefined}
      endContent={endIcon ? <Icon icon={endIcon} /> : undefined}
      variant={variant}
      size={size}
      href={to}
      as={RouterButtonLink}
    >
      {typeof children === 'string' ? undefined : children}
    </Button>
  );
}

interface PipeNodeProps {
  role: PipeNodeRole;
  kind: string;
  label: string;
  meta?: string;
  error?: boolean;
}

export function Pipeline({
  nodes,
  flowing = true,
  error = false,
  variant = 'compact',
}: {
  nodes: PipeNodeProps[];
  flowing?: boolean;
  error?: boolean;
  variant?: 'compact' | 'hero';
}) {
  const containerClass = variant === 'hero' ? 'pipe-hero' : 'pipe';
  return (
    <div className="pipe-scroll">
      <div className={containerClass}>
        {nodes.map((node, idx) => (
          <Fragment key={`${node.role}-${idx}-${node.label}`}>
            <div
              className={cn('pipe-node', `is-${node.role}`, node.error ? 'is-error' : null)}
            >
              <div className="pipe-node-kind">
                <span className={cn('pipe-node-chip', `chip-${node.role}`)} aria-hidden />
                {node.role === 'source' ? 'Source' : node.role === 'sink' ? 'Sink' : 'Processor'} · {node.kind}
              </div>
              <div className="pipe-node-title">{node.label}</div>
              {node.meta ? <div className="pipe-node-meta">{node.meta}</div> : null}
            </div>
            {idx < nodes.length - 1 ? (
              <div className={cn('pipe-connector', error ? 'is-error' : null)} aria-hidden>
                <span className="pipe-connector-line" />
                {flowing && !error ? <span className="pipe-connector-flow" /> : null}
                <span className="pipe-connector-arrow">
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path d="M3 2.5l4 3.5-4 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </span>
              </div>
            ) : null}
          </Fragment>
        ))}
      </div>
    </div>
  );
}

export function Panel({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return <Card className={className}>{children}</Card>;
}

export function Pill({
  children,
  tone = 'neutral',
}: {
  children: ReactNode;
  tone?: Tone;
}) {
  return <Badge variant={badgeVariant[tone]} label={children} />;
}

export function StatusDot({
  tone = 'neutral',
}: {
  tone?: Tone;
}) {
  return <AstryxStatusDot variant={dotVariant[tone]} label={`${tone} status`} />;
}

export function MetricCard({
  label,
  value,
  detail,
  tone = 'neutral',
}: {
  label: string;
  value: string;
  detail: string;
  tone?: Tone;
}) {
  return (
    <Card variant={cardVariant[tone]}>
      <VStack gap={1}>
        <Text type="supporting" weight="semibold" display="block">{label}</Text>
        <Text type="display-3" display="block" hasTabularNumbers>{value}</Text>
        <Text type="supporting" display="block" color="secondary">{detail}</Text>
      </VStack>
    </Card>
  );
}

export function PageHeader({
  eyebrow,
  title,
  sub,
  actions,
}: {
  eyebrow?: string;
  title: string;
  sub?: string;
  actions?: ReactNode;
}) {
  return (
    <HStack justify="between" align="end" gap={4} wrap="wrap">
      <VStack gap={1}>
        {eyebrow ? (
          <Text type="supporting" color="secondary" weight="semibold" display="block">
            {eyebrow.toUpperCase()}
          </Text>
        ) : null}
        <Heading level={1}>{title}</Heading>
        {sub ? (
          <Text type="supporting" color="secondary" display="block" maxLines={2}>
            {sub}
          </Text>
        ) : null}
      </VStack>
      {actions ? (
        <HStack gap={2} align="center" wrap="wrap" justify="end">
          {actions}
        </HStack>
      ) : null}
    </HStack>
  );
}

export function PanelHeader({
  eyebrow,
  title,
  actions,
}: {
  eyebrow: string;
  title: string;
  actions?: ReactNode;
}) {
  return (
    <HStack justify="between" align="center" gap={3} wrap="wrap">
      <VStack gap={0.5}>
        <Text type="supporting" color="secondary" weight="semibold" display="block">
          {eyebrow.toUpperCase()}
        </Text>
        <Heading level={2}>{title}</Heading>
      </VStack>
      {actions ? (
        <HStack gap={2} align="center" justify="end">
          {actions}
        </HStack>
      ) : null}
    </HStack>
  );
}

export function MetricTile({
  label,
  value,
  unit,
  sub,
  spark,
}: {
  label: string;
  value: string;
  unit?: string;
  sub?: string;
  spark?: ReactNode;
}) {
  return (
    <Card padding={3}>
      <VStack gap={1}>
        <Text type="supporting" color="secondary" weight="semibold" display="block">
          {label.toUpperCase()}
        </Text>
        <Text type="display-3" display="block" hasTabularNumbers>
          {value}
          {unit ? (
            <Text type="supporting" color="secondary" weight="medium">
              {' '}
              {unit}
            </Text>
          ) : null}
        </Text>
        {sub ? (
          <Text type="supporting" display="block" color="secondary">
            {sub}
          </Text>
        ) : null}
        {spark ? <HStack height={28}>{spark}</HStack> : null}
      </VStack>
    </Card>
  );
}

export function SectionHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <HStack justify="between" align="end" gap={3} wrap="wrap">
      <VStack gap={1}>
        <Text type="supporting" color="secondary" weight="semibold" display="block">{eyebrow}</Text>
        <Heading level={2}>{title}</Heading>
        <Text type="supporting" color="secondary" display="block">{description}</Text>
      </VStack>
      {actions ? <HStack gap={2} wrap="wrap" justify="end">{actions}</HStack> : null}
    </HStack>
  );
}

export function LoadingBlock({ lines = 3 }: { lines?: number }) {
  return (
    <VStack gap={2} aria-busy="true" aria-live="polite">
      {Array.from({ length: lines }).map((_, index) => (
        <Skeleton
          key={index}
          width={`${85 - index * 10}%`}
          height={12}
          index={index}
        />
      ))}
    </VStack>
  );
}
