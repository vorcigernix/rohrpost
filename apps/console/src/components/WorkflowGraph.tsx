import { Icon } from '@astryxdesign/core/Icon';
import { ArrowsPointingInIcon, ArrowsPointingOutIcon } from '@heroicons/react/24/outline';
import { useEffect, useMemo, useState } from 'react';
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  useNodesState,
  type Edge,
  type Node,
  type NodeProps,
} from '@xyflow/react';
import type { FlowSpec as BackendFlowSpec } from '@rohrpost/shared-flow-spec';
import { cn } from '../lib/utils';
import type { AdapterWorkloadRecord, RuntimeDeploymentRecord } from '../lib/api-types';
import {
  deriveWorkflowGraph,
  type WorkflowEdge,
  type WorkflowGraphModel,
  type WorkflowNode,
} from '../features/workflow/workflow-graph';

type WorkflowGraphNodeData = WorkflowNode & Record<string, unknown>;
type WorkflowGraphEdgeData = WorkflowEdge & Record<string, unknown>;
type WorkflowReactNode = Node<WorkflowGraphNodeData, 'workflowNode'>;
type WorkflowReactEdge = Edge<WorkflowGraphEdgeData>;
type WorkflowSummaryMetric = {
  label: string;
  value: string;
  tone?: 'neutral' | 'good' | 'warn' | 'danger';
};

const NODE_WIDTH = 224;
const NODE_HEIGHT = 118;
const X_GAP = 292;
const Y_GAP = 150;

const nodeTypes = {
  workflowNode: WorkflowNodeCard,
};

function nodeKindLabel(kind: WorkflowNode['kind']): string {
  switch (kind) {
    case 'source':
      return 'Ingest';
    case 'transform':
      return 'Transform';
    case 'branch':
      return 'Branch';
    case 'enrichment':
      return 'Enrichment';
    case 'queue':
      return 'Queue';
    case 'destination':
      return 'Destination';
  }
}

function graphStatusLabel(model: WorkflowGraphModel): string {
  if (model.bottleneckSummary) return model.bottleneckSummary;
  if (model.nodes.some((node) => node.status === 'unknown')) return 'Workflow metrics pending';
  if (model.nodes.some((node) => node.status === 'idle')) return 'Workflow idle';
  return 'Workflow healthy';
}

function layoutNodes(model: WorkflowGraphModel, draggable: boolean): WorkflowReactNode[] {
  const depth = new Map<string, number>();
  const incoming = new Map<string, number>();
  const outgoing = new Map<string, WorkflowEdge[]>();

  for (const node of model.nodes) {
    incoming.set(node.id, 0);
    outgoing.set(node.id, []);
  }
  for (const edge of model.edges) {
    incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1);
    outgoing.set(edge.source, [...(outgoing.get(edge.source) ?? []), edge]);
  }

  const roots = model.nodes.filter((node) => (incoming.get(node.id) ?? 0) === 0);
  const queue = roots.length > 0 ? roots.map((node) => node.id) : model.nodes.slice(0, 1).map((node) => node.id);
  for (const root of queue) {
    depth.set(root, 0);
  }

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) continue;
    const nextDepth = (depth.get(current) ?? 0) + 1;
    for (const edge of outgoing.get(current) ?? []) {
      if ((depth.get(edge.target) ?? -1) < nextDepth) {
        depth.set(edge.target, nextDepth);
        queue.push(edge.target);
      }
    }
  }

  const rows = new Map<number, WorkflowNode[]>();
  for (const node of model.nodes) {
    const row = depth.get(node.id) ?? 0;
    rows.set(row, [...(rows.get(row) ?? []), node]);
  }

  const sortedRows = Array.from(rows.entries()).sort(([left], [right]) => left - right);
  const positioned = new Map<string, { x: number; y: number }>();
  for (const [row, rowNodes] of sortedRows) {
    const sortedNodes = [...rowNodes].sort((left, right) => left.id.localeCompare(right.id));
    const rowWidth = (sortedNodes.length - 1) * X_GAP;
    sortedNodes.forEach((node, index) => {
      positioned.set(node.id, {
        x: index * X_GAP - rowWidth / 2,
        y: row * Y_GAP,
      });
    });
  }

  return model.nodes.map((node) => ({
    id: node.id,
    type: 'workflowNode',
    data: node as WorkflowGraphNodeData,
    position: positioned.get(node.id) ?? { x: 0, y: 0 },
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    draggable,
    selectable: true,
  }));
}

function toReactEdges(edges: WorkflowEdge[]): WorkflowReactEdge[] {
  return edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    label: edge.label,
    type: 'smoothstep',
    animated: edge.bottleneck || edge.status === 'backlogged',
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 14,
      height: 14,
    },
    data: edge as WorkflowGraphEdgeData,
    className: cn(
      'workflow-edge',
      `is-${edge.status}`,
      edge.bottleneck ? 'is-bottleneck' : null,
    ),
    labelBgPadding: [8, 4],
    labelBgBorderRadius: 6,
  }));
}

function WorkflowNodeCard({ data, selected }: NodeProps<WorkflowReactNode>) {
  return (
    <div
      className={cn(
        'workflow-node',
        `is-${data.kind}`,
        `is-${data.status}`,
        data.bottleneck ? 'is-bottleneck' : null,
        selected ? 'is-selected' : null,
      )}
    >
      <Handle type="target" position={Position.Top} className="workflow-handle" />
      <div className="workflow-node-head">
        <span className="workflow-node-kind">{nodeKindLabel(data.kind)}</span>
        <span className={cn('workflow-node-state', `is-${data.status}`)}>
          {data.bottleneck ? 'bottleneck' : data.status}
        </span>
      </div>
      <strong className="workflow-node-title">{data.label}</strong>
      {data.detail ? <span className="workflow-node-detail">{data.detail}</span> : null}
      <div className="workflow-node-metrics">
        {data.metrics.map((metric) => (
          <span key={`${metric.label}-${metric.value}`} className={cn(metric.muted ? 'is-muted' : null)}>
            <em>{metric.label}</em>
            <strong>{metric.value}</strong>
          </span>
        ))}
      </div>
      {data.kind === 'queue' ? <span className="workflow-queue-waterline" aria-hidden /> : null}
      <Handle type="source" position={Position.Bottom} className="workflow-handle" />
    </div>
  );
}

export function WorkflowGraph({
  spec,
  deployment,
  adapterWorkloads,
  compact = false,
  large = false,
  summaryMetrics = [],
  designer = false,
  selectedNodeId,
  onNodeSelect,
  fullscreenable = true,
}: {
  spec?: BackendFlowSpec | null;
  deployment?: RuntimeDeploymentRecord | null;
  adapterWorkloads?: AdapterWorkloadRecord[];
  compact?: boolean;
  large?: boolean;
  summaryMetrics?: WorkflowSummaryMetric[];
  designer?: boolean;
  selectedNodeId?: string | null;
  onNodeSelect?: (node: WorkflowNode | null) => void;
  fullscreenable?: boolean;
}) {
  const [isFullscreen, setIsFullscreen] = useState(false);
  const model = useMemo(
    () => deriveWorkflowGraph({ spec, deployment, adapterWorkloads }),
    [adapterWorkloads, deployment, spec],
  );
  const laidOutNodes = useMemo(
    () => layoutNodes(model, designer),
    [designer, model],
  );
  const [nodes, setNodes, onNodesChange] = useNodesState<WorkflowReactNode>(laidOutNodes);
  const edges = useMemo(() => toReactEdges(model.edges), [model.edges]);
  const hasGraph = nodes.length > 0;

  useEffect(() => {
    setNodes(laidOutNodes);
  }, [laidOutNodes, setNodes]);

  useEffect(() => {
    setNodes((currentNodes) =>
      currentNodes.map((node) => ({
        ...node,
        selected: selectedNodeId === node.id,
      })),
    );
  }, [selectedNodeId, setNodes]);

  return (
    <section
      className={cn(
        'workflow-graph-shell',
        compact ? 'is-compact' : null,
        large ? 'is-large' : null,
        designer ? 'is-designer' : null,
        isFullscreen ? 'is-fullscreen' : null,
      )}
    >
      <div className="workflow-graph-summary">
        <span className={cn('workflow-graph-status', model.bottleneckSummary ? 'is-bottleneck' : null)}>
          {graphStatusLabel(model)}
        </span>
        {summaryMetrics.length > 0 ? (
          <div className="workflow-graph-summary-metrics" aria-label="Workflow statistics">
            {summaryMetrics.map((metric) => (
              <span
                key={metric.label}
                className={cn('workflow-graph-summary-metric', metric.tone ? `is-${metric.tone}` : null)}
              >
                <em>{metric.label}</em>
                <strong>{metric.value}</strong>
              </span>
            ))}
          </div>
        ) : null}
        {designer || (fullscreenable && !compact) ? (
          <div className="workflow-graph-actions">
            {designer ? <span className="workflow-graph-mode">Designer</span> : null}
            {fullscreenable && !compact ? (
              <button
                type="button"
                className="workflow-graph-action"
                onClick={() => setIsFullscreen((value) => !value)}
                aria-label={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
                title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
              >
                <Icon icon={isFullscreen ? ArrowsPointingInIcon : ArrowsPointingOutIcon} size="sm" />
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
      <div className="workflow-graph-canvas">
        {hasGraph ? (
          <ReactFlow<WorkflowReactNode, WorkflowReactEdge>
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onNodeClick={(_, node) => onNodeSelect?.(node.data as WorkflowNode)}
            onPaneClick={() => onNodeSelect?.(null)}
            nodesDraggable={designer}
            nodesConnectable={false}
            elementsSelectable
            fitView
            fitViewOptions={{ padding: compact ? 0.28 : 0.18 }}
            minZoom={0.2}
            maxZoom={1.4}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={18} size={1} />
            {!compact ? <Controls showInteractive={false} /> : null}
          </ReactFlow>
        ) : (
          <div className="workflow-graph-empty">
            <p>Workflow graph will appear after a flow spec is available.</p>
          </div>
        )}
      </div>
    </section>
  );
}
