import type { ClusterLink, ClusterMember } from './admin';

export type LinkVisualState = 'connected' | 'failed' | 'stale' | 'unknown';
export type GraphNode = {
  id: string;
  x: number;
  y: number;
  self: boolean;
  member: ClusterMember;
};

export function graphLayout(self: ClusterMember, members: ClusterMember[]): GraphNode[] {
  const center: GraphNode = { id: self.node_id, x: 400, y: 220, self: true, member: self };
  const ordered = [...members].sort((a, b) => a.node_id.localeCompare(b.node_id));
  if (ordered.length === 0) return [center];
  const radiusX = ordered.length === 1 ? 240 : 285;
  const radiusY = ordered.length === 1 ? 0 : 155;
  return [
    center,
    ...ordered.map((member, index) => {
      const angle = -Math.PI / 2 + (index * Math.PI * 2) / ordered.length;
      return {
        id: member.node_id,
        x: Math.round(400 + Math.cos(angle) * radiusX),
        y: Math.round(220 + Math.sin(angle) * radiusY),
        self: false,
        member,
      };
    }),
  ];
}

export function classifyLink(
  link: ClusterLink | undefined,
  now: number,
  staleAfterMs: number,
): LinkVisualState {
  if (!link) return 'unknown';
  if (staleAfterMs > 0 && now - link.observed_at > staleAfterMs) return 'stale';
  return link.outcome;
}

export function linkPath(
  source: Pick<GraphNode, 'x' | 'y'>,
  target: Pick<GraphNode, 'x' | 'y'>,
  bend: number,
): string {
  const dx = target.x - source.x;
  const dy = target.y - source.y;
  const length = Math.hypot(dx, dy) || 1;
  const offset = 18 * bend;
  const cx = (source.x + target.x) / 2 - (dy / length) * offset;
  const cy = (source.y + target.y) / 2 + (dx / length) * offset;
  return `M ${source.x} ${source.y} Q ${cx} ${cy} ${target.x} ${target.y}`;
}
