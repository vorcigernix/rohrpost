export function Sparkline({
  data,
  color,
  height = 34,
  fill = true,
}: {
  data: number[];
  color?: string;
  height?: number;
  fill?: boolean;
}) {
  if (!data.length) return null;
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = Math.max(max - min, 1);
  const w = 100;
  const step = w / Math.max(data.length - 1, 1);
  const path = data
    .map((v, i) => {
      const x = i * step;
      const y = height - ((v - min) / range) * height;
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  const area = `${path} L${w},${height} L0,${height} Z`;
  const resolvedColor = color ?? 'var(--accent-text)';
  return (
    <svg
      className="sparkline-svg"
      viewBox={`0 0 ${w} ${height}`}
      preserveAspectRatio="none"
      aria-hidden
    >
      {fill ? <path d={area} fill={resolvedColor} fillOpacity={0.12} /> : null}
      <path d={path} fill="none" stroke={resolvedColor} strokeWidth={1.5} />
    </svg>
  );
}

export function genSeries(seed: number, n = 40, base = 100, amp = 30): number[] {
  const out: number[] = [];
  let v = base;
  for (let i = 0; i < n; i++) {
    v += (Math.sin(i * 0.4 + seed) * 0.4 + (Math.random() - 0.5)) * amp;
    out.push(Math.max(0, v));
  }
  return out;
}
