export function RohrpostMark({ size = 16 }: { size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
      style={{ flexShrink: 0, transform: 'scaleX(-1)' }}
    >
      <path d="M4 16 V7 Q4 4 7 4 H17 Q20 4 20 7 V17 Q20 20 17 20 H11 Q8 20 8 17 V13 Q8 10 11 10 H12.5" />
      <rect x="14.8" y="8.9" width="3.4" height="2.2" rx="1.1" fill="currentColor" stroke="none" />
    </svg>
  );
}
