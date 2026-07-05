import { useRef } from 'react';

function escapeHtml(value: string): string {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function highlightJson(value: string): string {
  return escapeHtml(value).replace(
    /("(?:\\.|[^"\\])*"(?=\s*:))|("(?:\\.|[^"\\])*")|\b(true|false)\b|\b(null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g,
    (match, key, stringValue, booleanValue, nullValue, numberValue) => {
      if (key) return `<span class="json-token-key">${key}</span>`;
      if (stringValue) return `<span class="json-token-string">${stringValue}</span>`;
      if (booleanValue) return `<span class="json-token-boolean">${booleanValue}</span>`;
      if (nullValue) return `<span class="json-token-null">${nullValue}</span>`;
      if (numberValue) return `<span class="json-token-number">${numberValue}</span>`;
      return match;
    },
  );
}

export function JsonSyntaxEditor({
  value,
  onChange,
  compact = false,
}: {
  value: string;
  onChange: (value: string) => void;
  compact?: boolean;
}) {
  const highlightRef = useRef<HTMLPreElement | null>(null);

  return (
    <div className={`json-syntax-editor ${compact ? 'json-syntax-editor-compact' : ''}`}>
      <pre
        ref={highlightRef}
        className="json-syntax-highlight"
        aria-hidden="true"
        dangerouslySetInnerHTML={{ __html: `${highlightJson(value)}\n` }}
      />
      <textarea
        className="json-syntax-input"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        onScroll={(event) => {
          if (!highlightRef.current) return;
          highlightRef.current.scrollTop = event.currentTarget.scrollTop;
          highlightRef.current.scrollLeft = event.currentTarget.scrollLeft;
        }}
        spellCheck={false}
      />
    </div>
  );
}
