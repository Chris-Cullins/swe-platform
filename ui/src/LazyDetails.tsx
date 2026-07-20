import { useState } from 'react'

function formatJSON(value: unknown): string {
  try { return JSON.stringify(value, null, 2) ?? String(value) } catch { return '[Unrenderable value]' }
}

export function LazyJSONDetails({ summary, value, className }: { summary: string; value: unknown; className?: string }) {
  const [open, setOpen] = useState(false)
  return <details className={className} onToggle={event => setOpen(event.currentTarget.open)}>
    <summary>{summary}</summary>{open && <pre>{formatJSON(value)}</pre>}
  </details>
}

export function LazyTextDetails({ summary, text }: { summary: string; text: string }) {
  const [open, setOpen] = useState(false)
  return <details onToggle={event => setOpen(event.currentTarget.open)}>
    <summary>{summary}</summary>{open && <pre>{text}</pre>}
  </details>
}
