import { useEffect, useRef, useState } from 'react'
import { Terminal as XTerm } from '@xterm/xterm'
import { FitAddon } from '@xterm/addon-fit'
import '@xterm/xterm/css/xterm.css'
import { api } from './api'

export default function Terminal({ namespace, environment }: { namespace: string; environment: string }) {
  const host = useRef<HTMLDivElement>(null)
  const [status, setStatus] = useState('Connecting')

  useEffect(() => {
    if (!host.current) return
    const terminal = new XTerm()
    const fit = new FitAddon()
    terminal.loadAddon(fit)
    terminal.open(host.current)

    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
    const socket = new WebSocket(`${protocol}//${location.host}${api.terminalPath(namespace, environment)}`)
    socket.binaryType = 'arraybuffer'
    let opened = false
    let disposed = false
    const dimensions = (type: 'open' | 'resize') => JSON.stringify({ type, cols: terminal.cols, rows: terminal.rows })

    socket.onopen = () => {
      fit.fit()
      socket.send(dimensions('open'))
      opened = true
      setStatus('Connected')
    }
    socket.onerror = () => setStatus('Connection error')
    socket.onclose = () => setStatus('Disconnected')
    socket.onmessage = async event => {
      try {
        const bytes = event.data instanceof ArrayBuffer
          ? new Uint8Array(event.data)
          : event.data instanceof Blob
            ? new Uint8Array(await event.data.arrayBuffer())
            : undefined
        if (bytes && !disposed) terminal.write(bytes)
      } catch {
        if (!disposed) setStatus('Terminal data error')
      }
    }

    const input = terminal.onData(value => {
      if (socket.readyState === WebSocket.OPEN) socket.send(new TextEncoder().encode(value))
    })
    const observer = new ResizeObserver(() => {
      fit.fit()
      if (opened && socket.readyState === WebSocket.OPEN) socket.send(dimensions('resize'))
    })
    observer.observe(host.current)

    return () => {
      disposed = true
      observer.disconnect()
      input.dispose()
      socket.onopen = socket.onclose = socket.onerror = socket.onmessage = null
      socket.close()
      fit.dispose()
      terminal.dispose()
    }
  }, [namespace, environment])

  return <section>
    <p role="status" aria-live="polite">Terminal: {status}</p>
    <div className="terminal" ref={host} role="application" aria-label={`Terminal for ${environment}`} />
  </section>
}
