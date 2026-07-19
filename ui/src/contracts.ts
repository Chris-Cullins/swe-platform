export interface Session {
  authenticated: true
  username: string
}

export interface Problem {
  type: string
  title: string
  status: number
  detail?: string
}

export interface Selector {
  environment?: string
  project?: string
  template?: string
}

export interface CreateRun {
  name: string
  selector: Selector
  agent: string
  prompt: string
}

export interface Run {
  name: string
  uid: string
  createdAt: string
  intent: {
    selector: Selector
    agent: string
    prompt: string
  }
  cancelRequested: boolean
  state: string
  environment?: {
    name: string
    ownership: 'Owned' | 'Claimed'
  }
  branch?: string
  usage: {
    cpuSeconds: number
    tokensIn: number
    tokensOut: number
  }
}

export interface RunList {
  items: Run[]
  continue?: string
}

export interface Environment {
  name: string
  uid: string
  createdAt: string
  project?: string
  template: string
  backend: string
  paused: boolean
  phase: string
  ready: boolean
  claim?: { runName: string; runUID: string }
  lastActiveAt?: string
}
