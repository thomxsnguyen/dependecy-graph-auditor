export type JobStatus =
  | "pending"
  | "running"
  | "waiting"
  | "retry_scheduled"
  | "completed"
  | "failed"
  | "dead_lettered"
  | "cancelled"

export interface Job {
  id: string
  type: "demo" | "dependency_audit" | string
  payload?: unknown
  status: JobStatus
  attempts: number
  maxAttempts: number
  scheduledAt: string
  rootJobId?: string
  lockedBy?: string
  lockedUntil?: string
  cancelRequestedAt?: string
  lastErrorKind?: string
  lastError?: string
  createdAt: string
  startedAt?: string
  completedAt?: string
}

export interface Attempt {
  attempt: number
  workerId: string
  status: string
  startedAt: string
  finishedAt?: string
  errorKind?: string
  error?: string
}

export interface JobEvent {
  id: number
  type: string
  attempt?: number
  workerId?: string
  details?: unknown
  occurredAt: string
}

export interface AuditResult {
  ecosystem: string
  name: string
  version: string
  license: string
  verdict: string
  parentName?: string
  parentVersion?: string
}

export interface UpdateResult {
  jobId: string
  ecosystem: "npm" | "pypi" | "go"
  name: string
  currentVersion: string
  latestVersion: string
  staleness: "major" | "minor" | "patch" | "up_to_date" | "unknown"
}

export interface JobDetail {
  job: Job
  attempts: Attempt[]
  events: JobEvent[]
  result?: unknown
  childCounts?: Partial<Record<JobStatus, number>>
  updateResults?: UpdateResult[]
  auditResults?: AuditResult[]
  auditRelationships?: Array<{ ecosystem: string; parentName: string; parentVersion?: string; childName: string; childVersion: string }>
}

export interface JobPage {
  jobs: Job[]
  nextCursor?: string
  counts: Partial<Record<JobStatus, number>>
}

export interface DLQEntry {
  id: number
  jobId: string
  jobType: string
  payload: unknown
  attempts: number
  errorKind: string
  error: string
  deadAt: string
  replayedAt?: string
  replayedAsJobId?: string
}

export interface DLQPage {
  entries: DLQEntry[]
  nextCursor?: string
}
