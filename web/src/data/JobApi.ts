import type { DLQPage, Job, JobDetail, JobPage, JobStatus } from "../types/jobs"

interface APIErrorBody { error?: { message?: string } }

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  })
  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as APIErrorBody
    throw new Error(body.error?.message ?? `Request failed (${response.status})`)
  }
  return response.json() as Promise<T>
}

export const JobApi = {
  list(query: string, status: JobStatus | "", cursor = "") {
    const params = new URLSearchParams({ limit: "25" })
    if (query) params.set("q", query)
    if (status) params.set("status", status)
    if (cursor) params.set("cursor", cursor)
    return request<JobPage>(`/api/jobs?${params}`)
  },
  get(id: string) { return request<JobDetail>(`/api/jobs/${id}`) },
  submit(type: "demo" | "dependency_audit" | "dependency_update_scan", payload: unknown, maxAttempts = 5) {
    return request<{ job: Job }>("/api/jobs", {
      method: "POST",
      body: JSON.stringify({ type, payload, maxAttempts }),
    })
  },
  cancel(id: string) { return request<{ job: Job }>(`/api/jobs/${id}/cancel`, { method: "POST" }) },
  retry(id: string) { return request<{ job: Job }>(`/api/jobs/${id}/retry`, { method: "POST" }) },
  listDLQ() { return request<DLQPage>("/api/dlq?limit=50") },
  replay(id: number) { return request<{ job: Job }>(`/api/dlq/${id}/replay`, { method: "POST" }) },
}
