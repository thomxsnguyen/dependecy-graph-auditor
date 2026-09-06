import { act, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { OperationsDashboard } from "../src/app/OperationsDashboard"
import type { JobDetail, UpdateResult } from "../src/types/jobs"
import { JobApi } from "../src/data/JobApi"

vi.mock("../src/data/JobApi", () => ({
  JobApi: {
    list: vi.fn(), get: vi.fn(), submit: vi.fn(), cancel: vi.fn(), retry: vi.fn(),
    listDLQ: vi.fn(), replay: vi.fn(),
  },
}))

const row = {
  id: "12345678-abcd", type: "demo", payload: { durationMs: 0 }, status: "completed" as const,
  attempts: 1, maxAttempts: 5, scheduledAt: "2026-09-04T12:00:00Z",
  createdAt: "2026-09-04T12:00:00Z", completedAt: "2026-09-04T12:00:01Z",
}

beforeEach(() => {
  vi.mocked(JobApi.list).mockResolvedValue({ jobs: [row], counts: { completed: 1, running: 0 } })
  vi.mocked(JobApi.listDLQ).mockResolvedValue({ entries: [] })
  vi.mocked(JobApi.get).mockResolvedValue({
    job: row, attempts: [{ attempt: 1, workerId: "worker-1", status: "completed", startedAt: row.createdAt }],
    events: [{ id: 1, type: "submitted", occurredAt: row.createdAt }, { id: 2, type: "completed", workerId: "worker-1", occurredAt: row.completedAt }],
    result: { message: "done" },
  })
})

afterEach(() => vi.resetAllMocks())

it("shows queue totals and a selected job lifecycle", async () => {
  const user = userEvent.setup()
  render(<OperationsDashboard />)
  expect(await screen.findByText("12345678")).toBeInTheDocument()
  expect(screen.getByRole("region", { name: "Queue totals" })).toHaveTextContent("completed1")
  await user.click(screen.getByText("12345678"))
  await waitFor(() => expect(screen.getByText("Lifecycle")).toBeInTheDocument())
  expect(screen.getAllByText(/worker-1/)).toHaveLength(2)
  expect(screen.getByText(/"message": "done"/)).toBeInTheDocument()
})

it("submits the controlled retry demonstration", async () => {
  const user = userEvent.setup()
  vi.mocked(JobApi.submit).mockResolvedValue({ job: row })
  render(<OperationsDashboard />)
  await screen.findByText("12345678")
  await user.click(screen.getByRole("button", { name: /retry twice/i }))
  await waitFor(() => expect(JobApi.submit).toHaveBeenCalledWith("demo", { durationMs: 0, transientFailures: 2 }))
})

const updateRow = (jobId: string, staleness: UpdateResult["staleness"], name = jobId): UpdateResult => ({
  jobId, ecosystem: "npm", name, currentVersion: "1.0.0", latestVersion: "2.0.0", staleness,
})

function scanDetail(overrides: Partial<JobDetail> = {}): JobDetail {
  return {
    job: { ...row, id: "scan1234-root", rootJobId: "scan1234-root", type: "dependency_update_scan", status: "waiting" },
    attempts: [], events: [], childCounts: { completed: 1, pending: 1 },
    updateResults: [updateRow("child-one", "major")], ...overrides,
  }
}

async function selectScan(detail: JobDetail) {
  vi.mocked(JobApi.list).mockResolvedValue({ jobs: [detail.job], counts: {} })
  vi.mocked(JobApi.get).mockResolvedValue(detail)
  const user = userEvent.setup()
  render(<OperationsDashboard />)
  await user.click(await screen.findByText("scan1234"))
  await screen.findByRole("heading", { name: "Dependency updates" })
  return user
}

it("submits an update scan for the entered URL and selects it instead of the old job", async () => {
  const user = userEvent.setup()
  render(<OperationsDashboard />)
  await user.click(await screen.findByText("12345678"))
  await screen.findByText("Lifecycle")
  const detail = scanDetail()
  vi.mocked(JobApi.get).mockImplementation(async (id) => id === detail.job.id ? detail : { job: row, attempts: [], events: [] })
  let finish!: (value: { job: JobDetail["job"] }) => void
  vi.mocked(JobApi.submit).mockReturnValue(new Promise((resolve) => { finish = resolve }))
  const input = screen.getByLabelText("GitHub repository")
  await user.clear(input)
  await user.type(input, "https://github.com/thomxsnguyen/ArtistBlender")
  const button = screen.getByRole("button", { name: "Check for updates" })
  await user.click(button)
  expect(JobApi.submit).toHaveBeenCalledWith("dependency_update_scan", { repositoryUrl: "https://github.com/thomxsnguyen/ArtistBlender" })
  expect(button).toBeDisabled()
  await user.click(button)
  expect(JobApi.submit).toHaveBeenCalledTimes(1)
  await act(async () => finish({ job: detail.job }))
  await screen.findByRole("heading", { name: "Dependency updates" })
  expect(screen.getByRole("heading", { name: "scan1234" })).toBeInTheDocument()
})

it("keeps repository audit submission and URL validation", async () => {
  const user = userEvent.setup()
  vi.mocked(JobApi.submit).mockResolvedValue({ job: row })
  render(<OperationsDashboard />)
  await screen.findByText("12345678")
  await user.click(screen.getByRole("button", { name: "Audit dependencies" }))
  expect(JobApi.submit).toHaveBeenCalledWith("dependency_audit", { repositoryUrl: "https://github.com/example/project", ref: "main" })
  await waitFor(() => expect(screen.getByRole("button", { name: "Check for updates" })).toBeEnabled())
  vi.mocked(JobApi.submit).mockClear()
  await user.clear(screen.getByLabelText("GitHub repository"))
  await user.click(screen.getByRole("button", { name: "Check for updates" }))
  expect(JobApi.submit).not.toHaveBeenCalled()
  expect(screen.queryByRole("heading", { name: "Dependency updates" })).not.toBeInTheDocument()
})

it("orders update rows by urgency and stable ties without changing API data", async () => {
  const rows = [updateRow("unknown", "unknown"), updateRow("current", "up_to_date"), updateRow("patch", "patch"),
    updateRow("minor", "minor"), updateRow("major-z", "major", "z"), updateRow("major-a2", "major", "a"), updateRow("major-a1", "major", "a")]
  rows[4].ecosystem = "go"
  const original = rows.map((item) => item.jobId)
  await selectScan(scanDetail({ updateResults: rows, childCounts: { completed: 7 } }))
  const table = screen.getByRole("table", { name: "Dependency updates" })
  const names = within(table).getAllByRole("row").slice(1).map((element) => within(element).getAllByRole("cell")[0].textContent)
  expect(names).toEqual(["z", "a", "a", "minor", "patch", "current", "unknown"])
  for (const text of ["🔴 Major", "🟡 Minor", "🟢 Patch", "✅ Up to date", "Unknown"]) expect(within(table).getAllByText(text).length).toBeGreaterThan(0)
  expect(rows.map((item) => item.jobId)).toEqual(original)
  expect(screen.getByText(/may differ from the installed version/)).toBeInTheDocument()
})

it("shows partial progress and adds rows through the existing polling cycle", async () => {
  const detail = scanDetail()
  await selectScan(detail)
  expect(screen.getByText("1 of 2 checks completed")).toBeInTheDocument()
  const updated = { ...detail, updateResults: [...detail.updateResults!, updateRow("child-two", "patch")], childCounts: { completed: 2 } }
  vi.mocked(JobApi.get).mockResolvedValue(updated)
  await waitFor(() => expect(screen.getByText("child-two")).toBeInTheDocument(), { timeout: 4500 })
  expect(screen.getByText("2 of 2 checks completed")).toBeInTheDocument()
})

it.each(["failed", "dead_lettered", "cancelled"] as const)("retains successful rows for a %s scan", async (status) => {
  const detail = scanDetail()
  detail.job = { ...detail.job, status, lastError: "One check did not finish" }
  detail.childCounts = { completed: 1, [status]: 1 }
  await selectScan(detail)
  expect(screen.getByText("Incomplete")).toBeInTheDocument()
  expect(screen.getByText("child-one")).toBeInTheDocument()
  expect(screen.getByText("One check did not finish")).toBeInTheDocument()
  expect(screen.queryByText("All checked dependencies are up to date.")).not.toBeInTheDocument()
})

it("distinguishes a starting scan from an empty completed scan", async () => {
  const detail = scanDetail({ updateResults: undefined, childCounts: {} })
  const user = await selectScan(detail)
  expect(screen.getByText("Scanning dependency manifests…")).toBeInTheDocument()
  expect(screen.queryByText("No direct dependencies found.")).not.toBeInTheDocument()
  vi.mocked(JobApi.get).mockResolvedValue({ ...detail, job: { ...detail.job, status: "completed" } })
  await user.click(screen.getByRole("button", { name: "Refresh jobs" }))
  await screen.findByText("No direct dependencies found.")
  expect(screen.queryByText("Scanning dependency manifests…")).not.toBeInTheDocument()
})

it("only confirms up-to-date dependencies after successful completion without unknown rows", async () => {
  const detail = scanDetail({ updateResults: [updateRow("current", "up_to_date")], childCounts: { completed: 1 } })
  const user = await selectScan(detail)
  expect(screen.queryByText("All checked dependencies are up to date.")).not.toBeInTheDocument()
  const completed = { ...detail, job: { ...detail.job, status: "completed" as const } }
  vi.mocked(JobApi.get).mockResolvedValue(completed)
  await user.click(screen.getByRole("button", { name: "Refresh jobs" }))
  await screen.findByText("All checked dependencies are up to date.")
  vi.mocked(JobApi.get).mockResolvedValue({ ...completed, updateResults: [updateRow("unknown", "unknown")] })
  await user.click(screen.getByRole("button", { name: "Refresh jobs" }))
  await screen.findByText("Unknown")
  expect(screen.queryByText("All checked dependencies are up to date.")).not.toBeInTheDocument()
})

it("keeps existing rows when a refresh fails", async () => {
  const user = await selectScan(scanDetail())
  vi.mocked(JobApi.get).mockRejectedValue(new Error("Report refresh failed"))
  await user.click(screen.getByRole("button", { name: "Refresh jobs" }))
  expect(await screen.findByRole("alert")).toHaveTextContent("Report refresh failed")
  expect(screen.getByText("child-one")).toBeInTheDocument()
  expect(screen.queryByText("No direct dependencies found.")).not.toBeInTheDocument()
})
