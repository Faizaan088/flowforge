import axios from "axios";

export const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL ?? "http://localhost:8000",
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem("flowforge_token");
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export type User = {
  id: number;
  username: string;
  email?: string | null;
  role: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
};

export type Execution = {
  id: number;
  job_definition_id?: number | null;
  category?: string | null;
  priority: number;
  status: string;
  attempt: number;
  max_retries: number;
  worker_id?: string | null;
  lease_until?: string | null;
  available_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  error_summary?: string | null;
  created_at: string;
};

export type Policy = {
  id: number;
  target_type: string;
  target_id: string;
  max_concurrency?: number;
  max_requests?: number;
  window_seconds?: number;
  is_enabled: boolean;
  created_at: string;
  updated_at: string;
};

export type Worker = {
  worker_id: string;
  status: string;
  created_at: string;
  last_heartbeat_at: string;
  is_alive: boolean;
};

export type WorkflowTask = {
  id: number;
  name: string;
  task_type: string;
  config?: Record<string, unknown> | null;
};

export type WorkflowEdge = {
  id: number;
  upstream_task_id: number;
  downstream_task_id: number;
};

export type WorkflowDefinition = {
  id: number;
  name: string;
  description?: string | null;
  created_at: string;
  tasks: WorkflowTask[];
  edges: WorkflowEdge[];
};

export type WorkflowTaskExecution = {
  id: number;
  workflow_task_id: number;
  task_name?: string | null;
  task_type?: string | null;
  execution_id?: number | null;
  status: string;
  attempt: number;
  started_at?: string | null;
  finished_at?: string | null;
  error_summary?: string | null;
};

export type WorkflowRun = {
  id: number;
  workflow_id: number;
  status: string;
  triggered_by?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  error_summary?: string | null;
  created_at: string;
  task_executions: WorkflowTaskExecution[];
};

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------
export async function login(username: string, password: string) {
  const { data } = await api.post("/auth/login", { username, password });
  localStorage.setItem("flowforge_token", data.access_token);
  localStorage.setItem("flowforge_user", JSON.stringify(data.user));
  return data;
}

export function logout() {
  localStorage.removeItem("flowforge_token");
  localStorage.removeItem("flowforge_user");
}

export async function me() {
  return (await api.get<User>("/auth/me")).data;
}

export async function listUsers() {
  return (await api.get<User[]>("/auth/users")).data;
}

export async function createUser(payload: { username: string; password: string; email?: string; role: string }) {
  return (await api.post<User>("/auth/users", payload)).data;
}

// ---------------------------------------------------------------------------
// Executions
// ---------------------------------------------------------------------------
export async function executions() {
  return (await api.get<Execution[]>("/executions/")).data;
}

export async function getExecution(id: number) {
  return (await api.get<Execution>(`/executions/${id}`)).data;
}

export async function cancelExecution(id: number, reason?: string) {
  return (await api.post(`/executions/${id}/cancel`, { reason })).data;
}

export async function updateExecutionPriority(id: number, priority: number) {
  return (await api.patch(`/executions/${id}/priority`, { priority })).data;
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------
export async function workers() {
  return (await api.get<Worker[]>("/workers/")).data;
}

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------
export async function workflows() {
  return (await api.get<WorkflowDefinition[]>("/workflows/")).data;
}

export async function getWorkflow(id: number) {
  return (await api.get<WorkflowDefinition>(`/workflows/${id}`)).data;
}

export async function createWorkflow(payload: {
  name: string;
  description?: string;
  tasks: Array<{ name: string; task_type?: string; config?: Record<string, unknown> }>;
  edges?: Array<[string, string] | [number, number]>;
}) {
  return (await api.post<WorkflowDefinition>("/workflows/", payload)).data;
}

export async function workflowRuns(workflowId?: number) {
  const url = workflowId ? `/workflows/runs/?workflow_id=${workflowId}` : "/workflows/runs/";
  return (await api.get<WorkflowRun[]>(url)).data;
}

export async function getWorkflowRun(runId: number) {
  return (await api.get<WorkflowRun>(`/workflows/runs/${runId}`)).data;
}

export async function triggerWorkflowRun(workflowId: number, triggeredBy = "DASHBOARD") {
  return (await api.post<WorkflowRun>(`/workflows/${workflowId}/runs`, { triggered_by: triggeredBy })).data;
}

export async function cancelWorkflowRun(runId: number, reason?: string) {
  return (await api.post(`/workflows/runs/${runId}/cancel`, { reason })).data;
}

// ---------------------------------------------------------------------------
// Policies
// ---------------------------------------------------------------------------
export async function concurrencyPolicies() {
  return (await api.get<Policy[]>("/policies/concurrency")).data;
}

export async function createConcurrencyPolicy(payload: {
  target_type: string;
  target_id: string;
  max_concurrency: number;
}) {
  return (await api.post<Policy>("/policies/concurrency", payload)).data;
}

export async function setConcurrencyPolicyEnabled(id: number, isEnabled: boolean) {
  const path = isEnabled ? `/policies/concurrency/${id}/enable` : `/policies/concurrency/${id}/disable`;
  return (await api.post<Policy>(path)).data;
}

export async function deleteConcurrencyPolicy(id: number) {
  return (await api.delete(`/policies/concurrency/${id}`)).data;
}

export async function ratePolicies() {
  return (await api.get<Policy[]>("/policies/rate-limit")).data;
}

export async function createRatePolicy(payload: {
  target_type: string;
  target_id: string;
  max_requests: number;
  window_seconds: number;
}) {
  return (await api.post<Policy>("/policies/rate-limit", payload)).data;
}

export async function setRatePolicyEnabled(id: number, isEnabled: boolean) {
  const path = isEnabled ? `/policies/rate-limit/${id}/enable` : `/policies/rate-limit/${id}/disable`;
  return (await api.post<Policy>(path)).data;
}

export async function deleteRatePolicy(id: number) {
  return (await api.delete(`/policies/rate-limit/${id}`)).data;
}

// ---------------------------------------------------------------------------
// System
// ---------------------------------------------------------------------------
export async function summary() {
  return (await api.get("/system/summary")).data;
}

export async function health() {
  return (await api.get("/health/ready")).data;
}

export async function reconcileQueue() {
  return (await api.post("/system/reconcile-queue")).data;
}

export async function systemSweep() {
  return (await api.post("/system/sweep")).data;
}
