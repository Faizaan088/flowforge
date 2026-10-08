import axios from "axios";

export const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL ?? "http://localhost:8000",
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem("flowforge_token");
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export type User = {
  id: number; username: string; email?: string | null; role: string;
  is_active: boolean; created_at: string; updated_at: string;
};
export type Execution = {
  id: number; job_definition_id: number; category?: string | null;
  priority: number; status: string; attempt: number; max_retries: number;
  worker_id?: string | null; lease_until?: string | null;
  available_at?: string | null; started_at?: string | null;
  finished_at?: string | null; error_summary?: string | null;
  created_at: string;
};
export type Policy = {
  id: number; target_type: string; target_id: string;
  max_concurrency?: number; max_requests?: number; window_seconds?: number;
  is_enabled: boolean; created_at: string; updated_at: string;
};

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
export async function me() { return (await api.get<User>("/auth/me")).data; }
export async function executions() { return (await api.get<Execution[]>("/executions/")).data; }
export async function cancelExecution(id: number, reason?: string) {
  return (await api.post(`/executions/${id}/cancel`, { reason })).data;
}
export async function summary() { return (await api.get("/system/summary")).data; }
export async function health() { return (await api.get("/health/ready")).data; }
export async function concurrencyPolicies() {
  return (await api.get<Policy[]>("/policies/concurrency")).data;
}
export async function ratePolicies() {
  return (await api.get<Policy[]>("/policies/rate-limit")).data;
}
