import React, { useEffect, useMemo, useState } from "react";
import { BrowserRouter, NavLink, Route, Routes, useLocation } from "react-router-dom";
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  Boxes,
  CheckCircle2,
  Clock,
  CornerDownRight,
  Filter,
  Gauge,
  Key,
  LayoutDashboard,
  LogOut,
  Menu,
  Network,
  Play,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  Sliders,
  Trash2,
  Users,
  X,
  XCircle,
} from "lucide-react";
import {
  cancelExecution,
  cancelWorkflowRun,
  concurrencyPolicies,
  createConcurrencyPolicy,
  createRatePolicy,
  createUser,
  createWorkflow,
  deleteConcurrencyPolicy,
  deleteRatePolicy,
  executions,
  health,
  listUsers,
  login,
  me,
  ratePolicies,
  reconcileQueue,
  setConcurrencyPolicyEnabled,
  setRatePolicyEnabled,
  summary,
  systemSweep,
  triggerWorkflowRun,
  updateExecutionPriority,
  workers,
  workflowRuns,
  workflows,
  type Execution,
  type Policy,
  type User,
  type Worker,
  type WorkflowDefinition,
  type WorkflowRun,
} from "./api";
import { connectEvents, type LiveEvent } from "./ws";

// ---------------------------------------------------------------------------
// RBAC Helpers
// ---------------------------------------------------------------------------
const ROLE_PERMISSIONS: Record<string, string[]> = {
  admin: [
    "executions:read",
    "executions:trigger",
    "executions:cancel",
    "executions:priority",
    "workflows:read",
    "workflows:manage",
    "policies:read",
    "policies:manage",
    "system:reconcile",
    "users:manage",
  ],
  operator: [
    "executions:read",
    "executions:trigger",
    "executions:cancel",
    "executions:priority",
    "workflows:read",
    "workflows:manage",
    "policies:read",
  ],
  observer: ["executions:read", "workflows:read", "policies:read"],
};

function can(user: User | null, permission: string): boolean {
  if (!user) return false;
  return ROLE_PERMISSIONS[user.role]?.includes(permission) ?? false;
}

// ---------------------------------------------------------------------------
// Common UI Components
// ---------------------------------------------------------------------------
function Card({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return (
    <div
      className={`rounded-2xl border border-slate-800 bg-slate-900/75 p-5 shadow-xl shadow-black/20 backdrop-blur-sm ${className}`}
    >
      {children}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const s = status.toUpperCase();
  let color = "bg-slate-800 text-slate-300 border-slate-700";
  let icon = <Clock size={12} />;

  if (["SUCCEEDED", "ACTIVE", "READY"].includes(s)) {
    color = "bg-emerald-500/15 text-emerald-300 border-emerald-500/30";
    icon = <CheckCircle2 size={12} />;
  } else if (["RUNNING", "CLAIMED"].includes(s)) {
    color = "bg-cyan-500/15 text-cyan-300 border-cyan-500/30";
    icon = <RefreshCw size={12} className="animate-spin" />;
  } else if (["FAILED", "DEAD_LETTERED"].includes(s)) {
    color = "bg-rose-500/15 text-rose-300 border-rose-500/30";
    icon = <XCircle size={12} />;
  } else if (["CANCELLED"].includes(s)) {
    color = "bg-zinc-500/20 text-zinc-400 border-zinc-600/30";
    icon = <AlertCircle size={12} />;
  } else if (["QUEUED", "RETRY_WAIT", "PENDING"].includes(s)) {
    color = "bg-amber-500/15 text-amber-300 border-amber-500/30";
    icon = <AlertTriangle size={12} />;
  }

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium tracking-wide ${color}`}
    >
      {icon}
      {s}
    </span>
  );
}

function RoleBadge({ role }: { role: string }) {
  const color =
    role === "admin"
      ? "bg-purple-500/20 text-purple-300 border-purple-500/30"
      : role === "operator"
      ? "bg-cyan-500/20 text-cyan-300 border-cyan-500/30"
      : "bg-slate-700/50 text-slate-300 border-slate-600";
  return (
    <span className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-xs uppercase font-semibold tracking-wider ${color}`}>
      <Key size={10} />
      {role}
    </span>
  );
}

function Stat({
  label,
  value,
  icon,
  hint,
}: {
  label: string;
  value: unknown;
  icon: React.ReactNode;
  hint?: string;
}) {
  return (
    <Card>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-400">{label}</p>
          <p className="mt-2 text-3xl font-bold tracking-tight text-slate-100">{String(value)}</p>
          {hint && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-950 p-2.5 text-cyan-400">
          {icon}
        </div>
      </div>
    </Card>
  );
}

function Modal({
  title,
  isOpen,
  onClose,
  children,
}: {
  title: string;
  isOpen: boolean;
  onClose: () => void;
  children: React.ReactNode;
}) {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
      <div className="relative w-full max-w-lg rounded-3xl border border-slate-800 bg-slate-900 p-6 shadow-2xl">
        <div className="flex items-center justify-between border-b border-slate-800 pb-4">
          <h3 className="text-lg font-bold text-slate-100">{title}</h3>
          <button
            onClick={onClose}
            className="rounded-lg p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-100"
          >
            <X size={18} />
          </button>
        </div>
        <div className="mt-4">{children}</div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Login View
// ---------------------------------------------------------------------------
function Login({ onLogin }: { onLogin: (u: User) => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const result = await login(username, password);
      onLogin(result.user);
    } catch {
      setError("Invalid username or password, or API is unreachable.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-slate-950 p-6">
      <form
        onSubmit={submit}
        className="w-full max-w-md rounded-3xl border border-slate-800 bg-slate-900/90 p-8 shadow-2xl backdrop-blur-md"
      >
        <div className="mb-8">
          <div className="flex items-center gap-2">
            <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
              <Network size={20} />
            </span>
            <div className="text-3xl font-black tracking-tight text-white">
              Flow<span className="text-cyan-400">Forge</span>
            </div>
          </div>
          <p className="mt-2 text-sm text-slate-400">
            Durable workflow orchestration and control plane
          </p>
        </div>

        {error && (
          <div className="mb-5 flex items-center gap-2 rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-300">
            <AlertCircle size={16} />
            <span>{error}</span>
          </div>
        )}

        <label className="block text-sm font-medium text-slate-300">
          Username
          <input
            required
            autoFocus
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="admin"
            className="mt-1.5 mb-4 w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none transition-colors focus:border-cyan-500"
          />
        </label>

        <label className="block text-sm font-medium text-slate-300">
          Password
          <input
            required
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="••••••••"
            className="mt-1.5 mb-6 w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none transition-colors focus:border-cyan-500"
          />
        </label>

        <button
          disabled={busy}
          type="submit"
          className="flex w-full items-center justify-center gap-2 rounded-xl bg-cyan-500 px-4 py-3 font-semibold text-slate-950 transition hover:bg-cyan-400 disabled:opacity-50"
        >
          {busy ? (
            <>
              <RefreshCw size={16} className="animate-spin" />
              <span>Authenticating…</span>
            </>
          ) : (
            "Sign In"
          )}
        </button>

        <p className="mt-6 text-center text-xs text-slate-500">
          Default bootstrap: <code className="text-slate-400 font-mono">admin / admin123</code>
        </p>
      </form>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Navigation & Layout
// ---------------------------------------------------------------------------
const NAV_ITEMS = [
  ["/", "Dashboard", LayoutDashboard],
  ["/executions", "Executions", Activity],
  ["/workflows", "Workflows", Network],
  ["/workers", "Workers", Boxes],
  ["/policies", "Policies", ShieldCheck],
  ["/system", "System", Gauge],
  ["/users", "Users", Users],
] as const;

function Layout({ user, onLogout }: { user: User; onLogout: () => void }) {
  const [mobile, setMobile] = useState(false);
  const location = useLocation();

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      {/* Sidebar */}
      <aside
        className={`fixed inset-y-0 left-0 z-30 w-64 border-r border-slate-800 bg-slate-950 p-4 transition-transform lg:translate-x-0 ${
          mobile ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between px-3 py-3">
          <div className="flex items-center gap-2">
            <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
              <Network size={18} />
            </span>
            <div className="text-2xl font-black text-white">
              Flow<span className="text-cyan-400">Forge</span>
            </div>
          </div>
          <button
            className="lg:hidden text-slate-400 hover:text-white"
            onClick={() => setMobile(false)}
          >
            <X size={20} />
          </button>
        </div>
        <p className="px-3 pb-5 text-xs font-bold uppercase tracking-widest text-slate-500">
          Control Plane
        </p>
        <nav className="space-y-1">
          {NAV_ITEMS.map(([to, label, Icon]) => (
            <NavLink
              key={to}
              to={to}
              onClick={() => setMobile(false)}
              className={({ isActive }) =>
                `flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-cyan-500/10 text-cyan-300 border border-cyan-500/20"
                    : "text-slate-400 hover:bg-slate-900 hover:text-slate-100"
                }`
              }
            >
              <Icon size={18} />
              {label}
            </NavLink>
          ))}
        </nav>

        {/* User profile card in sidebar */}
        <div className="absolute bottom-4 left-4 right-4 rounded-2xl border border-slate-800 bg-slate-900/90 p-3.5 backdrop-blur-sm">
          <div className="flex items-center justify-between">
            <div className="truncate">
              <div className="text-sm font-semibold truncate text-white">{user.username}</div>
              <div className="mt-0.5">
                <RoleBadge role={user.role} />
              </div>
            </div>
            <button
              onClick={onLogout}
              title="Sign out"
              className="rounded-lg p-2 text-slate-400 hover:bg-slate-800 hover:text-rose-300"
            >
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <div className="lg:pl-64">
        <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-slate-800 bg-slate-950/90 px-6 backdrop-blur">
          <div className="flex items-center gap-3">
            <button className="lg:hidden text-slate-400" onClick={() => setMobile(true)}>
              <Menu size={22} />
            </button>
            <div>
              <div className="text-sm font-semibold text-slate-200">
                {NAV_ITEMS.find((n) => n[0] === location.pathname)?.[1] ?? "FlowForge"}
              </div>
              <div className="text-xs text-slate-500">Production Control Plane</div>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <ApiHealthIndicator />
          </div>
        </header>

        <main className="p-6 md:p-8">
          <Routes>
            <Route path="/" element={<Dashboard user={user} />} />
            <Route path="/executions" element={<Executions user={user} />} />
            <Route path="/workflows" element={<Workflows user={user} />} />
            <Route path="/workers" element={<WorkersView />} />
            <Route path="/policies" element={<Policies user={user} />} />
            <Route path="/system" element={<System user={user} />} />
            <Route path="/users" element={<UsersPage user={user} />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}

function ApiHealthIndicator() {
  const [h, setH] = useState<{ status?: string; database?: string; redis?: string } | null>(null);

  useEffect(() => {
    const check = () => health().then(setH).catch(() => setH({ status: "unavailable" }));
    check();
    const id = setInterval(check, 15000);
    return () => clearInterval(id);
  }, []);

  const ready = h?.status === "ready" || h?.status === "healthy";
  return (
    <div className="flex items-center gap-2 rounded-full border border-slate-800 bg-slate-900 px-3 py-1 text-xs text-slate-300">
      <span
        className={`h-2 w-2 rounded-full ${
          ready ? "bg-emerald-400 animate-pulse" : "bg-rose-400"
        }`}
      />
      <span className="capitalize">{h?.status ?? "connecting…"}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 1. Dashboard View
// ---------------------------------------------------------------------------
function Dashboard({ user }: { user: User }) {
  const [data, setData] = useState<Record<string, unknown>>({});
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [loading, setLoading] = useState(true);

  const refreshSummary = () => {
    summary()
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    refreshSummary();
    const id = setInterval(refreshSummary, 8000);
    const disconnect = connectEvents((e) => {
      setEvents((prev) => [e, ...prev].slice(0, 10));
    });
    return () => {
      clearInterval(id);
      disconnect();
    };
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">System Overview</h1>
          <p className="mt-1 text-sm text-slate-400">
            Real-time telemetry and throughput across cluster nodes.
          </p>
        </div>
        <button
          onClick={refreshSummary}
          className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
        >
          <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          Refresh
        </button>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Stat
          label="Queued"
          value={data.queued ?? data.queued_executions ?? 0}
          icon={<Clock size={20} />}
          hint="Durable waiting in PostgreSQL"
        />
        <Stat
          label="Running / Claimed"
          value={data.running ?? data.running_executions ?? 0}
          icon={<Play size={20} />}
          hint="Active execution capacity"
        />
        <Stat
          label="Active Workers"
          value={data.active_workers ?? 0}
          icon={<Boxes size={20} />}
          hint="Liveness heartbeats received"
        />
        <Stat
          label="Dead Lettered"
          value={data.dead_lettered ?? data.dead_lettered_executions ?? 0}
          icon={<AlertTriangle size={20} />}
          hint="Exhausted maximum retries"
        />
      </div>

      <div className="grid gap-6 xl:grid-cols-3">
        <Card className="xl:col-span-2">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <span className="h-2 w-2 rounded-full bg-cyan-400 animate-pulse" />
              <h2 className="text-lg font-semibold text-white">Live Execution Events</h2>
            </div>
            <span className="text-xs text-slate-500">Redis Pub/Sub WebSocket Stream</span>
          </div>
          {events.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-slate-500">
              <RefreshCw size={24} className="mb-2 animate-spin opacity-40" />
              <p className="text-sm">Listening for live execution & workflow events…</p>
            </div>
          ) : (
            <div className="space-y-2">
              {events.map((e, i) => (
                <div
                  key={`${e.event_id ?? i}-${e.timestamp ?? i}`}
                  className="flex items-center justify-between rounded-xl border border-slate-800 bg-slate-950 px-4 py-2.5 text-sm"
                >
                  <div className="flex items-center gap-2.5">
                    <span className="font-mono text-xs text-cyan-400 font-medium">
                      {e.event_type}
                    </span>
                    <span className="text-xs text-slate-500">
                      {e.entity_type ? `${e.entity_type} #${e.entity_id}` : ""}
                    </span>
                  </div>
                  <span className="font-mono text-xs text-slate-500">
                    {e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : "just now"}
                  </span>
                </div>
              ))}
            </div>
          )}
        </Card>

        <Card>
          <h2 className="mb-4 text-lg font-semibold text-white">Cluster Health</h2>
          <div className="space-y-3">
            <div className="rounded-xl border border-slate-800 bg-slate-950 p-3.5">
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-400">PostgreSQL Store</span>
                <span className="text-emerald-400 font-semibold flex items-center gap-1">
                  <CheckCircle2 size={14} /> Healthy
                </span>
              </div>
              <p className="mt-1 text-xs text-slate-500">Durable source of truth</p>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950 p-3.5">
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-400">Redis Coordination</span>
                <span className="text-emerald-400 font-semibold flex items-center gap-1">
                  <CheckCircle2 size={14} /> Connected
                </span>
              </div>
              <p className="mt-1 text-xs text-slate-500">Transient queue & pub/sub</p>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950 p-3.5">
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-400">User Role</span>
                <RoleBadge role={user.role} />
              </div>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Executions View
// ---------------------------------------------------------------------------
function Executions({ user }: { user: User }) {
  const [rows, setRows] = useState<Execution[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedExec, setSelectedExec] = useState<Execution | null>(null);
  const [filterStatus, setFilterStatus] = useState<string>("ALL");
  const [searchQuery, setSearchQuery] = useState("");
  const [priorityModal, setPriorityModal] = useState<{ id: number; current: number } | null>(null);
  const [newPriority, setNewPriority] = useState<number>(0);

  const load = () => {
    executions()
      .then(setRows)
      .catch(() => setRows([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, []);

  async function handleCancel(id: number) {
    if (!confirm(`Cancel execution #${id}?`)) return;
    try {
      await cancelExecution(id, "Cancelled by dashboard operator");
      await load();
      if (selectedExec?.id === id) {
        setSelectedExec(null);
      }
    } catch (e: unknown) {
      alert("Failed to cancel execution: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handlePrioritySave(e: React.FormEvent) {
    e.preventDefault();
    if (!priorityModal) return;
    try {
      await updateExecutionPriority(priorityModal.id, newPriority);
      setPriorityModal(null);
      await load();
    } catch (err: unknown) {
      alert("Failed to update priority: " + (err instanceof Error ? err.message : String(err)));
    }
  }

  const filtered = useMemo(() => {
    return rows.filter((r) => {
      if (filterStatus !== "ALL" && r.status !== filterStatus) return false;
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        const matchId = String(r.id).includes(q);
        const matchJob = String(r.job_definition_id ?? "").includes(q);
        const matchWorker = (r.worker_id ?? "").toLowerCase().includes(q);
        if (!matchId && !matchJob && !matchWorker) return false;
      }
      return true;
    });
  }, [rows, filterStatus, searchQuery]);

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Executions</h1>
          <p className="mt-1 text-sm text-slate-400">
            Durable task execution lifecycle, priorities, and lease tracking.
          </p>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-2 self-start rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
        >
          <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          Refresh
        </button>
      </div>

      {/* Filter and search bar */}
      <Card className="flex flex-wrap items-center justify-between gap-4 py-3">
        <div className="flex items-center gap-2">
          <Filter size={16} className="text-slate-500" />
          <span className="text-xs font-medium text-slate-400">Status:</span>
          <select
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value)}
            className="rounded-xl border border-slate-800 bg-slate-950 px-3 py-1.5 text-xs text-slate-200 outline-none focus:border-cyan-500"
          >
            <option value="ALL">All Statuses ({rows.length})</option>
            <option value="QUEUED">QUEUED</option>
            <option value="CLAIMED">CLAIMED</option>
            <option value="RUNNING">RUNNING</option>
            <option value="SUCCEEDED">SUCCEEDED</option>
            <option value="FAILED">FAILED</option>
            <option value="RETRY_WAIT">RETRY_WAIT</option>
            <option value="DEAD_LETTERED">DEAD_LETTERED</option>
            <option value="CANCELLED">CANCELLED</option>
          </select>
        </div>

        <div className="flex items-center gap-2">
          <Search size={16} className="text-slate-500" />
          <input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search by ID, Job, Worker…"
            className="w-56 rounded-xl border border-slate-800 bg-slate-950 px-3 py-1.5 text-xs text-slate-200 outline-none focus:border-cyan-500"
          />
        </div>
      </Card>

      {/* Table */}
      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Execution ID</th>
                <th>Status</th>
                <th>Job Definition</th>
                <th>Attempt</th>
                <th>Priority</th>
                <th>Worker</th>
                <th>Created</th>
                <th className="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-slate-500">
                    <RefreshCw size={20} className="mx-auto mb-2 animate-spin opacity-40" />
                    Loading executions…
                  </td>
                </tr>
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-slate-500">
                    No executions match your criteria.
                  </td>
                </tr>
              ) : (
                filtered.map((x) => (
                  <tr
                    key={x.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3.5 font-mono text-cyan-400 font-medium">#{x.id}</td>
                    <td>
                      <StatusBadge status={x.status} />
                    </td>
                    <td className="text-slate-300">
                      {x.job_definition_id ? `Job #${x.job_definition_id}` : "Workflow Task"}
                    </td>
                    <td className="text-slate-300">
                      {x.attempt} / {x.max_retries}
                    </td>
                    <td>
                      <span className="inline-flex items-center gap-1 rounded bg-slate-950 px-2 py-0.5 text-xs font-mono font-semibold text-slate-300">
                        {x.priority}
                      </span>
                    </td>
                    <td className="text-xs font-mono text-slate-400">{x.worker_id ?? "—"}</td>
                    <td className="text-xs text-slate-500">
                      {new Date(x.created_at).toLocaleTimeString()}
                    </td>
                    <td className="text-right">
                      <div className="flex items-center justify-end gap-2">
                        <button
                          onClick={() => setSelectedExec(x)}
                          className="rounded-lg bg-slate-800 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-700"
                        >
                          Inspect
                        </button>
                        {can(user, "executions:priority") && x.status === "QUEUED" && (
                          <button
                            onClick={() => {
                              setPriorityModal({ id: x.id, current: x.priority });
                              setNewPriority(x.priority);
                            }}
                            className="rounded-lg border border-slate-700 px-2.5 py-1 text-xs text-slate-300 hover:border-cyan-500 hover:text-cyan-300"
                          >
                            Priority
                          </button>
                        )}
                        {can(user, "executions:cancel") &&
                          ["QUEUED", "CLAIMED", "RUNNING", "RETRY_WAIT"].includes(x.status) && (
                            <button
                              onClick={() => handleCancel(x.id)}
                              className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-2.5 py-1 text-xs text-rose-300 hover:bg-rose-500/20"
                            >
                              Cancel
                            </button>
                          )}
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Inspect Execution Modal */}
      <Modal
        title={`Execution #${selectedExec?.id ?? ""}`}
        isOpen={selectedExec !== null}
        onClose={() => setSelectedExec(null)}
      >
        {selectedExec && (
          <div className="space-y-4 text-sm">
            <div className="grid grid-cols-2 gap-3 rounded-xl border border-slate-800 bg-slate-950 p-4">
              <div>
                <span className="text-xs text-slate-500">Status</span>
                <div className="mt-1">
                  <StatusBadge status={selectedExec.status} />
                </div>
              </div>
              <div>
                <span className="text-xs text-slate-500">Priority</span>
                <p className="mt-1 font-mono font-bold text-white">{selectedExec.priority}</p>
              </div>
              <div>
                <span className="text-xs text-slate-500">Attempt Count</span>
                <p className="mt-1 font-mono text-slate-200">
                  {selectedExec.attempt} (Max {selectedExec.max_retries})
                </p>
              </div>
              <div>
                <span className="text-xs text-slate-500">Assigned Worker</span>
                <p className="mt-1 font-mono text-slate-200">{selectedExec.worker_id ?? "None"}</p>
              </div>
              <div>
                <span className="text-xs text-slate-500">Category</span>
                <p className="mt-1 text-slate-200">{selectedExec.category ?? "default"}</p>
              </div>
              <div>
                <span className="text-xs text-slate-500">Lease Until</span>
                <p className="mt-1 font-mono text-xs text-slate-400">
                  {selectedExec.lease_until
                    ? new Date(selectedExec.lease_until).toLocaleTimeString()
                    : "—"}
                </p>
              </div>
            </div>

            {selectedExec.error_summary && (
              <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-xs text-rose-300">
                <span className="font-semibold block mb-1">Error Summary</span>
                {selectedExec.error_summary}
              </div>
            )}

            <div className="text-xs text-slate-500">
              Created: {new Date(selectedExec.created_at).toLocaleString()}
            </div>

            <div className="flex justify-end gap-2 pt-2">
              {can(user, "executions:cancel") &&
                ["QUEUED", "CLAIMED", "RUNNING", "RETRY_WAIT"].includes(selectedExec.status) && (
                  <button
                    onClick={() => handleCancel(selectedExec.id)}
                    className="rounded-xl bg-rose-600 px-4 py-2 font-semibold text-white hover:bg-rose-500"
                  >
                    Cancel Execution
                  </button>
                )}
            </div>
          </div>
        )}
      </Modal>

      {/* Priority Modal */}
      <Modal
        title={`Set Priority for Execution #${priorityModal?.id ?? ""}`}
        isOpen={priorityModal !== null}
        onClose={() => setPriorityModal(null)}
      >
        <form onSubmit={handlePrioritySave} className="space-y-4">
          <p className="text-xs text-slate-400">
            Higher numbers claim ahead of lower numbers in the queue.
          </p>
          <label className="block text-sm font-medium text-slate-300">
            Priority Level
            <input
              type="number"
              value={newPriority}
              onChange={(e) => setNewPriority(parseInt(e.target.value) || 0)}
              className="mt-1 w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={() => setPriorityModal(null)}
              className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
            >
              Save Priority
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. Workflows View
// ---------------------------------------------------------------------------
function Workflows({ user }: { user: User }) {
  const [tab, setTab] = useState<"definitions" | "runs">("definitions");
  const [wfs, setWfs] = useState<WorkflowDefinition[]>([]);
  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedWf, setSelectedWf] = useState<WorkflowDefinition | null>(null);
  const [selectedRun, setSelectedRun] = useState<WorkflowRun | null>(null);
  const [createModal, setCreateModal] = useState(false);

  // New workflow form state
  const [wfName, setWfName] = useState("");
  const [wfDesc, setWfDesc] = useState("");
  const [wfTasks, setWfTasks] = useState("extract, transform, load");
  const [busyCreate, setBusyCreate] = useState(false);

  const loadData = () => {
    workflows()
      .then(setWfs)
      .catch(() => setWfs([]));
    workflowRuns()
      .then(setRuns)
      .catch(() => setRuns([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadData();
    const id = setInterval(loadData, 6000);
    return () => clearInterval(id);
  }, []);

  async function handleTrigger(wfId: number) {
    if (!can(user, "workflows:manage")) return;
    try {
      await triggerWorkflowRun(wfId, "DASHBOARD");
      await loadData();
      setTab("runs");
    } catch (e: unknown) {
      alert("Failed to trigger run: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handleCancelRun(runId: number) {
    if (!confirm(`Cancel workflow run #${runId}?`)) return;
    try {
      await cancelWorkflowRun(runId, "Cancelled by dashboard operator");
      await loadData();
    } catch (e: unknown) {
      alert("Failed to cancel run: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handleCreateWorkflow(e: React.FormEvent) {
    e.preventDefault();
    setBusyCreate(true);
    try {
      const taskNames = wfTasks
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean);
      const tasks = taskNames.map((name) => ({ name, task_type: "STANDARD" }));
      const edges: [string, string][] = [];
      for (let i = 0; i < taskNames.length - 1; i++) {
        edges.push([taskNames[i], taskNames[i + 1]]);
      }
      await createWorkflow({
        name: wfName,
        description: wfDesc || undefined,
        tasks,
        edges,
      });
      setCreateModal(false);
      setWfName("");
      setWfDesc("");
      await loadData();
    } catch (err: unknown) {
      alert("Failed to create workflow: " + (err instanceof Error ? err.message : String(err)));
    } finally {
      setBusyCreate(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Workflows</h1>
          <p className="mt-1 text-sm text-slate-400">
            Directed Acyclic Graph (DAG) definitions, runs, and task progression.
          </p>
        </div>
        <div className="flex items-center gap-3">
          {can(user, "workflows:manage") && (
            <button
              onClick={() => setCreateModal(true)}
              className="flex items-center gap-2 rounded-xl bg-cyan-500 px-3.5 py-2 text-xs font-semibold text-slate-950 hover:bg-cyan-400"
            >
              <Plus size={16} />
              New Workflow
            </button>
          )}
          <button
            onClick={loadData}
            className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-slate-800 gap-4">
        <button
          onClick={() => setTab("definitions")}
          className={`pb-3 text-sm font-semibold transition-colors ${
            tab === "definitions"
              ? "border-b-2 border-cyan-400 text-cyan-400"
              : "text-slate-400 hover:text-slate-200"
          }`}
        >
          Workflow Definitions ({wfs.length})
        </button>
        <button
          onClick={() => setTab("runs")}
          className={`pb-3 text-sm font-semibold transition-colors ${
            tab === "runs"
              ? "border-b-2 border-cyan-400 text-cyan-400"
              : "text-slate-400 hover:text-slate-200"
          }`}
        >
          Workflow Runs ({runs.length})
        </button>
      </div>

      {tab === "definitions" && (
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="pb-3">ID</th>
                  <th>Name</th>
                  <th>Description</th>
                  <th>Tasks</th>
                  <th>Edges</th>
                  <th>Created</th>
                  <th className="text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {wfs.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="py-12 text-center text-slate-500">
                      No workflows defined yet. Click "New Workflow" to create one.
                    </td>
                  </tr>
                ) : (
                  wfs.map((w) => (
                    <tr
                      key={w.id}
                      className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                    >
                      <td className="py-3.5 font-mono text-cyan-400">#{w.id}</td>
                      <td className="font-semibold text-white">{w.name}</td>
                      <td className="text-slate-400">{w.description ?? "—"}</td>
                      <td>
                        <span className="rounded bg-slate-950 px-2 py-0.5 text-xs font-mono">
                          {w.tasks?.length ?? 0} tasks
                        </span>
                      </td>
                      <td>
                        <span className="rounded bg-slate-950 px-2 py-0.5 text-xs font-mono">
                          {w.edges?.length ?? 0} edges
                        </span>
                      </td>
                      <td className="text-xs text-slate-500">
                        {new Date(w.created_at).toLocaleDateString()}
                      </td>
                      <td className="text-right">
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={() => setSelectedWf(w)}
                            className="rounded-lg bg-slate-800 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-700"
                          >
                            Inspect DAG
                          </button>
                          {can(user, "workflows:manage") && (
                            <button
                              onClick={() => handleTrigger(w.id)}
                              className="flex items-center gap-1 rounded-lg border border-cyan-500/30 bg-cyan-500/10 px-2.5 py-1 text-xs text-cyan-300 hover:bg-cyan-500/20"
                            >
                              <Play size={12} />
                              Run
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {tab === "runs" && (
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="pb-3">Run ID</th>
                  <th>Workflow</th>
                  <th>Status</th>
                  <th>Triggered By</th>
                  <th>Tasks</th>
                  <th>Created</th>
                  <th className="text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {runs.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="py-12 text-center text-slate-500">
                      No workflow runs triggered yet.
                    </td>
                  </tr>
                ) : (
                  runs.map((r) => (
                    <tr
                      key={r.id}
                      className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                    >
                      <td className="py-3.5 font-mono text-cyan-400">#{r.id}</td>
                      <td className="text-slate-300 font-medium">Workflow #{r.workflow_id}</td>
                      <td>
                        <StatusBadge status={r.status} />
                      </td>
                      <td className="text-xs font-mono text-slate-400">{r.triggered_by ?? "MANUAL"}</td>
                      <td>
                        <span className="rounded bg-slate-950 px-2 py-0.5 text-xs font-mono">
                          {r.task_executions?.length ?? 0} tasks
                        </span>
                      </td>
                      <td className="text-xs text-slate-500">
                        {new Date(r.created_at).toLocaleTimeString()}
                      </td>
                      <td className="text-right">
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={() => setSelectedRun(r)}
                            className="rounded-lg bg-slate-800 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-700"
                          >
                            Details
                          </button>
                          {can(user, "workflows:manage") &&
                            ["PENDING", "RUNNING"].includes(r.status) && (
                              <button
                                onClick={() => handleCancelRun(r.id)}
                                className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-2.5 py-1 text-xs text-rose-300 hover:bg-rose-500/20"
                              >
                                Cancel
                              </button>
                            )}
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* DAG Inspect Modal */}
      <Modal
        title={`DAG: ${selectedWf?.name ?? ""}`}
        isOpen={selectedWf !== null}
        onClose={() => setSelectedWf(null)}
      >
        {selectedWf && (
          <div className="space-y-4 text-sm">
            <p className="text-xs text-slate-400">{selectedWf.description ?? "No description"}</p>
            <div>
              <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
                Tasks ({selectedWf.tasks.length})
              </h4>
              <div className="space-y-2">
                {selectedWf.tasks.map((t) => (
                  <div
                    key={t.id}
                    className="flex items-center justify-between rounded-xl border border-slate-800 bg-slate-950 px-3 py-2 text-xs"
                  >
                    <span className="font-semibold text-white">{t.name}</span>
                    <span className="font-mono text-slate-400">{t.task_type}</span>
                  </div>
                ))}
              </div>
            </div>

            <div>
              <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
                Dependency Edges ({selectedWf.edges.length})
              </h4>
              <div className="space-y-1.5">
                {selectedWf.edges.map((e) => (
                  <div
                    key={e.id}
                    className="flex items-center gap-2 rounded-xl bg-slate-950 px-3 py-1.5 text-xs font-mono text-slate-300"
                  >
                    <span>Task #{e.upstream_task_id}</span>
                    <CornerDownRight size={14} className="text-cyan-400" />
                    <span>Task #{e.downstream_task_id}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </Modal>

      {/* Run Inspect Modal */}
      <Modal
        title={`Run #${selectedRun?.id ?? ""} Details`}
        isOpen={selectedRun !== null}
        onClose={() => setSelectedRun(null)}
      >
        {selectedRun && (
          <div className="space-y-4 text-sm">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-xs text-slate-500">Status</span>
                <div className="mt-1">
                  <StatusBadge status={selectedRun.status} />
                </div>
              </div>
              <div className="text-right">
                <span className="text-xs text-slate-500">Triggered By</span>
                <p className="font-mono text-xs text-slate-300">{selectedRun.triggered_by}</p>
              </div>
            </div>

            <div>
              <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
                Task Executions
              </h4>
              <div className="space-y-2 max-h-60 overflow-y-auto">
                {selectedRun.task_executions.map((te) => (
                  <div
                    key={te.id}
                    className="flex items-center justify-between rounded-xl border border-slate-800 bg-slate-950 p-2.5 text-xs"
                  >
                    <div>
                      <span className="font-semibold text-white">
                        {te.task_name ?? `Task #${te.workflow_task_id}`}
                      </span>
                      {te.execution_id && (
                        <span className="ml-2 font-mono text-slate-500">
                          (Exec #{te.execution_id})
                        </span>
                      )}
                    </div>
                    <StatusBadge status={te.status} />
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </Modal>

      {/* Create Workflow Modal */}
      <Modal
        title="Create New Workflow"
        isOpen={createModal}
        onClose={() => setCreateModal(false)}
      >
        <form onSubmit={handleCreateWorkflow} className="space-y-4">
          <label className="block text-sm font-medium text-slate-300">
            Workflow Name
            <input
              required
              value={wfName}
              onChange={(e) => setWfName(e.target.value)}
              placeholder="data-etl-pipeline"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <label className="block text-sm font-medium text-slate-300">
            Description
            <input
              value={wfDesc}
              onChange={(e) => setWfDesc(e.target.value)}
              placeholder="ETL pipeline for daily analytics"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <label className="block text-sm font-medium text-slate-300">
            Sequential Tasks (Comma-separated)
            <input
              required
              value={wfTasks}
              onChange={(e) => setWfTasks(e.target.value)}
              placeholder="fetch, transform, validate, load"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500 font-mono text-xs"
            />
          </label>
          <p className="text-xs text-slate-500">
            Tasks will be wired in sequential order (DAG edges created automatically).
          </p>
          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={() => setCreateModal(false)}
              className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={busyCreate}
              className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
            >
              {busyCreate ? "Creating…" : "Create Workflow"}
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 4. Workers View
// ---------------------------------------------------------------------------
function WorkersView() {
  const [data, setData] = useState<Worker[]>([]);
  const [loading, setLoading] = useState(true);

  const loadWorkers = () => {
    workers()
      .then(setData)
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadWorkers();
    const id = setInterval(loadWorkers, 5000);
    return () => clearInterval(id);
  }, []);

  const activeCount = data.filter((w) => w.is_alive).length;

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Workers</h1>
          <p className="mt-1 text-sm text-slate-400">
            Distributed worker cluster registry, heartbeat liveness, and status.
          </p>
        </div>
        <button
          onClick={loadWorkers}
          className="flex items-center gap-2 self-start rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
        >
          <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          Refresh
        </button>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        <Stat
          label="Active Liveness"
          value={activeCount}
          icon={<Boxes size={20} />}
          hint="Heartbeat received in last 60 seconds"
        />
        <Stat
          label="Total Registered"
          value={data.length}
          icon={<Sliders size={20} />}
          hint="Total worker nodes registered in cluster"
        />
      </div>

      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Worker ID / Hostname</th>
                <th>Status</th>
                <th>Liveness</th>
                <th>Last Heartbeat</th>
                <th>Registered At</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={5} className="py-12 text-center text-slate-500">
                    <RefreshCw size={20} className="mx-auto mb-2 animate-spin opacity-40" />
                    Loading worker cluster…
                  </td>
                </tr>
              ) : data.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-12 text-center text-slate-500">
                    No workers currently registered in cluster.
                  </td>
                </tr>
              ) : (
                data.map((w) => (
                  <tr
                    key={w.worker_id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3.5 font-mono text-cyan-400 font-medium">{w.worker_id}</td>
                    <td>
                      <StatusBadge status={w.status} />
                    </td>
                    <td>
                      {w.is_alive ? (
                        <span className="inline-flex items-center gap-1.5 text-xs text-emerald-400 font-semibold">
                          <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
                          Alive
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 text-xs text-slate-500 font-medium">
                          <span className="h-2 w-2 rounded-full bg-slate-600" />
                          Stale / Dead
                        </span>
                      )}
                    </td>
                    <td className="text-xs font-mono text-slate-300">
                      {new Date(w.last_heartbeat_at).toLocaleTimeString()}
                    </td>
                    <td className="text-xs text-slate-500">
                      {new Date(w.created_at).toLocaleString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 5. Policies View
// ---------------------------------------------------------------------------
function Policies({ user }: { user: User }) {
  const [c, setC] = useState<Policy[]>([]);
  const [r, setR] = useState<Policy[]>([]);
  const [loading, setLoading] = useState(true);
  const [modal, setModal] = useState<"concurrency" | "rate" | null>(null);

  // Form states
  const [targetType, setTargetType] = useState("CATEGORY");
  const [targetId, setTargetId] = useState("");
  const [concurrencyVal, setConcurrencyVal] = useState(5);
  const [requestsVal, setRequestsVal] = useState(10);
  const [windowVal, setWindowVal] = useState(60);

  const loadPolicies = () => {
    concurrencyPolicies()
      .then(setC)
      .catch(() => setC([]));
    ratePolicies()
      .then(setR)
      .catch(() => setR([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadPolicies();
  }, []);

  async function handleToggleConcurrency(p: Policy) {
    if (!can(user, "policies:manage")) return;
    try {
      await setConcurrencyPolicyEnabled(p.id, !p.is_enabled);
      await loadPolicies();
    } catch (e: unknown) {
      alert("Error toggling policy: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handleDeleteConcurrency(id: number) {
    if (!can(user, "policies:manage")) return;
    if (!confirm("Delete this concurrency policy?")) return;
    try {
      await deleteConcurrencyPolicy(id);
      await loadPolicies();
    } catch (e: unknown) {
      alert("Error deleting policy: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handleToggleRate(p: Policy) {
    if (!can(user, "policies:manage")) return;
    try {
      await setRatePolicyEnabled(p.id, !p.is_enabled);
      await loadPolicies();
    } catch (e: unknown) {
      alert("Error toggling rate limit: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handleDeleteRate(id: number) {
    if (!can(user, "policies:manage")) return;
    if (!confirm("Delete this rate limit policy?")) return;
    try {
      await deleteRatePolicy(id);
      await loadPolicies();
    } catch (e: unknown) {
      alert("Error deleting policy: " + (e instanceof Error ? e.message : String(e)));
    }
  }

  async function handleCreatePolicy(e: React.FormEvent) {
    e.preventDefault();
    try {
      if (modal === "concurrency") {
        await createConcurrencyPolicy({
          target_type: targetType,
          target_id: targetId,
          max_concurrency: concurrencyVal,
        });
      } else if (modal === "rate") {
        await createRatePolicy({
          target_type: targetType,
          target_id: targetId,
          max_requests: requestsVal,
          window_seconds: windowVal,
        });
      }
      setModal(null);
      setTargetId("");
      await loadPolicies();
    } catch (err: unknown) {
      alert("Failed to create policy: " + (err instanceof Error ? err.message : String(err)));
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Policies</h1>
          <p className="mt-1 text-sm text-slate-400">
            Durable concurrency limits and sliding-window rate limit admission controls.
          </p>
        </div>
        <div className="flex items-center gap-3">
          {can(user, "policies:manage") && (
            <>
              <button
                onClick={() => {
                  setTargetType("TASK_TYPE");
                  setModal("concurrency");
                }}
                className="rounded-xl bg-cyan-500 px-3.5 py-2 text-xs font-semibold text-slate-950 hover:bg-cyan-400"
              >
                + Concurrency Limit
              </button>
              <button
                onClick={() => {
                  setTargetType("CATEGORY");
                  setModal("rate");
                }}
                className="rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-3.5 py-2 text-xs font-semibold text-cyan-300 hover:bg-cyan-500/20"
              >
                + Rate Limit
              </button>
            </>
          )}
          <button
            onClick={loadPolicies}
            className="rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          </button>
        </div>
      </div>

      {/* Concurrency Policy Table */}
      <Card>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">Concurrency Limits</h2>
          <span className="text-xs text-slate-500">Atomic PostgreSQL admission gates</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Target Type</th>
                <th>Target ID</th>
                <th>Max Concurrency</th>
                <th>Status</th>
                <th className="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {c.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-8 text-center text-slate-500">
                    No concurrency policies configured.
                  </td>
                </tr>
              ) : (
                c.map((p) => (
                  <tr
                    key={p.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3 font-semibold text-slate-200">{p.target_type}</td>
                    <td className="font-mono text-cyan-400">{p.target_id}</td>
                    <td className="font-mono font-bold text-white">{p.max_concurrency}</td>
                    <td>
                      <span
                        className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
                          p.is_enabled
                            ? "bg-emerald-500/15 text-emerald-300"
                            : "bg-slate-800 text-slate-400"
                        }`}
                      >
                        {p.is_enabled ? "Enabled" : "Disabled"}
                      </span>
                    </td>
                    <td className="text-right">
                      {can(user, "policies:manage") && (
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={() => handleToggleConcurrency(p)}
                            className="rounded-lg bg-slate-800 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-700"
                          >
                            {p.is_enabled ? "Disable" : "Enable"}
                          </button>
                          <button
                            onClick={() => handleDeleteConcurrency(p.id)}
                            className="rounded-lg p-1 text-slate-400 hover:text-rose-400"
                          >
                            <Trash2 size={14} />
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Rate Limit Policy Table */}
      <Card>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">Rate Limits</h2>
          <span className="text-xs text-slate-500">Rolling window capacity controls</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Target Type</th>
                <th>Target ID</th>
                <th>Limit / Window</th>
                <th>Status</th>
                <th className="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {r.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-8 text-center text-slate-500">
                    No rate limit policies configured.
                  </td>
                </tr>
              ) : (
                r.map((p) => (
                  <tr
                    key={p.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3 font-semibold text-slate-200">{p.target_type}</td>
                    <td className="font-mono text-cyan-400">{p.target_id}</td>
                    <td className="font-mono font-bold text-white">
                      {p.max_requests} req / {p.window_seconds}s
                    </td>
                    <td>
                      <span
                        className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
                          p.is_enabled
                            ? "bg-emerald-500/15 text-emerald-300"
                            : "bg-slate-800 text-slate-400"
                        }`}
                      >
                        {p.is_enabled ? "Enabled" : "Disabled"}
                      </span>
                    </td>
                    <td className="text-right">
                      {can(user, "policies:manage") && (
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={() => handleToggleRate(p)}
                            className="rounded-lg bg-slate-800 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-700"
                          >
                            {p.is_enabled ? "Disable" : "Enable"}
                          </button>
                          <button
                            onClick={() => handleDeleteRate(p.id)}
                            className="rounded-lg p-1 text-slate-400 hover:text-rose-400"
                          >
                            <Trash2 size={14} />
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Policy Form Modal */}
      <Modal
        title={modal === "concurrency" ? "Add Concurrency Limit" : "Add Rate Limit"}
        isOpen={modal !== null}
        onClose={() => setModal(null)}
      >
        <form onSubmit={handleCreatePolicy} className="space-y-4">
          <label className="block text-sm font-medium text-slate-300">
            Target Type
            <select
              value={targetType}
              onChange={(e) => setTargetType(e.target.value)}
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            >
              {modal === "concurrency" ? (
                <>
                  <option value="TASK_TYPE">TASK_TYPE</option>
                  <option value="WORKFLOW">WORKFLOW</option>
                  <option value="CATEGORY">CATEGORY</option>
                </>
              ) : (
                <>
                  <option value="CATEGORY">CATEGORY</option>
                  <option value="TASK_TYPE">TASK_TYPE</option>
                </>
              )}
            </select>
          </label>

          <label className="block text-sm font-medium text-slate-300">
            Target Identifier
            <input
              required
              value={targetId}
              onChange={(e) => setTargetId(e.target.value)}
              placeholder={targetType === "TASK_TYPE" ? "CPU_BOUND" : "payments"}
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500 font-mono text-xs"
            />
          </label>

          {modal === "concurrency" ? (
            <label className="block text-sm font-medium text-slate-300">
              Max Concurrency
              <input
                required
                type="number"
                min={1}
                value={concurrencyVal}
                onChange={(e) => setConcurrencyVal(parseInt(e.target.value) || 1)}
                className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
              />
            </label>
          ) : (
            <div className="grid grid-cols-2 gap-3">
              <label className="block text-sm font-medium text-slate-300">
                Max Requests
                <input
                  required
                  type="number"
                  min={1}
                  value={requestsVal}
                  onChange={(e) => setRequestsVal(parseInt(e.target.value) || 1)}
                  className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
                />
              </label>
              <label className="block text-sm font-medium text-slate-300">
                Window (Seconds)
                <input
                  required
                  type="number"
                  min={1}
                  value={windowVal}
                  onChange={(e) => setWindowVal(parseInt(e.target.value) || 1)}
                  className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
                />
              </label>
            </div>
          )}

          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={() => setModal(null)}
              className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
            >
              Save Policy
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 6. System View
// ---------------------------------------------------------------------------
function System({ user }: { user: User }) {
  const [h, setH] = useState<Record<string, unknown> | null>(null);
  const [summaryData, setSummaryData] = useState<Record<string, unknown> | null>(null);
  const [actionBusy, setActionBusy] = useState(false);
  const [message, setMessage] = useState("");

  const refresh = () => {
    health()
      .then(setH)
      .catch(() => setH({ status: "unavailable" }));
    summary()
      .then(setSummaryData)
      .catch(() => {});
  };

  useEffect(() => {
    refresh();
  }, []);

  async function handleReconcile() {
    setActionBusy(true);
    setMessage("");
    try {
      const res = await reconcileQueue();
      setMessage(`Queue Reconciled: ${JSON.stringify(res)}`);
      refresh();
    } catch (e: unknown) {
      setMessage("Error: " + (e instanceof Error ? e.message : String(e)));
    } finally {
      setActionBusy(false);
    }
  }

  async function handleSweep() {
    setActionBusy(true);
    setMessage("");
    try {
      const res = await systemSweep();
      setMessage(`Sweep Complete: ${JSON.stringify(res)}`);
      refresh();
    } catch (e: unknown) {
      setMessage("Error: " + (e instanceof Error ? e.message : String(e)));
    } finally {
      setActionBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">System Diagnostics</h1>
        <p className="mt-1 text-sm text-slate-400">
          Control plane health probes, queue maintenance, and metrics exposition.
        </p>
      </div>

      {message && (
        <div className="rounded-xl border border-cyan-500/30 bg-cyan-500/10 p-4 text-xs font-mono text-cyan-300">
          {message}
        </div>
      )}

      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <h2 className="mb-4 text-lg font-semibold text-white">Readiness Probes</h2>
          <div className="space-y-3">
            <div className="flex items-center justify-between rounded-xl bg-slate-950 p-3.5 text-sm">
              <span className="text-slate-400">Database Connection</span>
              <span className="font-semibold text-emerald-400">
                {String(h?.database ?? "checking…")}
              </span>
            </div>
            <div className="flex items-center justify-between rounded-xl bg-slate-950 p-3.5 text-sm">
              <span className="text-slate-400">Redis Connection</span>
              <span className="font-semibold text-emerald-400">
                {String(h?.redis ?? "checking…")}
              </span>
            </div>
            <div className="flex items-center justify-between rounded-xl bg-slate-950 p-3.5 text-sm">
              <span className="text-slate-400">Overall Probe Status</span>
              <span className="font-semibold text-cyan-400">
                {String(h?.status ?? "checking…")}
              </span>
            </div>
          </div>
        </Card>

        <Card>
          <h2 className="mb-4 text-lg font-semibold text-white">Maintenance Operations</h2>
          <p className="mb-4 text-xs text-slate-400">
            Durable PostgreSQL queue recovery and reconciliation with Redis.
          </p>
          <div className="space-y-3">
            {can(user, "system:reconcile") ? (
              <>
                <button
                  disabled={actionBusy}
                  onClick={handleReconcile}
                  className="flex w-full items-center justify-center gap-2 rounded-xl bg-cyan-500 px-4 py-2.5 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
                >
                  <RefreshCw size={16} className={actionBusy ? "animate-spin" : ""} />
                  Reconcile Execution Queue
                </button>
                <button
                  disabled={actionBusy}
                  onClick={handleSweep}
                  className="flex w-full items-center justify-center gap-2 rounded-xl border border-slate-700 bg-slate-950 px-4 py-2.5 text-sm font-semibold text-slate-200 hover:border-slate-600 disabled:opacity-50"
                >
                  <Sliders size={16} />
                  Execute Stale Task Sweep
                </button>
              </>
            ) : (
              <div className="rounded-xl border border-slate-800 bg-slate-950 p-4 text-xs text-slate-500">
                Maintenance operations require the <code className="text-slate-400">admin</code> role.
              </div>
            )}
          </div>
        </Card>
      </div>

      <Card>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">Telemetry & Operational Summary</h2>
          <a
            href="http://localhost:8000/metrics"
            target="_blank"
            rel="noreferrer"
            className="text-xs font-semibold text-cyan-400 hover:underline"
          >
            Open /metrics Exposition ↗
          </a>
        </div>
        <pre className="overflow-auto rounded-xl bg-slate-950 p-4 text-xs font-mono text-slate-300">
          {JSON.stringify(summaryData, null, 2)}
        </pre>
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 7. Users View
// ---------------------------------------------------------------------------
function UsersPage({ user }: { user: User }) {
  const [allUsers, setAllUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(false);
  const [addModal, setAddModal] = useState(false);

  // Form states
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newEmail, setNewEmail] = useState("");
  const [newRole, setNewRole] = useState("operator");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (can(user, "users:manage")) {
      listUsers()
        .then(setAllUsers)
        .catch(() => setAllUsers([]));
    }
  }, [user]);

  const loadAll = () => {
    if (can(user, "users:manage")) {
      setLoading(true);
      listUsers()
        .then(setAllUsers)
        .catch(() => setAllUsers([]))
        .finally(() => setLoading(false));
    }
  };

  async function handleCreateUser(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    try {
      await createUser({
        username: newUsername,
        password: newPassword,
        email: newEmail || undefined,
        role: newRole,
      });
      setAddModal(false);
      setNewUsername("");
      setNewPassword("");
      setNewEmail("");
      loadAll();
    } catch (err: unknown) {
      alert("Failed to create user: " + (err instanceof Error ? err.message : String(err)));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Users & RBAC</h1>
          <p className="mt-1 text-sm text-slate-400">
            Control-plane operator identities and permission policies.
          </p>
        </div>
        {can(user, "users:manage") && (
          <button
            onClick={() => setAddModal(true)}
            className="flex items-center gap-2 self-start rounded-xl bg-cyan-500 px-3.5 py-2 text-xs font-semibold text-slate-950 hover:bg-cyan-400"
          >
            <Plus size={16} />
            Create User
          </button>
        )}
      </div>

      <Card>
        <h2 className="mb-4 text-lg font-semibold text-white">Current Session Identity</h2>
        <div className="flex flex-wrap items-center justify-between gap-4 rounded-xl border border-slate-800 bg-slate-950 p-4">
          <div>
            <div className="flex items-center gap-2">
              <span className="font-bold text-white text-base">{user.username}</span>
              <RoleBadge role={user.role} />
            </div>
            <p className="mt-1 text-xs text-slate-400">{user.email ?? "No email configured"}</p>
          </div>
          <div className="text-right text-xs text-slate-500">
            ID: <span className="font-mono text-slate-300">#{user.id}</span>
          </div>
        </div>
      </Card>

      {can(user, "users:manage") && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-white">System Users Directory</h2>
            <button
              onClick={loadAll}
              className="text-xs text-slate-400 hover:text-white flex items-center gap-1"
            >
              <RefreshCw size={12} className={loading ? "animate-spin" : ""} />
              Refresh
            </button>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="pb-3">User</th>
                  <th>Role</th>
                  <th>Email</th>
                  <th>Status</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {allUsers.map((u) => (
                  <tr
                    key={u.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3 font-semibold text-white font-mono">{u.username}</td>
                    <td>
                      <RoleBadge role={u.role} />
                    </td>
                    <td className="text-slate-400 text-xs">{u.email ?? "—"}</td>
                    <td>
                      <span
                        className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
                          u.is_active
                            ? "bg-emerald-500/15 text-emerald-300"
                            : "bg-rose-500/15 text-rose-300"
                        }`}
                      >
                        {u.is_active ? "Active" : "Disabled"}
                      </span>
                    </td>
                    <td className="text-xs text-slate-500">
                      {new Date(u.created_at).toLocaleDateString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* Create User Modal */}
      <Modal title="Create New Operator / User" isOpen={addModal} onClose={() => setAddModal(false)}>
        <form onSubmit={handleCreateUser} className="space-y-4">
          <label className="block text-sm font-medium text-slate-300">
            Username
            <input
              required
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              placeholder="ops_engineer"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <label className="block text-sm font-medium text-slate-300">
            Password
            <input
              required
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              placeholder="••••••••"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <label className="block text-sm font-medium text-slate-300">
            Email (Optional)
            <input
              type="email"
              value={newEmail}
              onChange={(e) => setNewEmail(e.target.value)}
              placeholder="ops@flowforge.dev"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <label className="block text-sm font-medium text-slate-300">
            Role
            <select
              value={newRole}
              onChange={(e) => setNewRole(e.target.value)}
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            >
              <option value="operator">Operator (Trigger/Cancel executions, Manage Workflows)</option>
              <option value="observer">Observer (Read-only)</option>
              <option value="admin">Admin (Full Control, Policies, Reconcile, User Management)</option>
            </select>
          </label>
          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={() => setAddModal(false)}
              className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={busy}
              className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
            >
              {busy ? "Creating…" : "Create User"}
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Root App
// ---------------------------------------------------------------------------
export default function App() {
  const [user, setUser] = useState<User | null>(() => {
    try {
      return JSON.parse(localStorage.getItem("flowforge_user") || "null");
    } catch {
      return null;
    }
  });

  useEffect(() => {
    if (localStorage.getItem("flowforge_token")) {
      me()
        .then(setUser)
        .catch(() => {
          localStorage.clear();
          setUser(null);
        });
    }
  }, []);

  if (!user) {
    return <Login onLogin={setUser} />;
  }

  return (
    <BrowserRouter>
      <Layout
        user={user}
        onLogout={() => {
          localStorage.clear();
          setUser(null);
        }}
      />
    </BrowserRouter>
  );
}
