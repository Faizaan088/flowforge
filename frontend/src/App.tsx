import { useEffect, useState } from "react";
import { BrowserRouter, NavLink, Route, Routes, useLocation } from "react-router-dom";
import { Activity, Boxes, Gauge, LayoutDashboard, LogOut, Menu, Network, ShieldCheck, Users, X } from "lucide-react";
import { cancelExecution, concurrencyPolicies, executions, health, login, me, ratePolicies, summary, type Execution, type Policy, type User } from "./api";
import { connectEvents, type LiveEvent } from "./ws";

const nav = [
  ["/", "Dashboard", LayoutDashboard],
  ["/executions", "Executions", Activity],
  ["/workflows", "Workflows", Network],
  ["/workers", "Workers", Boxes],
  ["/policies", "Policies", ShieldCheck],
  ["/system", "System", Gauge],
  ["/users", "Users", Users],
] as const;

function Card({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <div className={`rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-xl shadow-black/10 ${className}`}>{children}</div>;
}

function Login({ onLogin }: { onLogin: (u: User) => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault(); setBusy(true); setError("");
    try { const result = await login(username, password); onLogin(result.user); }
    catch { setError("Invalid credentials or API unavailable."); }
    finally { setBusy(false); }
  }

  return <div className="flex min-h-screen items-center justify-center bg-slate-950 p-6">
    <form onSubmit={submit} className="w-full max-w-md rounded-3xl border border-slate-800 bg-slate-900 p-8 shadow-2xl">
      <div className="mb-8">
        <div className="mb-3 text-3xl font-black tracking-tight">Flow<span className="text-cyan-400">Forge</span></div>
        <p className="text-slate-400">Distributed workflow orchestration</p>
      </div>
      <label className="text-sm text-slate-300">Username
        <input required value={username} onChange={e => setUsername(e.target.value)} className="mt-2 mb-4 w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 outline-none focus:border-cyan-500" />
      </label>
      <label className="text-sm text-slate-300">Password
        <input required type="password" value={password} onChange={e => setPassword(e.target.value)} className="mt-2 mb-5 w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 outline-none focus:border-cyan-500" />
      </label>
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      <button disabled={busy} className="w-full rounded-xl bg-cyan-500 px-4 py-3 font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50">{busy ? "Signing in…" : "Sign in"}</button>
    </form>
  </div>;
}

function Layout({ user, onLogout }: { user: User; onLogout: () => void }) {
  const [mobile, setMobile] = useState(false);
  const location = useLocation();

  return <div className="min-h-screen bg-slate-950 text-slate-100">
    <aside className={`fixed inset-y-0 left-0 z-30 w-64 border-r border-slate-800 bg-slate-950 p-4 transition-transform lg:translate-x-0 ${mobile ? "translate-x-0" : "-translate-x-full"}`}>
      <div className="flex items-center justify-between px-3 py-3">
        <div className="text-2xl font-black">Flow<span className="text-cyan-400">Forge</span></div>
        <button className="lg:hidden" onClick={() => setMobile(false)}><X /></button>
      </div>
      <p className="px-3 pb-5 text-xs uppercase tracking-widest text-slate-500">Control plane</p>
      <nav className="space-y-1">
        {nav.map(([to, label, Icon]) => <NavLink key={to} to={to} onClick={() => setMobile(false)}
          className={({ isActive }) => `flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm ${isActive ? "bg-cyan-500/10 text-cyan-300" : "text-slate-400 hover:bg-slate-900 hover:text-slate-100"}`}>
          <Icon size={18} />{label}
        </NavLink>)}
      </nav>
      <div className="absolute bottom-4 left-4 right-4 rounded-2xl border border-slate-800 bg-slate-900 p-3">
        <div className="text-sm font-medium">{user.username}</div>
        <div className="text-xs text-slate-500">{user.role}</div>
        <button onClick={onLogout} className="mt-3 flex items-center gap-2 text-xs text-slate-400 hover:text-rose-300"><LogOut size={14} /> Sign out</button>
      </div>
    </aside>
    <div className="lg:pl-64">
      <header className="sticky top-0 z-20 flex h-16 items-center gap-4 border-b border-slate-800 bg-slate-950/90 px-5 backdrop-blur">
        <button className="lg:hidden" onClick={() => setMobile(true)}><Menu /></button>
        <div>
          <div className="text-sm font-medium">{nav.find(n => n[0] === location.pathname)?.[1] ?? "FlowForge"}</div>
          <div className="text-xs text-slate-500">Operational control plane</div>
        </div>
      </header>
      <main className="p-5 md:p-8">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/executions" element={<Executions />} />
          <Route path="/workflows" element={<Placeholder title="Workflows" text="The workflow engine is ready. This view will be wired to dedicated workflow REST endpoints when exposed by the backend." />} />
          <Route path="/workers" element={<Placeholder title="Workers" text="Worker registration and heartbeat are available. A dedicated worker listing endpoint is needed for this view." />} />
          <Route path="/policies" element={<Policies />} />
          <Route path="/system" element={<System />} />
          <Route path="/users" element={<UsersPage />} />
        </Routes>
      </main>
    </div>
  </div>;
}

function Dashboard() {
  const [data, setData] = useState<Record<string, unknown>>({});
  const [events, setEvents] = useState<LiveEvent[]>([]);
  useEffect(() => {
    summary().then(setData).catch(() => {});
    return connectEvents(e => setEvents(v => [e, ...v].slice(0, 8)));
  }, []);
  return <div className="space-y-6">
    <div><h1 className="text-3xl font-bold">System overview</h1><p className="mt-1 text-slate-400">Live operational state across FlowForge.</p></div>
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Stat label="Queued" value={data.queued ?? data.queued_executions ?? "—"} icon="⏳" />
      <Stat label="Running" value={data.running ?? data.running_executions ?? "—"} icon="▶" />
      <Stat label="Workers" value={data.active_workers ?? "—"} icon="⚙" />
      <Stat label="Dead lettered" value={data.dead_lettered ?? "—"} icon="⚠" />
    </div>
    <div className="grid gap-6 xl:grid-cols-3">
      <Card className="xl:col-span-2"><h2 className="mb-4 text-lg font-semibold">Live events</h2>
        {events.length === 0 ? <p className="text-sm text-slate-500">Waiting for execution events…</p> :
          <div className="space-y-2">{events.map((e, i) => <div key={`${e.event_id ?? i}`} className="flex items-center justify-between rounded-xl bg-slate-950 px-3 py-2 text-sm"><span>{e.event_type}</span><span className="text-slate-500">{e.entity_type ?? "system"} {e.entity_id ?? ""}</span></div>)}</div>}
      </Card>
      <Card><h2 className="mb-4 text-lg font-semibold">API health</h2><Health /></Card>
    </div>
  </div>;
}

function Stat({ label, value, icon }: { label: string; value: unknown; icon: string }) {
  return <Card><div className="flex items-center justify-between"><div><p className="text-sm text-slate-400">{label}</p><p className="mt-2 text-3xl font-semibold">{String(value)}</p></div><span className="text-2xl">{icon}</span></div></Card>;
}

function Executions() {
  const [rows, setRows] = useState<Execution[]>([]);
  const [loading, setLoading] = useState(true);
  const load = () => executions().then(setRows).catch(() => setRows([])).finally(() => setLoading(false));
  useEffect(() => { load(); const id = setInterval(load, 5000); return () => clearInterval(id); }, []);

  async function cancel(id: number) { await cancelExecution(id, "Cancelled from dashboard"); await load(); }

  return <div className="space-y-6">
    <div><h1 className="text-3xl font-bold">Executions</h1><p className="mt-1 text-slate-400">Inspect and control durable execution state.</p></div>
    <Card><div className="overflow-x-auto"><table className="w-full text-left text-sm">
      <thead className="text-slate-500"><tr><th className="pb-3">ID</th><th>Status</th><th>Job</th><th>Attempt</th><th>Priority</th><th>Worker</th><th /></tr></thead>
      <tbody>{loading ? <tr><td colSpan={7} className="py-8 text-center text-slate-500">Loading…</td></tr> :
        rows.map(x => <tr key={x.id} className="border-t border-slate-800">
          <td className="py-3 font-mono">#{x.id}</td><td><Status status={x.status} /></td><td>{x.job_definition_id}</td><td>{x.attempt}</td><td>{x.priority}</td><td className="text-slate-400">{x.worker_id ?? "—"}</td>
          <td>{["QUEUED", "CLAIMED", "RUNNING", "RETRY_WAIT"].includes(x.status) && <button onClick={() => cancel(x.id)} className="text-xs text-rose-300 hover:text-rose-200">Cancel</button>}</td>
        </tr>)}</tbody>
    </table></div></Card>
  </div>;
}

function Status({ status }: { status: string }) {
  const good = ["SUCCEEDED", "RUNNING", "READY", "ACTIVE"].includes(status);
  const bad = ["FAILED", "DEAD_LETTERED", "CANCELLED"].includes(status);
  return <span className={`rounded-full px-2.5 py-1 text-xs font-medium ${good ? "bg-emerald-500/15 text-emerald-300" : bad ? "bg-rose-500/15 text-rose-300" : "bg-amber-500/15 text-amber-300"}`}>{status}</span>;
}

function Policies() {
  const [c, setC] = useState<Policy[]>([]); const [r, setR] = useState<Policy[]>([]);
  useEffect(() => { concurrencyPolicies().then(setC).catch(() => {}); ratePolicies().then(setR).catch(() => {}); }, []);
  return <div className="space-y-6">
    <div><h1 className="text-3xl font-bold">Policies</h1><p className="mt-1 text-slate-400">Concurrency and rate-limit controls.</p></div>
    <Card><h2 className="mb-4 text-lg font-semibold">Concurrency</h2><PolicyTable rows={c} kind="concurrency" /></Card>
    <Card><h2 className="mb-4 text-lg font-semibold">Rate limits</h2><PolicyTable rows={r} kind="rate" /></Card>
  </div>;
}

function PolicyTable({ rows, kind }: { rows: Policy[]; kind: string }) {
  return <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="text-slate-500"><tr><th className="pb-3">Target</th><th>Type</th><th>Limit</th><th>Enabled</th></tr></thead>
    <tbody>{rows.map(p => <tr key={p.id} className="border-t border-slate-800"><td className="py-3 font-mono">{p.target_id}</td><td>{p.target_type}</td><td>{kind === "concurrency" ? p.max_concurrency : `${p.max_requests}/${p.window_seconds}s`}</td><td>{p.is_enabled ? "Yes" : "No"}</td></tr>)}</tbody>
  </table></div>;
}

function System() {
  const [h, setH] = useState<unknown>(null);
  useEffect(() => { health().then(setH).catch(() => setH({ status: "unavailable" })); }, []);
  return <div className="space-y-6"><h1 className="text-3xl font-bold">System</h1><Card><h2 className="text-lg font-semibold">Readiness</h2><pre className="mt-4 overflow-auto rounded-xl bg-slate-950 p-4 text-sm text-slate-300">{JSON.stringify(h, null, 2)}</pre></Card></div>;
}

function UsersPage() {
  const [u, setU] = useState<User | null>(null);
  useEffect(() => { me().then(setU).catch(() => {}); }, []);
  return <div className="space-y-6"><h1 className="text-3xl font-bold">Users</h1><Card>{u ? <><p className="font-semibold">{u.username}</p><p className="text-sm text-slate-400">{u.email ?? "No email"} · {u.role}</p></> : <p className="text-slate-500">Loading current user…</p>}</Card></div>;
}

function Health() {
  const [h, setH] = useState<Record<string, unknown> | null>(null);
  useEffect(() => { health().then(setH).catch(() => {}); }, []);
  const ready = h?.status === "ready" || h?.status === "healthy";
  return <div className="flex items-center gap-3"><span className={`h-3 w-3 rounded-full ${ready ? "bg-emerald-400" : "bg-amber-400"}`} /><span>{String(h?.status ?? "Checking…")}</span></div>;
}

function Placeholder({ title, text }: { title: string; text: string }) {
  return <Card><h1 className="text-2xl font-bold">{title}</h1><p className="mt-2 text-slate-400">{text}</p></Card>;
}

export default function App() {
  const [user, setUser] = useState<User | null>(() => {
    try { return JSON.parse(localStorage.getItem("flowforge_user") || "null"); } catch { return null; }
  });

  useEffect(() => {
    if (localStorage.getItem("flowforge_token")) me().then(setUser).catch(() => { localStorage.clear(); setUser(null); });
  }, []);

  if (!user) return <Login onLogin={setUser} />;
  return <BrowserRouter><Layout user={user} onLogout={() => { localStorage.clear(); setUser(null); }} /></BrowserRouter>;
}
