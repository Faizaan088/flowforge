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
  Moon,
  Sun,
  Calendar,
  FileText,
  Copy,
  Check,
  Info,
  ArrowRight,
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
  backendLogout,
  forgotPassword,
  resetPassword,
  verifyEmail,
  resendVerification,
  deleteAccount,
  listApiKeys,
  createApiKey,
  revokeApiKey,
  me,
  ratePolicies,
  reconcileQueue,
  setConcurrencyPolicyEnabled,
  setRatePolicyEnabled,
  summary,
  systemSweep,
  triggerWorkflowRun,
  updateExecutionPriority,
  updateUser,
  workers,
  workflowRuns,
  workflows,
  listSchedules,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  listAuditEvents,
  disableWorker,
  enableWorker,
  type Execution,
  type Policy,
  type User,
  type Worker,
  type WorkflowDefinition,
  type WorkflowRun,
  type Schedule,
  type AuditEvent,
  type ApiKey,
} from "./api";
import { connectEvents, type LiveEvent } from "./ws";
import { trackPageView } from "./analytics";

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
  isOpen = true,
  onClose,
  children,
}: {
  title: string;
  isOpen?: boolean;
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
// Toast Notification System
// ---------------------------------------------------------------------------
type Toast = {
  id: string;
  type: "success" | "error" | "info";
  message: string;
};

function ToastContainer({
  toasts,
  onClose,
}: {
  toasts: Toast[];
  onClose: (id: string) => void;
}) {
  if (toasts.length === 0) return null;
  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col gap-2 max-w-sm w-full pointer-events-none">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`pointer-events-auto flex items-start gap-3 rounded-2xl p-4 shadow-2xl border backdrop-blur-md transition-all ${
            t.type === "success"
              ? "bg-slate-900/95 border-emerald-500/40 text-emerald-200"
              : t.type === "error"
              ? "bg-slate-900/95 border-rose-500/40 text-rose-200"
              : "bg-slate-900/95 border-cyan-500/40 text-cyan-200"
          }`}
        >
          {t.type === "success" && <CheckCircle2 size={18} className="text-emerald-400 mt-0.5 shrink-0" />}
          {t.type === "error" && <AlertCircle size={18} className="text-rose-400 mt-0.5 shrink-0" />}
          {t.type === "info" && <Info size={18} className="text-cyan-400 mt-0.5 shrink-0" />}
          <div className="flex-1 text-xs leading-relaxed font-medium">{t.message}</div>
          <button onClick={() => onClose(t.id)} className="text-slate-400 hover:text-white shrink-0">
            <X size={14} />
          </button>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Onboarding Modal for First-time Users
// ---------------------------------------------------------------------------
function OnboardingModal({ onClose }: { onClose: () => void }) {
  const [step, setStep] = useState(0);

  const steps = [
    {
      title: "Welcome to FlowForge",
      desc: "FlowForge is a resilient distributed workflow orchestration and task scheduling control plane. Let's take a quick 1-minute tour of your new cluster.",
      icon: Network,
    },
    {
      title: "Live Execution Telemetry",
      desc: "Real-time WebSockets stream state changes directly to your dashboard. Monitor job statuses, queue reconciliations, and throughput with sub-second latency.",
      icon: Activity,
    },
    {
      title: "Workflows & DAG Execution",
      desc: "Construct complex task dependencies with cycles validation, concurrency boundaries, and automatic exponential-backoff retry policies.",
      icon: LayoutDashboard,
    },
    {
      title: "API Keys & Production Security",
      desc: "Generate cryptographically secure API keys to connect external worker fleets, automated CI/CD runners, and headless dispatch scripts.",
      icon: Key,
    },
  ];

  const current = steps[step];
  const Icon = current.icon;

  const handleNext = () => {
    if (step < steps.length - 1) {
      setStep(step + 1);
    } else {
      localStorage.setItem("flowforge_onboarded", "true");
      onClose();
    }
  };

  const handleSkip = () => {
    localStorage.setItem("flowforge_onboarded", "true");
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
      <div className="w-full max-w-lg rounded-3xl border border-slate-800 bg-slate-900 p-8 shadow-2xl">
        <div className="flex items-center justify-between pb-6 border-b border-slate-800/80">
          <div className="flex items-center gap-3">
            <span className="flex h-10 w-10 items-center justify-center rounded-2xl bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
              <Icon size={22} />
            </span>
            <div>
              <h2 className="text-xl font-bold text-white">{current.title}</h2>
              <p className="text-xs text-slate-400">Step {step + 1} of {steps.length}</p>
            </div>
          </div>
          <button onClick={handleSkip} className="text-xs text-slate-500 hover:text-slate-300">
            Skip Tour
          </button>
        </div>

        <div className="py-6">
          <p className="text-sm leading-relaxed text-slate-300">{current.desc}</p>
        </div>

        <div className="flex items-center justify-between pt-4 border-t border-slate-800/80">
          <div className="flex gap-1.5">
            {steps.map((_, i) => (
              <span
                key={i}
                className={`h-2 rounded-full transition-all ${
                  i === step ? "w-6 bg-cyan-500" : "w-2 bg-slate-800"
                }`}
              />
            ))}
          </div>
          <div className="flex gap-2">
            {step > 0 && (
              <button
                onClick={() => setStep(step - 1)}
                className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
              >
                Back
              </button>
            )}
            <button
              onClick={handleNext}
              className="flex items-center gap-2 rounded-xl bg-cyan-500 px-5 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
            >
              <span>{step === steps.length - 1 ? "Get Started" : "Next"}</span>
              <CornerDownRight size={14} />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Forgot Password Modal
// ---------------------------------------------------------------------------
function ForgotPasswordModal({ onClose }: { onClose: () => void }) {
  const [identifier, setIdentifier] = useState("");
  const [busy, setBusy] = useState(false);
  const [sent, setSent] = useState(false);
  const [error, setError] = useState("");

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      await forgotPassword(identifier);
      setSent(true);
    } catch {
      setError("Unable to process request. Please try again later.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal title="Reset Password" onClose={onClose}>
      {sent ? (
        <div className="space-y-4 py-3 text-center">
          <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
            <CheckCircle2 size={24} />
          </div>
          <h3 className="text-base font-semibold text-white">Reset Link Dispatched</h3>
          <p className="text-xs text-slate-300 leading-relaxed">
            If an account matches <span className="font-mono text-cyan-400 font-semibold">{identifier}</span>, password reset instructions have been sent. Check your inbox or development console logs.
          </p>
          <button
            onClick={onClose}
            className="w-full rounded-xl bg-slate-800 py-2.5 text-sm font-semibold text-white hover:bg-slate-700"
          >
            Return to Login
          </button>
        </div>
      ) : (
        <form onSubmit={submit} className="space-y-4">
          <p className="text-xs text-slate-400">
            Enter your registered username or email address and we'll generate a secure password reset link.
          </p>
          {error && (
            <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-xs text-rose-300">
              {error}
            </div>
          )}
          <label className="block text-sm font-medium text-slate-300">
            Username or Email
            <input
              required
              autoFocus
              value={identifier}
              onChange={(e) => setIdentifier(e.target.value)}
              placeholder="e.g. admin or engineer@company.com"
              className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
            />
          </label>
          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={busy}
              className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
            >
              {busy ? "Sending..." : "Send Reset Link"}
            </button>
          </div>
        </form>
      )}
    </Modal>
  );
}

// ---------------------------------------------------------------------------
// Reset Password Page (/reset-password)
// ---------------------------------------------------------------------------
function ResetPasswordPage({ theme, onToggleTheme }: { theme: Theme; onToggleTheme: () => void }) {
  const token = new URLSearchParams(window.location.search).get("token") || "";
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (newPassword.length < 12) {
      setError("Password must be at least 12 characters.");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      await resetPassword(token, newPassword);
      setSuccess(true);
    } catch {
      setError("Invalid or expired password reset token. Please request a new link.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="relative flex min-h-screen items-center justify-center bg-slate-950 p-6 text-slate-100">
      <button
        type="button"
        onClick={onToggleTheme}
        className="theme-toggle absolute right-6 top-6"
        aria-label="Toggle theme"
      >
        {theme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
      </button>

      <div className="w-full max-w-md rounded-3xl border border-slate-800 bg-slate-900/90 p-8 shadow-2xl backdrop-blur-md">
        <div className="mb-6 flex items-center gap-2">
          <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
            <Key size={20} />
          </span>
          <div className="text-2xl font-black text-white">Flow<span className="text-cyan-400">Forge</span></div>
        </div>

        {success ? (
          <div className="space-y-4 py-4 text-center">
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
              <CheckCircle2 size={32} />
            </div>
            <h2 className="text-xl font-bold text-white">Password Updated!</h2>
            <p className="text-sm text-slate-300">
              Your password has been changed successfully. You can now log into your account.
            </p>
            <a
              href="/"
              className="inline-block w-full rounded-xl bg-cyan-500 py-3 text-center text-sm font-semibold text-slate-950 hover:bg-cyan-400"
            >
              Sign In Now
            </a>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="space-y-4">
            <h2 className="text-xl font-bold text-white">Set New Password</h2>
            <p className="text-xs text-slate-400">Choose a strong password with at least 12 characters.</p>

            {error && (
              <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-xs text-rose-300">
                {error}
              </div>
            )}

            {!token && (
              <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-300">
                Missing reset token in URL. Please use the link provided in your email.
              </div>
            )}

            <label className="block text-sm font-medium text-slate-300">
              New Password
              <input
                required
                type="password"
                minLength={12}
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                placeholder="At least 12 characters"
                className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
              />
            </label>

            <label className="block text-sm font-medium text-slate-300">
              Confirm New Password
              <input
                required
                type="password"
                minLength={12}
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                placeholder="Repeat password"
                className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
              />
            </label>

            <button
              disabled={busy || !token}
              type="submit"
              className="w-full rounded-xl bg-cyan-500 py-3 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
            >
              {busy ? "Updating Password..." : "Update Password"}
            </button>

            <div className="pt-2 text-center">
              <a href="/" className="text-xs text-slate-400 hover:text-cyan-400">
                Back to Sign In
              </a>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Verify Email Page (/verify-email)
// ---------------------------------------------------------------------------
function VerifyEmailPage({ theme, onToggleTheme }: { theme: Theme; onToggleTheme: () => void }) {
  const token = new URLSearchParams(window.location.search).get("token") || "";
  const [loading, setLoading] = useState(() => Boolean(token));
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState(() => (!token ? "No verification token found in URL." : ""));

  useEffect(() => {
    if (!token) return;
    verifyEmail(token)
      .then(() => setSuccess(true))
      .catch((err) => setError(err.response?.data?.detail || "Invalid or expired verification token."))
      .finally(() => setLoading(false));
  }, [token]);

  return (
    <div className="relative flex min-h-screen items-center justify-center bg-slate-950 p-6 text-slate-100">
      <button
        type="button"
        onClick={onToggleTheme}
        className="theme-toggle absolute right-6 top-6"
        aria-label="Toggle theme"
      >
        {theme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
      </button>

      <div className="w-full max-w-md rounded-3xl border border-slate-800 bg-slate-900/90 p-8 text-center shadow-2xl backdrop-blur-md">
        <div className="mb-6 flex items-center justify-center gap-2">
          <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
            <CheckCircle2 size={20} />
          </span>
          <div className="text-2xl font-black text-white">Flow<span className="text-cyan-400">Forge</span></div>
        </div>

        {loading ? (
          <div className="py-8 space-y-3">
            <RefreshCw size={28} className="mx-auto animate-spin text-cyan-400" />
            <p className="text-sm text-slate-300">Verifying your email address…</p>
          </div>
        ) : success ? (
          <div className="space-y-4 py-4">
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
              <CheckCircle2 size={32} />
            </div>
            <h2 className="text-xl font-bold text-white">Email Verified!</h2>
            <p className="text-sm text-slate-300">
              Your email address has been verified. Your FlowForge account features are now fully activated.
            </p>
            <a
              href="/"
              className="inline-block w-full rounded-xl bg-cyan-500 py-3 text-center text-sm font-semibold text-slate-950 hover:bg-cyan-400"
            >
              Continue to Dashboard
            </a>
          </div>
        ) : (
          <div className="space-y-4 py-4">
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-rose-500/20 text-rose-400 border border-rose-500/30">
              <XCircle size={32} />
            </div>
            <h2 className="text-xl font-bold text-white">Verification Failed</h2>
            <p className="text-sm text-rose-300">{error}</p>
            <a
              href="/"
              className="inline-block w-full rounded-xl bg-slate-800 py-3 text-center text-sm font-semibold text-slate-200 hover:bg-slate-700"
            >
              Back to Sign In
            </a>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Login View
// ---------------------------------------------------------------------------
type Theme = "light" | "dark";

function Login({
  onLogin,
  theme,
  onToggleTheme,
}: {
  onLogin: (u: User) => void;
  theme: Theme;
  onToggleTheme: () => void;
}) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [showForgot, setShowForgot] = useState(false);

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
    <div className="relative flex min-h-screen items-center justify-center bg-slate-950 p-6">
      <button
        type="button"
        onClick={onToggleTheme}
        className="theme-toggle absolute right-6 top-6"
        aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} theme`}
        title="Change appearance"
      >
        {theme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
        <span className="text-sm">
          {theme === "dark" ? "Light" : "Dark"}
        </span>
      </button>

      <form
        onSubmit={submit}
        className="w-full max-w-md rounded-3xl border border-slate-800 bg-slate-900/90 p-8 shadow-2xl backdrop-blur-md"
      >
        <div className="mb-8">
          <a href="/" className="inline-flex items-center gap-2 hover:opacity-90 transition-opacity">
            <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
              <Network size={20} />
            </span>
            <div className="text-3xl font-black tracking-tight text-white">
              Flow<span className="text-cyan-400">Forge</span>
            </div>
          </a>
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
            className="mt-1.5 mb-2 w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none transition-colors focus:border-cyan-500"
          />
        </label>

        <div className="mb-6 flex items-center justify-between">
          <span className="text-xs text-slate-500">Default: admin / admin</span>
          <button
            type="button"
            onClick={() => setShowForgot(true)}
            className="text-xs font-semibold text-cyan-400 hover:underline"
          >
            Forgot password?
          </button>
        </div>

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

        <div className="mt-6 flex items-center justify-between text-xs text-slate-500 pt-4 border-t border-slate-800/80">
          <a href="/terms" className="hover:text-cyan-400 transition-colors">Terms of Service</a>
          <span>•</span>
          <a href="/privacy" className="hover:text-cyan-400 transition-colors">Privacy Policy</a>
          <span>•</span>
          <span className="text-slate-600">v0.1.0-prod</span>
        </div>
        <p className="mt-3 text-center text-xs text-slate-600">
          © {new Date().getFullYear()} FlowForge. Distributed workflow orchestration.
        </p>
      </form>

      {showForgot && <ForgotPasswordModal onClose={() => setShowForgot(false)} />}
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
  ["/schedules", "Schedules", Calendar],
  ["/workers", "Workers", Boxes],
  ["/policies", "Policies", ShieldCheck],
  ["/api-keys", "API Keys", Key],
  ["/audit", "Audit Log", FileText],
  ["/system", "System", Gauge],
  ["/users", "Users", Users],
  ["/settings", "Settings", Sliders],
] as const;

function Layout({
  user,
  onLogout,
  theme,
  onToggleTheme,
}: {
  user: User;
  onLogout: () => void;
  theme: Theme;
  onToggleTheme: () => void;
}) {
  const [mobile, setMobile] = useState(false);
  const location = useLocation();
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [showOnboarding, setShowOnboarding] = useState(() => !localStorage.getItem("flowforge_onboarded"));

  const addToast = (type: "success" | "error" | "info", message: string) => {
    const id = Math.random().toString(36).substring(2, 9);
    setToasts((prev) => [...prev, { id, type, message }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 4500);
  };

  const removeToast = (id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  useEffect(() => {
    const currentItem = NAV_ITEMS.find((n) => n[0] === location.pathname);
    const title = currentItem ? currentItem[1] : location.pathname === "/terms" ? "Terms of Service" : location.pathname === "/privacy" ? "Privacy Policy" : "FlowForge";
    document.title = `${title} — FlowForge`;
    trackPageView(location.pathname);
  }, [location.pathname]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 overflow-x-hidden">
      {/* Sidebar */}
      <aside
        className={`fixed inset-y-0 left-0 z-30 w-64 border-r border-slate-800 bg-slate-950 p-4 transition-transform lg:translate-x-0 ${
          mobile ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between px-3 py-3">
          <NavLink to="/" className="flex items-center gap-2 hover:opacity-90 transition-opacity">
            <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-cyan-500/20 text-cyan-400 border border-cyan-500/30">
              <Network size={18} />
            </span>
            <div className="text-2xl font-black text-white">
              Flow<span className="text-cyan-400">Forge</span>
            </div>
          </NavLink>
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
            <button
              type="button"
              onClick={onToggleTheme}
              className="theme-toggle"
              aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} theme`}
              title="Change appearance"
            >
              {theme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
              <span className="text-sm">
                {theme === "dark" ? "Light" : "Dark"}
              </span>
            </button>
          </div>
        </header>

        <main className="p-6 md:p-8">
          {user.email && !user.is_verified && (
            <div className="mb-6 rounded-2xl border border-amber-500/30 bg-amber-500/10 p-4 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 text-xs text-amber-200">
              <div className="flex items-center gap-2.5">
                <AlertTriangle size={18} className="text-amber-400 shrink-0" />
                <span>Your email address ({user.email}) is not yet verified. Verify your email to secure your account.</span>
              </div>
              <button
                type="button"
                onClick={async () => {
                  try {
                    await resendVerification(user.email || undefined);
                    addToast("success", "Verification email dispatched! Check your inbox.");
                  } catch {
                    addToast("error", "Failed to send verification email.");
                  }
                }}
                className="font-semibold underline text-cyan-300 hover:text-white shrink-0"
              >
                Resend Verification Link
              </button>
            </div>
          )}

          <Routes>
            <Route path="/" element={<Dashboard user={user} />} />
            <Route path="/executions" element={<Executions user={user} />} />
            <Route path="/workflows" element={<Workflows user={user} />} />
            <Route path="/schedules" element={<SchedulesView user={user} />} />
            <Route path="/workers" element={<WorkersView user={user} />} />
            <Route path="/policies" element={<Policies user={user} />} />
            <Route path="/api-keys" element={<ApiKeysView user={user} onNotify={addToast} />} />
            <Route path="/audit" element={<AuditView user={user} />} />
            <Route path="/system" element={<System user={user} />} />
            <Route path="/users" element={<UsersPage user={user} />} />
            <Route path="/settings" element={<SettingsView user={user} onLogout={onLogout} onNotify={addToast} />} />
            <Route path="/terms" element={<TermsPage />} />
            <Route path="/privacy" element={<PrivacyPage />} />
            <Route path="*" element={<NotFoundPage />} />
          </Routes>

          <footer className="mt-16 border-t border-slate-900 pt-6 pb-4 flex flex-col sm:flex-row items-center justify-between text-xs text-slate-500 gap-4">
            <div>© {new Date().getFullYear()} FlowForge Orchestration Engine. All rights reserved.</div>
            <div className="flex items-center gap-4">
              <NavLink to="/terms" className="hover:text-cyan-400 transition-colors">Terms of Service</NavLink>
              <span>•</span>
              <NavLink to="/privacy" className="hover:text-cyan-400 transition-colors">Privacy Policy</NavLink>
              <span>•</span>
              <span className="text-slate-600">v0.1.0-prod</span>
            </div>
          </footer>
        </main>
      </div>

      <ToastContainer toasts={toasts} onClose={removeToast} />
      {showOnboarding && <OnboardingModal onClose={() => setShowOnboarding(false)} />}
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
    const disconnect = connectEvents(
      (e) => {
        setEvents((prev) => [e, ...prev].slice(0, 10));
      },
      refreshSummary,
    );
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
function WorkersView({ user }: { user: User }) {
  const [data, setData] = useState<Worker[]>([]);
  const [loading, setLoading] = useState(true);
  const [disablingId, setDisablingId] = useState<string | null>(null);
  const [enablingId, setEnablingId] = useState<string | null>(null);

  const loadWorkers = () => {
    workers()
      .then(setData)
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  };

  const handleDisable = async (workerId: string) => {
    if (!confirm(`Are you sure you want to disable worker ${workerId}?`)) return;
    setDisablingId(workerId);
    try {
      await disableWorker(workerId);
      loadWorkers();
    } catch {
      alert("Failed to disable worker.");
    } finally {
      setDisablingId(null);
    }
  };

  const handleEnable = async (workerId: string) => {
    setEnablingId(workerId);
    try {
      await enableWorker(workerId);
      loadWorkers();
    } catch {
      alert("Failed to enable worker.");
    } finally {
      setEnablingId(null);
    }
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
                {can(user, "policies:manage") && <th className="text-right">Actions</th>}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
                    <RefreshCw size={20} className="mx-auto mb-2 animate-spin opacity-40" />
                    Loading worker cluster…
                  </td>
                </tr>
              ) : data.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
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
                    {can(user, "policies:manage") && (
                      <td className="text-right">
                        {w.status === "ACTIVE" ? (
                          <button
                            onClick={() => handleDisable(w.worker_id)}
                            disabled={disablingId === w.worker_id}
                            className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-2.5 py-1 text-xs font-semibold text-rose-300 hover:bg-rose-500/20 disabled:opacity-50"
                          >
                            {disablingId === w.worker_id ? "Disabling…" : "Disable"}
                          </button>
                        ) : (
                          <button
                            onClick={() => handleEnable(w.worker_id)}
                            disabled={enablingId === w.worker_id}
                            className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-1 text-xs font-semibold text-emerald-300 hover:bg-emerald-500/20 disabled:opacity-50"
                          >
                            {enablingId === w.worker_id ? "Enabling…" : "Enable"}
                          </button>
                        )}
                      </td>
                    )}
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
  const [editingUser, setEditingUser] = useState<User | null>(null);
const [editRole, setEditRole] = useState("observer");
const [editActive, setEditActive] = useState(true);
const [editPassword, setEditPassword] = useState("");

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
  async function handleUpdateUser(e: React.FormEvent) {
  e.preventDefault();
  if (!editingUser) return;

  setBusy(true);
  try {
    await updateUser(editingUser.id, {
      role: editRole,
      is_active: editActive,
      ...(editPassword ? { password: editPassword } : {}),
    });

    setEditingUser(null);
    setEditPassword("");
    loadAll();
  } catch (err: unknown) {
    const detail =
      typeof err === "object" &&
      err !== null &&
      "response" in err
        ? (
            err as {
              response?: { data?: { detail?: string } };
            }
          ).response?.data?.detail
        : undefined;

    alert(
      "Failed to update user: " +
        (detail || (err instanceof Error ? err.message : String(err))),
    );
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
                  <th>Actions</th>
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
                    <td className="py-3">
  <button
    type="button"
    onClick={() => {
      setEditingUser(u);
      setEditRole(u.role);
      setEditActive(u.is_active);
      setEditPassword("");
    }}
    className="rounded-lg border border-slate-700 px-3 py-1.5 text-xs font-medium text-slate-200 hover:border-cyan-500 hover:text-cyan-300"
  >
    Edit
  </button>
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
      
{/* Edit User Modal */}
<Modal
  title={`Edit User${editingUser ? `: ${editingUser.username}` : ""}`}
  isOpen={editingUser !== null}
  onClose={() => {
    setEditingUser(null);
    setEditPassword("");
  }}
>
  <form onSubmit={handleUpdateUser} className="space-y-4">
    <label className="block text-sm font-medium text-slate-300">
      Role
      <select
        value={editRole}
        onChange={(e) => setEditRole(e.target.value)}
        className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
      >
        <option value="operator">Operator</option>
        <option value="observer">Observer (Read-only)</option>
        <option value="admin">Admin</option>
      </select>
    </label>

    <label className="flex items-center gap-3 text-sm text-slate-300">
      <input
        type="checkbox"
        checked={editActive}
        onChange={(e) => setEditActive(e.target.checked)}
        className="h-4 w-4 accent-cyan-500"
      />
      Account is active
    </label>

    <label className="block text-sm font-medium text-slate-300">
      New Password (Optional)
      <input
        type="password"
        minLength={4}
        value={editPassword}
        onChange={(e) => setEditPassword(e.target.value)}
        placeholder="Leave blank to keep current password"
        className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
      />
    </label>

    <div className="flex justify-end gap-2 pt-2">
      <button
        type="button"
        onClick={() => {
          setEditingUser(null);
          setEditPassword("");
        }}
        className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
      >
        Cancel
      </button>
      <button
        type="submit"
        disabled={busy}
        className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
      >
        {busy ? "Saving..." : "Save Changes"}
      </button>
    </div>
  </form>
</Modal>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 8. Schedules View
// ---------------------------------------------------------------------------
function SchedulesView({ user }: { user: User }) {
  const [items, setItems] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [name, setName] = useState("");
  const [scheduleType, setScheduleType] = useState("recurring");
  const [intervalSec, setIntervalSec] = useState("60");
  const [cronExpr, setCronExpr] = useState("*/5 * * * *");
  const [busy, setBusy] = useState(false);

  const loadData = () => {
    listSchedules()
      .then(setItems)
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadData();
  }, []);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      const config: Record<string, unknown> = {};
      if (scheduleType === "recurring") {
        if (cronExpr.trim()) {
          config.cron = cronExpr.trim();
        } else {
          config.interval_seconds = parseFloat(intervalSec);
        }
      } else if (scheduleType === "delayed") {
        config.delay_seconds = parseFloat(intervalSec);
      } else if (scheduleType === "one_time") {
        config.run_at = new Date(Date.now() + 60000).toISOString();
      }

      await createSchedule({
        name,
        schedule_type: scheduleType,
        enabled: true,
        configuration: config,
      });
      setModalOpen(false);
      setName("");
      loadData();
    } catch {
      alert("Failed to create schedule.");
    } finally {
      setBusy(false);
    }
  };

  const handleToggle = async (s: Schedule) => {
    try {
      await updateSchedule(s.id, { enabled: !s.enabled });
      loadData();
    } catch {
      alert("Failed to update schedule status.");
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm("Are you sure you want to delete this schedule?")) return;
    try {
      await deleteSchedule(id);
      loadData();
    } catch {
      alert("Failed to delete schedule.");
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Schedules</h1>
          <p className="mt-1 text-sm text-slate-400">
            Durable recurring cron, interval, delayed, and one-time job dispatch rules.
          </p>
        </div>
        <div className="flex gap-2">
          {can(user, "executions:trigger") && (
            <button
              onClick={() => setModalOpen(true)}
              className="flex items-center gap-2 rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
            >
              <Plus size={16} />
              New Schedule
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

      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Name</th>
                <th>Type</th>
                <th>Configuration</th>
                <th>Status</th>
                <th>Updated</th>
                <th className="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
                    <RefreshCw size={20} className="mx-auto mb-2 animate-spin opacity-40" />
                    Loading schedules…
                  </td>
                </tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
                    No active schedule definitions found.
                  </td>
                </tr>
              ) : (
                items.map((s) => (
                  <tr
                    key={s.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3.5 font-semibold text-slate-100">{s.name}</td>
                    <td className="capitalize font-mono text-xs text-cyan-400">{s.schedule_type}</td>
                    <td className="font-mono text-xs text-slate-400 max-w-xs truncate">
                      {s.configuration ? JSON.stringify(s.configuration) : "Default"}
                    </td>
                    <td>
                      <span
                        className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                          s.enabled
                            ? "bg-emerald-500/15 text-emerald-300 border border-emerald-500/30"
                            : "bg-slate-800 text-slate-400 border border-slate-700"
                        }`}
                      >
                        {s.enabled ? "Enabled" : "Disabled"}
                      </span>
                    </td>
                    <td className="text-xs text-slate-500">
                      {new Date(s.updated_at).toLocaleString()}
                    </td>
                    <td className="text-right space-x-2">
                      {can(user, "executions:trigger") && (
                        <>
                          <button
                            onClick={() => handleToggle(s)}
                            className="rounded-lg border border-slate-700 bg-slate-800 px-2.5 py-1 text-xs font-medium text-slate-300 hover:bg-slate-700"
                          >
                            {s.enabled ? "Disable" : "Enable"}
                          </button>
                          <button
                            onClick={() => handleDelete(s.id)}
                            className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-2.5 py-1 text-xs font-medium text-rose-300 hover:bg-rose-500/20"
                          >
                            Delete
                          </button>
                        </>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <Modal title="Create New Schedule" isOpen={modalOpen} onClose={() => setModalOpen(false)}>
        <form onSubmit={handleCreate} className="space-y-4">
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
              Schedule Name
            </label>
            <input
              type="text"
              required
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. hourly-cleanup"
              className="mt-1 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
            />
          </div>
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
              Schedule Type
            </label>
            <select
              value={scheduleType}
              onChange={(e) => setScheduleType(e.target.value)}
              className="mt-1 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
            >
              <option value="recurring">Recurring (Cron or Interval)</option>
              <option value="delayed">Delayed</option>
              <option value="one_time">One-Time</option>
            </select>
          </div>
          {scheduleType === "recurring" && (
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
                Cron Expression (5-field)
              </label>
              <input
                type="text"
                value={cronExpr}
                onChange={(e) => setCronExpr(e.target.value)}
                placeholder="*/5 * * * *"
                className="mt-1 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm font-mono text-slate-100 outline-none focus:border-cyan-500"
              />
              <p className="mt-1 text-xs text-slate-500">Example: */5 * * * * for every 5 minutes</p>
            </div>
          )}
          {scheduleType === "delayed" && (
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
                Delay Seconds
              </label>
              <input
                type="number"
                min="1"
                value={intervalSec}
                onChange={(e) => setIntervalSec(e.target.value)}
                className="mt-1 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
              />
            </div>
          )}
          <div className="flex justify-end gap-2 pt-4">
            <button
              type="button"
              onClick={() => setModalOpen(false)}
              className="rounded-xl px-4 py-2 text-sm font-semibold text-slate-400 hover:bg-slate-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={busy}
              className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
            >
              {busy ? "Creating…" : "Save Schedule"}
            </button>
          </div>
        </form>
      </Modal>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 9. Audit View
// ---------------------------------------------------------------------------
function AuditView(_props: { user?: User }) {
  void _props;
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionFilter, setActionFilter] = useState("");

  useEffect(() => {
    let active = true;
    listAuditEvents({ action: actionFilter || undefined })
      .then((data) => {
        if (active) setEvents(data);
      })
      .catch(() => {
        if (active) setEvents([]);
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [actionFilter]);

  const loadData = () => {
    setLoading(true);
    listAuditEvents({ action: actionFilter || undefined })
      .then(setEvents)
      .catch(() => setEvents([]))
      .finally(() => setLoading(false));
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">Audit Log</h1>
          <p className="mt-1 text-sm text-slate-400">
            Immutable system audit trail tracking administrative and operational lifecycle events.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <input
            type="text"
            placeholder="Filter by action..."
            value={actionFilter}
            onChange={(e) => setActionFilter(e.target.value)}
            className="rounded-xl border border-slate-800 bg-slate-900 px-3 py-1.5 text-xs text-slate-200 outline-none focus:border-cyan-500"
          />
          <button
            onClick={loadData}
            className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>
      </div>

      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Timestamp</th>
                <th>Actor</th>
                <th>Action</th>
                <th>Target Type</th>
                <th>Target ID</th>
                <th>Metadata</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
                    <RefreshCw size={20} className="mx-auto mb-2 animate-spin opacity-40" />
                    Loading audit events…
                  </td>
                </tr>
              ) : events.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
                    No audit records recorded yet.
                  </td>
                </tr>
              ) : (
                events.map((ev) => (
                  <tr
                    key={ev.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3 text-xs text-slate-400 font-mono">
                      {new Date(ev.created_at).toLocaleString()}
                    </td>
                    <td className="font-mono text-xs text-cyan-400">User #{ev.actor_id}</td>
                    <td className="font-semibold text-slate-200">{ev.action}</td>
                    <td className="text-xs text-slate-400">{ev.target_type}</td>
                    <td className="font-mono text-xs text-slate-300">{ev.target_id || "—"}</td>
                    <td className="font-mono text-xs text-slate-500 max-w-xs truncate">
                      {ev.metadata_ ? JSON.stringify(ev.metadata_) : "—"}
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
// ---------------------------------------------------------------------------
// 10. API Keys View
// ---------------------------------------------------------------------------
function ApiKeysView({
  user: _user,
  onNotify,
}: {
  user: User;
  onNotify: (type: "success" | "error" | "info", msg: string) => void;
}) {
  void _user;
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [keyName, setKeyName] = useState("");
  const [expiryDays, setExpiryDays] = useState("30");
  const [busy, setBusy] = useState(false);
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const loadKeys = () => {
    listApiKeys()
      .then(setKeys)
      .catch(() => setKeys([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadKeys();
  }, []);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      const exp = expiryDays === "never" ? undefined : parseInt(expiryDays, 10);
      const res = await createApiKey(keyName, exp);
      setCreatedKey(res.raw_key);
      setKeyName("");
      loadKeys();
      onNotify("success", "API key created successfully.");
    } catch {
      onNotify("error", "Failed to generate API key.");
    } finally {
      setBusy(false);
    }
  };

  const handleRevoke = async (id: number) => {
    if (!confirm("Are you sure you want to revoke this API key? This action is permanent and cannot be undone.")) return;
    try {
      await revokeApiKey(id);
      loadKeys();
      onNotify("success", "API key revoked successfully.");
    } catch {
      onNotify("error", "Failed to revoke API key.");
    }
  };

  const copyKey = () => {
    if (createdKey) {
      navigator.clipboard.writeText(createdKey);
      setCopied(true);
      setTimeout(() => setCopied(false), 3000);
      onNotify("info", "API key copied to clipboard.");
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-white">API Keys</h1>
          <p className="mt-1 text-sm text-slate-400">
            Generate and manage programmatic authentication keys with X-API-Key header access.
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => {
              setCreatedKey(null);
              setModalOpen(true);
            }}
            className="flex items-center gap-2 rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
          >
            <Plus size={16} />
            Generate New Key
          </button>
          <button
            onClick={loadKeys}
            className="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-300 hover:bg-slate-800"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>
      </div>

      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              <tr>
                <th className="pb-3">Name</th>
                <th>Prefix</th>
                <th>Status</th>
                <th>Created</th>
                <th>Expires</th>
                <th>Last Used</th>
                <th className="text-right">Action</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={7} className="py-12 text-center text-slate-500">
                    <RefreshCw size={20} className="mx-auto mb-2 animate-spin opacity-40" />
                    Loading API keys…
                  </td>
                </tr>
              ) : keys.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-12 text-center">
                    <Key size={32} className="mx-auto mb-3 text-slate-600" />
                    <p className="text-slate-400 font-medium">No API keys generated yet.</p>
                    <p className="text-xs text-slate-500 mt-1">Generate a key to connect CI/CD pipelines or headless scripts.</p>
                    <button
                      onClick={() => setModalOpen(true)}
                      className="mt-4 inline-flex items-center gap-2 rounded-xl bg-cyan-500/10 border border-cyan-500/20 px-3.5 py-1.5 text-xs font-semibold text-cyan-300 hover:bg-cyan-500/20"
                    >
                      <Plus size={14} />
                      Generate First API Key
                    </button>
                  </td>
                </tr>
              ) : (
                keys.map((k) => (
                  <tr
                    key={k.id}
                    className="border-t border-slate-800/80 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="py-3.5 font-semibold text-slate-100">{k.name}</td>
                    <td className="font-mono text-xs text-cyan-400">{k.key_prefix}••••••••</td>
                    <td>
                      <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
                        Active
                      </span>
                    </td>
                    <td className="text-xs text-slate-400">
                      {new Date(k.created_at).toLocaleDateString()}
                    </td>
                    <td className="text-xs text-slate-400">
                      {k.expires_at ? new Date(k.expires_at).toLocaleDateString() : "Never"}
                    </td>
                    <td className="text-xs text-slate-500">
                      {k.last_used_at ? new Date(k.last_used_at).toLocaleString() : "Never used"}
                    </td>
                    <td className="text-right">
                      <button
                        onClick={() => handleRevoke(k.id)}
                        className="rounded-lg p-1.5 text-slate-500 hover:bg-rose-500/10 hover:text-rose-400"
                        title="Revoke key"
                      >
                        <Trash2 size={16} />
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {modalOpen && (
        <Modal
          title={createdKey ? "API Key Created" : "Generate API Key"}
          onClose={() => {
            setModalOpen(false);
            setCreatedKey(null);
          }}
        >
          {createdKey ? (
            <div className="space-y-4">
              <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3.5 text-xs text-amber-300 flex items-start gap-2.5">
                <AlertTriangle size={18} className="shrink-0 mt-0.5" />
                <p>
                  Copy this API key now. For your security, this is the only time it will ever be displayed.
                </p>
              </div>

              <div className="rounded-xl border border-slate-700 bg-slate-950 p-3.5 flex items-center justify-between gap-2">
                <code className="text-xs font-mono text-cyan-300 truncate select-all">{createdKey}</code>
                <button
                  type="button"
                  onClick={copyKey}
                  className="flex items-center gap-1.5 rounded-lg bg-cyan-500/20 border border-cyan-500/40 px-3 py-1.5 text-xs font-semibold text-cyan-300 hover:bg-cyan-500/30 shrink-0"
                >
                  {copied ? <Check size={14} /> : <Copy size={14} />}
                  <span>{copied ? "Copied!" : "Copy"}</span>
                </button>
              </div>

              <p className="text-xs text-slate-400">
                To authenticate requests, pass this key in the <code className="text-slate-200">X-API-Key: {createdKey.slice(0, 12)}...</code> header.
              </p>

              <div className="flex justify-end pt-2">
                <button
                  type="button"
                  onClick={() => {
                    setModalOpen(false);
                    setCreatedKey(null);
                  }}
                  className="rounded-xl bg-cyan-500 px-5 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
                >
                  Done
                </button>
              </div>
            </div>
          ) : (
            <form onSubmit={handleCreate} className="space-y-4">
              <label className="block text-sm font-medium text-slate-300">
                Key Description / Name
                <input
                  required
                  autoFocus
                  value={keyName}
                  onChange={(e) => setKeyName(e.target.value)}
                  placeholder="e.g. GitHub Actions CI/CD"
                  className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
                />
              </label>

              <label className="block text-sm font-medium text-slate-300">
                Expiration
                <select
                  value={expiryDays}
                  onChange={(e) => setExpiryDays(e.target.value)}
                  className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-cyan-500"
                >
                  <option value="7">7 Days</option>
                  <option value="30">30 Days</option>
                  <option value="90">90 Days</option>
                  <option value="365">1 Year</option>
                  <option value="never">No Expiration (Never)</option>
                </select>
              </label>

              <div className="flex justify-end gap-2 pt-2">
                <button
                  type="button"
                  onClick={() => setModalOpen(false)}
                  className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={busy}
                  className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-400 disabled:opacity-50"
                >
                  {busy ? "Generating..." : "Generate Key"}
                </button>
              </div>
            </form>
          )}
        </Modal>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 11. Settings View
// ---------------------------------------------------------------------------
function SettingsView({
  user,
  onLogout,
  onNotify,
}: {
  user: User;
  onLogout: () => void;
  onNotify: (type: "success" | "error" | "info", msg: string) => void;
}) {
  const [deleteModalOpen, setDeleteModalOpen] = useState(false);
  const [confirmUsername, setConfirmUsername] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [deleting, setDeleting] = useState(false);
  const [resending, setResending] = useState(false);

  const handleResendVerification = async () => {
    setResending(true);
    try {
      await resendVerification(user.email || undefined);
      onNotify("success", "Verification email dispatched! Please check your inbox.");
    } catch {
      onNotify("error", "Failed to resend verification email.");
    } finally {
      setResending(false);
    }
  };

  const handleDeleteAccount = async (e: React.FormEvent) => {
    e.preventDefault();
    if (confirmUsername !== user.username) {
      alert("Confirmation username does not match.");
      return;
    }
    setDeleting(true);
    try {
      await deleteAccount(confirmUsername, confirmPassword);
      onNotify("info", "Your account has been deleted.");
      onLogout();
    } catch (err: unknown) {
      const msg =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        "Failed to delete account.";
      alert(msg);
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div className="space-y-8 max-w-4xl">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Account Settings</h1>
        <p className="mt-1 text-sm text-slate-400">
          Manage your personal profile, email verification, and account security.
        </p>
      </div>

      {/* Profile Overview */}
      <Card className="space-y-6">
        <h2 className="text-lg font-bold text-white flex items-center gap-2">
          <Users size={18} className="text-cyan-400" />
          User Profile
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm">
          <div>
            <span className="text-xs font-medium text-slate-400">Username</span>
            <p className="text-base font-semibold text-white mt-1">{user.username}</p>
          </div>
          <div>
            <span className="text-xs font-medium text-slate-400">Role & Permissions</span>
            <div className="mt-1">
              <RoleBadge role={user.role} />
            </div>
          </div>
          <div>
            <span className="text-xs font-medium text-slate-400">Registered Email</span>
            <p className="text-base font-medium text-slate-200 mt-1">{user.email || "No email registered"}</p>
          </div>
          <div>
            <span className="text-xs font-medium text-slate-400">Verification Status</span>
            <div className="mt-1 flex items-center gap-3">
              {user.is_verified ? (
                <span className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-semibold bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
                  <CheckCircle2 size={13} />
                  Verified
                </span>
              ) : (
                <>
                  <span className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-semibold bg-amber-500/15 text-amber-300 border border-amber-500/30">
                    <AlertTriangle size={13} />
                    Unverified
                  </span>
                  <button
                    type="button"
                    disabled={resending}
                    onClick={handleResendVerification}
                    className="text-xs font-semibold text-cyan-400 hover:underline disabled:opacity-50"
                  >
                    {resending ? "Sending..." : "Resend Link"}
                  </button>
                </>
              )}
            </div>
          </div>
        </div>
      </Card>

      {/* Danger Zone */}
      <div className="rounded-2xl border border-rose-500/30 bg-rose-950/20 p-6 backdrop-blur-sm">
        <h2 className="text-lg font-bold text-rose-300 flex items-center gap-2">
          <AlertCircle size={18} className="text-rose-400" />
          Danger Zone
        </h2>
        <p className="text-xs text-rose-200/80 mt-1">
          Deleting your account permanently removes your credentials, active API keys, and personal access tokens.
        </p>

        <div className="mt-5 pt-4 border-t border-rose-500/20 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <span className="text-sm font-semibold text-white">Delete User Account</span>
            <p className="text-xs text-slate-400">Once deleted, your account cannot be recovered.</p>
          </div>
          <button
            type="button"
            onClick={() => setDeleteModalOpen(true)}
            className="rounded-xl border border-rose-500/40 bg-rose-500/15 px-4 py-2 text-xs font-semibold text-rose-300 hover:bg-rose-500/25 shrink-0"
          >
            Delete Account
          </button>
        </div>
      </div>

      {deleteModalOpen && (
        <Modal title="Confirm Account Deletion" onClose={() => setDeleteModalOpen(false)}>
          <form onSubmit={handleDeleteAccount} className="space-y-4">
            <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3.5 text-xs text-rose-300">
              This action is permanent and irreversible. Please confirm your identity.
            </div>

            <label className="block text-sm font-medium text-slate-300">
              Type your username (<span className="text-cyan-400 font-semibold">{user.username}</span>) to confirm
              <input
                required
                autoFocus
                value={confirmUsername}
                onChange={(e) => setConfirmUsername(e.target.value)}
                placeholder={user.username}
                className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-rose-500"
              />
            </label>

            <label className="block text-sm font-medium text-slate-300">
              Your Current Password
              <input
                required
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                placeholder="••••••••••••"
                className="mt-1.5 w-full rounded-xl border border-slate-700 bg-slate-950 px-3.5 py-2.5 text-slate-100 outline-none focus:border-rose-500"
              />
            </label>

            <div className="flex justify-end gap-2 pt-2">
              <button
                type="button"
                onClick={() => setDeleteModalOpen(false)}
                className="rounded-xl px-4 py-2 text-sm text-slate-400 hover:bg-slate-800"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={deleting || confirmUsername !== user.username}
                className="rounded-xl bg-rose-500 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-600 disabled:opacity-50"
              >
                {deleting ? "Deleting..." : "Permanently Delete Account"}
              </button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 12. Public & Error Pages (404, Terms, Privacy)
// ---------------------------------------------------------------------------
function NotFoundPage() {
  return (
    <div className="flex min-h-[60vh] flex-col items-center justify-center text-center p-6">
      <div className="flex h-20 w-20 items-center justify-center rounded-3xl bg-slate-900 border border-slate-800 text-cyan-400 shadow-xl">
        <AlertTriangle size={36} />
      </div>
      <h1 className="text-4xl font-black text-white mt-6">404</h1>
      <p className="text-lg font-semibold text-slate-200 mt-1">Page Not Found</p>
      <p className="text-sm text-slate-400 max-w-sm mt-2">
        The route you requested does not exist on this cluster or was relocated.
      </p>
      <NavLink
        to="/"
        className="mt-6 inline-flex items-center gap-2 rounded-xl bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 hover:bg-cyan-400"
      >
        <LayoutDashboard size={16} />
        Return to Dashboard
      </NavLink>
    </div>
  );
}

function TermsPage() {
  return (
    <div className="max-w-4xl mx-auto py-8 px-4 text-slate-300 space-y-6">
      <div className="border-b border-slate-800 pb-4">
        <h1 className="text-3xl font-bold text-white">Terms of Service</h1>
        <p className="text-xs text-slate-400 mt-1">Last updated: October 2026</p>
      </div>
      <div className="space-y-4 text-sm leading-relaxed">
        <h2 className="text-lg font-semibold text-white">1. Acceptance of Terms</h2>
        <p>By accessing or utilizing the FlowForge workflow engine, you agree to comply with all operational policies and rate limits.</p>
        <h2 className="text-lg font-semibold text-white">2. Acceptable Use</h2>
        <p>You agree not to dispatch malicious tasks, circumvent concurrency policies, or attempt unauthorized principal escalation.</p>
        <h2 className="text-lg font-semibold text-white">3. API Key Security</h2>
        <p>You are solely responsible for guarding generated API keys and secrets against unauthorized disclosure.</p>
      </div>
      <div className="pt-4">
        <a href="/" className="inline-flex items-center gap-1.5 text-xs font-semibold text-cyan-400 hover:underline">
          <ArrowRight size={14} className="rotate-180" />
          Back to FlowForge
        </a>
      </div>
    </div>
  );
}

function PrivacyPage() {
  return (
    <div className="max-w-4xl mx-auto py-8 px-4 text-slate-300 space-y-6">
      <div className="border-b border-slate-800 pb-4">
        <h1 className="text-3xl font-bold text-white">Privacy Policy</h1>
        <p className="text-xs text-slate-400 mt-1">Last updated: October 2026</p>
      </div>
      <div className="space-y-4 text-sm leading-relaxed">
        <h2 className="text-lg font-semibold text-white">1. Information Collection</h2>
        <p>FlowForge logs operator actor identities, IP hashes, and timestamps for immutable audit compliance.</p>
        <h2 className="text-lg font-semibold text-white">2. Data Storage & Encryption</h2>
        <p>All passwords are encrypted with PBKDF2-HMAC-SHA256 with cryptographically random salts. Tokens are securely hashed.</p>
        <h2 className="text-lg font-semibold text-white">3. Data Retention & Deletion</h2>
        <p>You may permanently purge your user profile and active API keys at any time via Account Settings.</p>
      </div>
      <div className="pt-4">
        <a href="/" className="inline-flex items-center gap-1.5 text-xs font-semibold text-cyan-400 hover:underline">
          <ArrowRight size={14} className="rotate-180" />
          Back to FlowForge
        </a>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Root App & Content
// ---------------------------------------------------------------------------
function AppContent({
  theme,
  onToggleTheme,
}: {
  theme: Theme;
  onToggleTheme: () => void;
}) {
  const [user, setUser] = useState<User | null>(() => {
    try {
      return JSON.parse(localStorage.getItem("flowforge_user") || "null");
    } catch {
      return null;
    }
  });

  const location = useLocation();

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

  const handleLogout = async () => {
    await backendLogout();
    setUser(null);
  };

  // Public unauthenticated routes
  if (location.pathname === "/reset-password") {
    return <ResetPasswordPage theme={theme} onToggleTheme={onToggleTheme} />;
  }
  if (location.pathname === "/verify-email") {
    return <VerifyEmailPage theme={theme} onToggleTheme={onToggleTheme} />;
  }
  if (location.pathname === "/terms") {
    return (
      <div className="min-h-screen bg-slate-950 p-6">
        <TermsPage />
      </div>
    );
  }
  if (location.pathname === "/privacy") {
    return (
      <div className="min-h-screen bg-slate-950 p-6">
        <PrivacyPage />
      </div>
    );
  }

  if (!user) {
    return (
      <Login
        onLogin={setUser}
        theme={theme}
        onToggleTheme={onToggleTheme}
      />
    );
  }

  return (
    <Layout
      user={user}
      theme={theme}
      onToggleTheme={onToggleTheme}
      onLogout={handleLogout}
    />
  );
}

export default function App() {
  const [theme, setTheme] = useState<Theme>(() => {
    return localStorage.getItem("flowforge_theme") === "light"
      ? "light"
      : "dark";
  });

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("flowforge_theme", theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme((current) => (current === "dark" ? "light" : "dark"));
  };

  return (
    <BrowserRouter>
      <AppContent theme={theme} onToggleTheme={toggleTheme} />
    </BrowserRouter>
  );
}
