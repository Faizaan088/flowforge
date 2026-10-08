export type LiveEvent = {
  event_id?: number;
  event_type: string;
  entity_type?: string;
  entity_id?: number | string;
  timestamp?: string;
  payload?: Record<string, unknown>;
};

export function connectEvents(onEvent: (event: LiveEvent) => void) {
  const token = localStorage.getItem("flowforge_token");
  if (!token) return () => {};

  const base = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
  const wsBase = base.replace(/^http/, "ws");
  const ws = new WebSocket(`${wsBase}/ws/events?token=${encodeURIComponent(token)}`);

  ws.onopen = () => {
    ws.send(JSON.stringify({ action: "subscribe", channels: ["executions", "workflows"] }));
  };
  ws.onmessage = (message) => {
    try {
      const data = JSON.parse(message.data);
      if (data.event_type) onEvent(data);
    } catch { /* ignore malformed events */ }
  };

  return () => ws.close();
}
