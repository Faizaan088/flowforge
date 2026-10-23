export type LiveEvent = {
  event_id?: number;
  event_type: string;
  entity_type?: string;
  entity_id?: number | string;
  timestamp?: string;
  payload?: Record<string, unknown>;
};

export function connectEvents(
  onEvent: (event: LiveEvent) => void,
  onConnected?: () => void,
) {
  let stopped = false;
  let socket: WebSocket | null = null;
  let reconnectTimer: number | undefined;
  let heartbeatTimer: number | undefined;
  let reconnectDelay = 1000;

  const connect = () => {
    if (stopped) return;
    const token = localStorage.getItem("flowforge_token");
    if (!token) return;

    const base = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
    const wsUrl = base.startsWith("/")
      ? `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/ws/events`
      : `${base.replace(/^http/, "ws")}/ws/events`;

    // Use a WebSocket subprotocol rather than a query parameter so the JWT
    // does not appear in request URLs and ordinary access logs.
    socket = new WebSocket(wsUrl, ["flowforge", `token.${token}`]);
    socket.onopen = () => {
      reconnectDelay = 1000;
      // Reconcile dashboard state from the durable API after every connection,
      // including reconnects that may follow missed transient Pub/Sub events.
      onConnected?.();
      socket?.send(JSON.stringify({ action: "subscribe", channels: ["executions", "workflows"] }));
      heartbeatTimer = window.setInterval(() => {
        if (socket?.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ action: "ping" }));
        }
      }, 25000);
    };
    socket.onmessage = (message) => {
      try {
        const data = JSON.parse(message.data);
        if (data.event_type) onEvent(data);
      } catch { /* ignore malformed events */ }
    };
    socket.onclose = () => {
      if (heartbeatTimer !== undefined) window.clearInterval(heartbeatTimer);
      heartbeatTimer = undefined;
      if (!stopped && localStorage.getItem("flowforge_token")) {
        reconnectTimer = window.setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
      }
    };
    socket.onerror = () => socket?.close();
  };

  connect();
  return () => {
    stopped = true;
    if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer);
    if (heartbeatTimer !== undefined) window.clearInterval(heartbeatTimer);
    socket?.close();
  };
}
