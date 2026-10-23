/**
 * FlowForge Telemetry & Analytics Dispatcher
 * Dispatches operational pageview and event metrics when configured.
 */

declare global {
  interface Window {
    plausible?: (event: string, options?: { props?: Record<string, unknown> }) => void;
    gtag?: (command: string, action: string, params?: Record<string, unknown>) => void;
  }
}

export const trackPageView = (path: string) => {
  if (typeof window === "undefined") return;

  if (window.plausible) {
    window.plausible("pageview", { props: { path } });
  } else if (window.gtag) {
    window.gtag("event", "page_view", { page_path: path });
  } else if (import.meta.env.DEV) {
    console.debug(`[FlowForge Analytics] Page view: ${path}`);
  }
};

export const trackEvent = (action: string, category: string, label?: string) => {
  if (typeof window === "undefined") return;

  if (window.plausible) {
    window.plausible(action, { props: { category, label } });
  } else if (window.gtag) {
    window.gtag("event", action, { event_category: category, event_label: label });
  } else if (import.meta.env.DEV) {
    console.debug(`[FlowForge Analytics] Event: ${action} (${category}) ${label || ""}`);
  }
};
