(function () {
  async function syncPendingNotifications(silent) {
    if (!window.NotifCenter) return;
    try {
      const response = await fetch("/api/notificaciones/pendientes", { cache: "no-store" });
      const data = await response.json();
      if (!response.ok || !data.ok) return;
      if (Array.isArray(data.items) && data.items.length) {
        window.NotifCenter.sync(data.items, { silent: Boolean(silent) });
      }
    } catch (error) {
      if (window.console) console.warn("No se pudieron sincronizar notificaciones pendientes", error);
    }
  }

  window.addEventListener("load", () => {
    syncPendingNotifications(true);
    window.setInterval(() => syncPendingNotifications(false), 30000);
  });
})();
