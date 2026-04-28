(function () {
  function buildBadge(motivo) {
    const badge = document.createElement("span");
    badge.className = "mesa-atencion-badge";
    badge.dataset.mesaAlertBadge = "1";
    badge.title = motivo || "Mesa por atender";
    badge.innerHTML = '<span class="material-symbols-outlined" style="font-size:14px;">notifications_active</span>Por atender';
    return badge;
  }

  function pintarAvisos(avisos) {
    const activos = new Map((avisos || []).map((aviso) => [String(aviso.mesa_id), aviso]));
    document.querySelectorAll("[data-mesa-card]").forEach((card) => {
      const aviso = activos.get(String(card.dataset.mesaCard));
      let badge = card.querySelector("[data-mesa-alert-badge]");
      const badgeHost = card.querySelector(".mesa-card-open") || card;
      const trigger = card.querySelector("[data-mesa-alert-trigger]");
      card.classList.toggle("mesa-card--alerta", Boolean(aviso));
      if (aviso && !badge) {
        badgeHost.appendChild(buildBadge(aviso.motivo || ""));
      } else if (aviso && badge) {
        badge.title = aviso.motivo || "Mesa por atender";
      } else if (!aviso && badge) {
        badge.remove();
      }
      if (trigger) {
        const activo = Boolean(aviso);
        trigger.disabled = activo || trigger.dataset.saving === "1";
        trigger.classList.toggle("is-active", activo);
        const text = trigger.querySelector("[data-mesa-alert-trigger-text]");
        if (text) text.textContent = activo ? "Aviso activo" : "Marcar mesa";
      }
    });
  }

  async function cargarAvisosMesas() {
    try {
      const response = await fetch("/api/mesas/avisos", { cache: "no-store" });
      const data = await response.json();
      if (!response.ok || !data.ok) return;
      pintarAvisos(data.avisos || []);
    } catch (error) {
      if (window.console) console.warn("No se pudieron actualizar avisos de mesas", error);
    }
  }

  async function marcarMesaPorAtender(button) {
    const mesaId = button.dataset.mesaAlertTrigger;
    if (!mesaId || button.dataset.saving === "1") return;
    button.dataset.saving = "1";
    button.disabled = true;
    try {
      const response = await fetch(`/api/mesa/${mesaId}/pendiente-atencion`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          pendiente: true,
          motivo: "Mesa marcada para atencion",
        }),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.error || "No se pudo marcar la mesa");
      }
      await cargarAvisosMesas();
      if (window.Toast) Toast.success("Mesa marcada por atender.");
    } catch (error) {
      if (window.Toast) Toast.error(error.message || "No se pudo marcar la mesa.");
      button.disabled = false;
    } finally {
      button.dataset.saving = "0";
    }
  }

  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-mesa-alert-trigger]");
    if (!button || button.disabled) return;
    marcarMesaPorAtender(button);
  });

  cargarAvisosMesas();
  window.setInterval(cargarAvisosMesas, 8000);
})();
