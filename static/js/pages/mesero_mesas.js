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
      card.classList.toggle("mesa-card--alerta", Boolean(aviso));
      if (aviso && !badge) {
        card.appendChild(buildBadge(aviso.motivo || ""));
      } else if (aviso && badge) {
        badge.title = aviso.motivo || "Mesa por atender";
      } else if (!aviso && badge) {
        badge.remove();
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

  cargarAvisosMesas();
  window.setInterval(cargarAvisosMesas, 8000);
})();
