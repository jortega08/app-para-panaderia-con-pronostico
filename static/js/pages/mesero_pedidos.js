(function () {
  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function updateRow(row, entregada) {
    const total = Number(row.dataset.total || 0);
    const itemId = row.dataset.itemId;
    const finalValue = clamp(Number(entregada || 0), 0, total);
    row.dataset.entregada = String(finalValue);
    row.classList.toggle("is-complete", total > 0 && finalValue >= total);

    const label = row.querySelector(`[data-entrega-label="${itemId}"]`);
    if (label) {
      label.textContent = `Entregado ${finalValue}/${total} - pendiente ${Math.max(total - finalValue, 0)}`;
    }
    const progress = row.querySelector(`[data-entrega-progress="${itemId}"]`);
    if (progress) {
      progress.style.width = total ? `${(finalValue / total) * 100}%` : "0%";
    }
    row.querySelectorAll("[data-entrega-action]").forEach((button) => {
      const action = button.dataset.entregaAction;
      button.disabled = row.dataset.saving === "1"
        || (["increment", "deliver"].includes(action) && finalValue >= total)
        || (action === "decrement" && finalValue <= 0);
    });
  }

  function updatePedidoSummary(pedidoId) {
    const rows = Array.from(document.querySelectorAll(`[data-pedido-id="${pedidoId}"]`));
    const total = rows.reduce((sum, row) => sum + Number(row.dataset.total || 0), 0);
    const entregada = rows.reduce((sum, row) => sum + Number(row.dataset.entregada || 0), 0);
    const resumen = document.querySelector(`[data-pedido-entrega-resumen="${pedidoId}"]`);
    if (resumen) resumen.textContent = `${entregada}/${total}`;

    const complete = total > 0 && entregada >= total;
    let badge = document.querySelector(`[data-pedido-entrega-badge="${pedidoId}"]`);
    const header = rows[0]?.closest(".pedido-card")?.querySelector(".pedido-info");
    if (complete && !badge && header) {
      badge = document.createElement("span");
      badge.className = "pedido-badge pedido-badge-listo";
      badge.dataset.pedidoEntregaBadge = pedidoId;
      badge.textContent = "Entregado";
      header.appendChild(badge);
    } else if (!complete && badge) {
      badge.remove();
    }
  }

  function hydratePedidoFromResponse(pedido) {
    if (!pedido || !Array.isArray(pedido.items)) return;
    pedido.items.forEach((item) => {
      const row = document.querySelector(`[data-entrega-row="${item.id}"]`);
      if (row) updateRow(row, Number(item.cantidad_entregada || 0));
    });
    updatePedidoSummary(pedido.id);
  }

  async function persistRow(row, nextValue, previousValue) {
    row.dataset.saving = "1";
    updateRow(row, nextValue);
    try {
      const pedidoId = row.dataset.pedidoId;
      const itemId = row.dataset.itemId;
      const response = await fetch(`/api/pedido/${pedidoId}/items/${itemId}/entrega`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cantidad_entregada: nextValue }),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.error || "No se pudo actualizar la entrega");
      }
      hydratePedidoFromResponse(data.pedido);
      updatePedidoSummary(pedidoId);
    } catch (error) {
      updateRow(row, previousValue);
      updatePedidoSummary(row.dataset.pedidoId);
      if (window.Toast) Toast.error(error.message || "No se pudo actualizar la entrega.");
    } finally {
      row.dataset.saving = "0";
      updateRow(row, Number(row.dataset.entregada || 0));
    }
  }

  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-entrega-action]");
    if (!button) return;
    const row = button.closest("[data-entrega-row]");
    if (!row || row.dataset.saving === "1") return;

    const current = Number(row.dataset.entregada || 0);
    const total = Number(row.dataset.total || 0);
    const action = button.dataset.entregaAction;
    const delta = action === "decrement" ? -1 : 1;
    const next = clamp(current + delta, 0, total);
    if (next === current) return;
    persistRow(row, next, current);
  });

  document.querySelectorAll("[data-entrega-row]").forEach((row) => {
    updateRow(row, Number(row.dataset.entregada || 0));
  });
})();
