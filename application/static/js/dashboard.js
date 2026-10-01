(() => {
  const chart = document.getElementById("boarding-chart");
  if (!chart) return;
  const datasets = {
    7: JSON.parse(chart.dataset.seven || "[]"),
    30: JSON.parse(chart.dataset.thirty || "[]"),
    90: JSON.parse(chart.dataset.ninety || "[]"),
  };
  const labels = { 7: "Últimos 7 dias", 30: "Últimos 30 dias", 90: "Últimos 3 meses" };
  const periodLabel = document.getElementById("chart-period-label");
  const buttons = document.querySelectorAll("[data-period]");

  function render(period) {
    const items = datasets[period] || [];
    periodLabel.textContent = labels[period];
    if (!items.length || items.every((item) => Number(item.value) === 0)) {
      chart.innerHTML = '<div class="chart-empty-state"><span class="state-icon">⌁</span><strong>Sem embarques no período</strong><span>Os dados aparecerão assim que houver registros.</span></div>';
      return;
    }
    const width = 760;
    const height = 220;
    const padding = { top: 18, right: 14, bottom: 34, left: 14 };
    const max = Math.max(...items.map((item) => Number(item.value)), 1);
    const step = (width - padding.left - padding.right) / Math.max(items.length - 1, 1);
    const points = items.map((item, index) => {
      const x = padding.left + index * step;
      const y = padding.top + (height - padding.top - padding.bottom) * (1 - Number(item.value) / max);
      return { x, y, label: item.label, value: item.value };
    });
    const line = points.map((point) => `${point.x},${point.y}`).join(" ");
    const area = `${padding.left},${height - padding.bottom} ${line} ${points.at(-1).x},${height - padding.bottom}`;
    const stride = items.length > 14 ? Math.ceil(items.length / 7) : 1;
    chart.innerHTML = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${labels[period]}"><defs><linearGradient id="chart-fill" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#ff7a00" stop-opacity=".26"></stop><stop offset="1" stop-color="#ff7a00" stop-opacity="0"></stop></linearGradient></defs><line class="chart-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line><line class="chart-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line><polygon class="chart-area" points="${area}"></polygon><polyline class="chart-line" points="${line}"></polyline>${points.map((point, index) => `<g class="chart-point" tabindex="0" transform="translate(${point.x} ${point.y})"><circle r="${items.length > 30 ? 2.5 : 4}"></circle><title>${point.label}: ${point.value} embarques</title>${index % stride === 0 || index === points.length - 1 ? `<text y="28" text-anchor="middle">${point.label}</text>` : ""}</g>`).join("")}</svg>`;
  }
  buttons.forEach((button) => button.addEventListener("click", () => { buttons.forEach((item) => item.classList.toggle("is-active", item === button)); render(button.dataset.period); }));
  render("7");
})();
