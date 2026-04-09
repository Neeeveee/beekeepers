const dashboardMockDataAdapter = {
  async fetchMetrics() {
    return Promise.resolve([
      { id: "temperature", label: "蜂箱温度", value: 33.6, unit: "°C", meta: "mock: temperature" },
      { id: "humidity", label: "蜂箱湿度", value: 61.2, unit: "%", meta: "mock: humidity" },
      { id: "inflow", label: "进入数量", value: 128, unit: "", meta: "mock: in-count" },
      { id: "outflow", label: "离开数量", value: 117, unit: "", meta: "mock: out-count" },
      { id: "activity", label: "活跃指数", value: 0.74, unit: "", meta: "mock: activity-index" },
      { id: "risk", label: "风险等级", value: 2, unit: "", meta: "mock: risk-level" }
    ]);
  }
};

function formatMetricValue(value) {
  if (typeof value !== "number") {
    return value ?? "--";
  }

  const hasFraction = !Number.isInteger(value);
  return hasFraction ? value.toFixed(1) : String(value);
}

function createMetricCard(metric) {
  const card = document.createElement("article");
  card.className = "metric-card";
  card.dataset.metricId = metric.id;

  const title = document.createElement("p");
  title.className = "metric-card-label";
  title.textContent = metric.label;

  const value = document.createElement("p");
  value.className = "metric-card-value";
  value.textContent = formatMetricValue(metric.value);

  if (metric.unit) {
    const unit = document.createElement("span");
    unit.className = "metric-card-unit";
    unit.textContent = metric.unit;
    value.appendChild(unit);
  }

  const meta = document.createElement("p");
  meta.className = "metric-card-meta";
  meta.textContent = metric.meta || "";

  card.append(title, value, meta);
  return card;
}

async function renderMetrics() {
  const metricsGrid = document.getElementById("metrics-grid");
  const metricItems = await dashboardMockDataAdapter.fetchMetrics();

  metricsGrid.innerHTML = "";
  metricItems.forEach(metric => {
    metricsGrid.appendChild(createMetricCard(metric));
  });
}

renderMetrics();
