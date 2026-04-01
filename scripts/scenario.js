const scenarioDataset = [
  {
    id: "scenario-a",
    name: "Scenario A",
    badge: "Baseline",
    description: "维持当前管理强度，以稳定蜂群活动与开花资源的耦合关系为主。",
    summaryValue: "+12%",
    summaryLabel: "资源匹配提升",
    chartTitle: "Scenario A 趋势曲线",
    curve: [32, 38, 41, 48, 55, 60, 64],
    metrics: [
      { id: "activity", label: "活动响应", value: 74, unit: "%", meta: "相较当前方案，活跃峰值更平稳", tone: "positive" },
      { id: "nectar", label: "蜜源利用", value: 68, unit: "%", meta: "资源利用效率维持中高水平", tone: "" },
      { id: "risk", label: "错配风险", value: 21, unit: "%", meta: "风险控制在较低范围", tone: "positive" },
      { id: "stability", label: "系统稳定度", value: 83, unit: "", meta: "方案适合常态运行", tone: "" }
    ]
  },
  {
    id: "scenario-b",
    name: "Scenario B",
    badge: "Adaptive",
    description: "提高对天气波动的响应强度，优先提升资源补偿与行为调度灵活度。",
    summaryValue: "+18%",
    summaryLabel: "行为调节弹性",
    chartTitle: "Scenario B 趋势曲线",
    curve: [24, 29, 43, 58, 62, 70, 76],
    metrics: [
      { id: "activity", label: "活动响应", value: 81, unit: "%", meta: "对环境变化反应更快", tone: "positive" },
      { id: "nectar", label: "蜜源利用", value: 63, unit: "%", meta: "资源拉升明显，但波动更强", tone: "" },
      { id: "risk", label: "错配风险", value: 29, unit: "%", meta: "需要关注后段风险上扬", tone: "warning" },
      { id: "stability", label: "系统稳定度", value: 71, unit: "", meta: "适合弹性调整场景", tone: "" }
    ]
  },
  {
    id: "scenario-c",
    name: "Scenario C",
    badge: "Conservative",
    description: "采取更保守的干预节奏，优先降低预测波动，适用于风险敏感阶段。",
    summaryValue: "-9%",
    summaryLabel: "波动收敛",
    chartTitle: "Scenario C 趋势曲线",
    curve: [44, 46, 43, 41, 39, 37, 36],
    metrics: [
      { id: "activity", label: "活动响应", value: 59, unit: "%", meta: "行为变化更平缓", tone: "" },
      { id: "nectar", label: "蜜源利用", value: 57, unit: "%", meta: "资源利用趋稳但增幅有限", tone: "" },
      { id: "risk", label: "错配风险", value: 14, unit: "%", meta: "风险显著降低", tone: "positive" },
      { id: "stability", label: "系统稳定度", value: 88, unit: "", meta: "适合高约束决策场景", tone: "positive" }
    ]
  }
];

const state = {
  currentIndex: 0
};

const carouselElement = document.getElementById("scenarioCarousel");
const chartElement = document.getElementById("scenarioChart");
const metricsGridElement = document.getElementById("metricsGrid");
const chartTitleElement = document.getElementById("chartTitle");
const scenarioStateTagElement = document.getElementById("scenarioStateTag");
const prevButton = document.getElementById("carouselPrev");
const nextButton = document.getElementById("carouselNext");

function wrapIndex(index) {
  const total = scenarioDataset.length;
  return (index + total) % total;
}

function getScenario(index = state.currentIndex) {
  return scenarioDataset[wrapIndex(index)];
}

function createScenarioCardMarkup(item, position) {
  const transformMap = {
    center: "translateX(-50%) translateY(0) scale(1)",
    left: "translateX(calc(-50% - 270px)) translateY(18px) scale(0.84)",
    right: "translateX(calc(-50% + 270px)) translateY(18px) scale(0.84)"
  };

  const classMap = {
    center: "scenario-card is-center",
    left: "scenario-card is-side",
    right: "scenario-card is-side"
  };

  return `
    <article
      class="${classMap[position]}"
      style="transform:${transformMap[position]}; z-index:${position === "center" ? 3 : 2};"
      data-scenario-id="${item.id}"
      aria-hidden="${position === "center" ? "false" : "true"}"
    >
      <div>
        <div class="scenario-card-header">
          <h3 class="scenario-card-name">${item.name}</h3>
          <span class="scenario-card-badge">${item.badge}</span>
        </div>
        <p class="scenario-card-description">${item.description}</p>
      </div>
      <div class="scenario-card-footer">
        <div class="scenario-card-kpi">
          <p class="scenario-card-kpi-label">${item.summaryLabel}</p>
          <p class="scenario-card-kpi-value">${item.summaryValue}</p>
        </div>
      </div>
    </article>
  `;
}

function renderCarousel() {
  const current = getScenario(state.currentIndex);
  const previous = getScenario(state.currentIndex - 1);
  const next = getScenario(state.currentIndex + 1);

  carouselElement.innerHTML = [
    createScenarioCardMarkup(previous, "left"),
    createScenarioCardMarkup(current, "center"),
    createScenarioCardMarkup(next, "right")
  ].join("");
}

function buildSmoothPath(points) {
  if (!points.length) {
    return "";
  }

  if (points.length === 1) {
    return `M ${points[0].x} ${points[0].y}`;
  }

  let path = `M ${points[0].x} ${points[0].y}`;
  for (let index = 1; index < points.length; index += 1) {
    const prev = points[index - 1];
    const current = points[index];
    const controlX = (prev.x + current.x) / 2;
    path += ` C ${controlX} ${prev.y}, ${controlX} ${current.y}, ${current.x} ${current.y}`;
  }
  return path;
}

function renderChart() {
  const scenario = getScenario();
  const chartValues = scenario.curve;
  const width = 760;
  const height = 320;
  const padding = { top: 26, right: 28, bottom: 36, left: 38 };
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const maxValue = 100;
  const minValue = 0;

  const points = chartValues.map((value, index) => {
    const x = padding.left + (innerWidth / (chartValues.length - 1)) * index;
    const y = padding.top + innerHeight - ((value - minValue) / (maxValue - minValue)) * innerHeight;
    return { x, y, value };
  });

  const path = buildSmoothPath(points);
  const areaPath = `${path} L ${points[points.length - 1].x} ${padding.top + innerHeight} L ${points[0].x} ${padding.top + innerHeight} Z`;
  const gridRows = [0, 25, 50, 75, 100];
  const xLabels = ["W1", "W2", "W3", "W4", "W5", "W6", "W7"];

  chartTitleElement.textContent = scenario.chartTitle;
  scenarioStateTagElement.textContent = scenario.name;

  chartElement.innerHTML = `
    <defs>
      <linearGradient id="scenarioAreaGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="rgba(240, 221, 44, 0.34)"></stop>
        <stop offset="100%" stop-color="rgba(240, 221, 44, 0.02)"></stop>
      </linearGradient>
    </defs>
    ${gridRows.map(row => {
      const y = padding.top + innerHeight - (row / 100) * innerHeight;
      return `
        <line class="chart-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
        <text class="chart-axis-label" x="${padding.left - 10}" y="${y + 4}" text-anchor="end">${row}</text>
      `;
    }).join("")}
    ${points.map((point, index) => `
      <line class="chart-grid-line" x1="${point.x}" y1="${padding.top}" x2="${point.x}" y2="${padding.top + innerHeight}"></line>
      <text class="chart-axis-label" x="${point.x}" y="${height - 12}" text-anchor="middle">${xLabels[index]}</text>
    `).join("")}
    <path class="chart-area-fill" d="${areaPath}" fill="url(#scenarioAreaGradient)"></path>
    <path class="chart-curve-base" d="${path}"></path>
    <path class="chart-curve-main" d="${path}"></path>
    ${points.map((point, index) => `
      <circle class="${index === points.length - 1 ? "chart-point-highlight" : "chart-point"}" cx="${point.x}" cy="${point.y}" r="${index === points.length - 1 ? 6 : 5}"></circle>
    `).join("")}
  `;
}

function createMetricCard(metric) {
  const card = document.createElement("article");
  card.className = `metric-card ${metric.tone ? `metric-card-${metric.tone}` : ""}`.trim();

  const label = document.createElement("p");
  label.className = "metric-card-label";
  label.textContent = metric.label;

  const valueRow = document.createElement("div");
  valueRow.className = "metric-card-value-row";

  const value = document.createElement("p");
  value.className = "metric-card-value";
  value.textContent = String(metric.value);

  valueRow.appendChild(value);

  if (metric.unit) {
    const unit = document.createElement("span");
    unit.className = "metric-card-unit";
    unit.textContent = metric.unit;
    valueRow.appendChild(unit);
  }

  const meta = document.createElement("p");
  meta.className = "metric-card-meta";
  meta.textContent = metric.meta;

  card.append(label, valueRow, meta);
  return card;
}

function renderMetrics() {
  const scenario = getScenario();
  metricsGridElement.innerHTML = "";
  scenario.metrics.forEach(metric => {
    metricsGridElement.appendChild(createMetricCard(metric));
  });
}

function renderScenarioPage() {
  renderCarousel();
  renderChart();
  renderMetrics();
}

function goToScenario(nextIndex) {
  state.currentIndex = wrapIndex(nextIndex);
  renderScenarioPage();
}

prevButton.addEventListener("click", () => {
  goToScenario(state.currentIndex - 1);
});

nextButton.addEventListener("click", () => {
  goToScenario(state.currentIndex + 1);
});

renderScenarioPage();
