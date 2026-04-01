const scenarios = [
  {
    id: "scenario-a",
    shortLabel: "A",
    title: "Scenario A",
    subtitle: "Balanced placeholder direction",
    description: "A calm default strategy shell for future AI output and data injection.",
    statusMode: "Focused",
    statusVersion: "Draft 01",
    metrics: [
      { label: "Coverage", value: "74", fill: 74 },
      { label: "Cost", value: "42", fill: 42 },
      { label: "Stability", value: "81", fill: 81 },
      { label: "Impact", value: "+12%", fill: 68 },
    ],
  },
  {
    id: "scenario-b",
    shortLabel: "B",
    title: "Scenario B",
    subtitle: "Intervention-heavy placeholder direction",
    description: "A more active strategy shell with stronger placeholder metric changes.",
    statusMode: "Testing",
    statusVersion: "Draft 02",
    metrics: [
      { label: "Coverage", value: "63", fill: 63 },
      { label: "Cost", value: "68", fill: 68 },
      { label: "Stability", value: "58", fill: 58 },
      { label: "Impact", value: "+19%", fill: 76 },
    ],
  },
  {
    id: "scenario-c",
    shortLabel: "C",
    title: "Scenario C",
    subtitle: "Conservative placeholder direction",
    description: "A restrained strategy shell intended for low-risk comparison views.",
    statusMode: "Reviewing",
    statusVersion: "Draft 03",
    metrics: [
      { label: "Coverage", value: "52", fill: 52 },
      { label: "Cost", value: "27", fill: 27 },
      { label: "Stability", value: "88", fill: 88 },
      { label: "Impact", value: "+7%", fill: 49 },
    ],
  },
];

const state = {
  focusIndex: 0,
  selectedId: scenarios[0].id,
  overlayVisible: true,
};

const overlay = document.getElementById("scenarioOverlay");
const carouselStage = document.getElementById("carouselStage");
const metricCardList = document.getElementById("metricCardList");
const chatComposer = document.getElementById("chatComposer");
const overlayRecallButton = document.getElementById("overlayRecallButton");
const contentTitle = document.getElementById("contentTitle");
const contentSubtitle = document.getElementById("contentSubtitle");
const panelBadge = document.getElementById("panelBadge");
const statusMode = document.getElementById("statusMode");
const statusVersion = document.getElementById("statusVersion");
const scenarioMiniChart = document.getElementById("scenarioMiniChart");
const prevButton = document.getElementById("carouselPrev");
const nextButton = document.getElementById("carouselNext");

function normalizeIndex(index) {
  const total = scenarios.length;
  return ((index % total) + total) % total;
}

function setOverlayVisible(visible) {
  state.overlayVisible = visible;
  overlay.classList.toggle("is-hidden", !visible);
  overlayRecallButton.style.opacity = visible ? "0.72" : "1";
}

function buildMetricCard(metric) {
  return `
    <article class="metric-card">
      <div class="metric-topline">
        <span class="metric-label">${metric.label}</span>
        <strong class="metric-value">${metric.value}</strong>
      </div>
      <div class="metric-line" style="--fill: ${metric.fill}%"></div>
    </article>
  `;
}

function buildCard(scenario, offset, absoluteIndex) {
  const baseTranslate = offset * 250;
  const scale = offset === 0 ? 1 : 0.88;
  const opacity = offset === 0 ? 1 : 0.45;
  const blur = offset === 0 ? 0 : 1.5;
  const zIndex = offset === 0 ? 3 : 2 - Math.abs(offset);
  const isSelected = scenario.id === state.selectedId;

  return `
    <button
      type="button"
      class="scenario-card ${offset === 0 ? "is-center" : ""} ${isSelected ? "is-selected" : ""}"
      data-card-index="${absoluteIndex}"
      data-scenario-id="${scenario.id}"
      style="
        transform: translateX(calc(-50% + ${baseTranslate}px)) scale(${scale});
        opacity: ${opacity};
        z-index: ${zIndex};
        filter: blur(${blur}px);
      "
      aria-label="${scenario.title}"
    >
      <div class="card-topline">
        <span class="card-chip"></span>
        <span class="card-kpi"></span>
      </div>
      <span class="card-title-placeholder"></span>
      <span class="card-text-placeholder"></span>
      <span class="card-text-placeholder short"></span>
      <div class="card-number-row">
        <span class="card-number-placeholder"></span>
        <span class="card-number-placeholder"></span>
      </div>
      <span class="card-footer-placeholder"></span>
    </button>
  `;
}

function renderCarousel() {
  const leftIndex = normalizeIndex(state.focusIndex - 1);
  const centerIndex = normalizeIndex(state.focusIndex);
  const rightIndex = normalizeIndex(state.focusIndex + 1);

  const cards = [
    buildCard(scenarios[leftIndex], -1, leftIndex),
    buildCard(scenarios[centerIndex], 0, centerIndex),
    buildCard(scenarios[rightIndex], 1, rightIndex),
  ];

  carouselStage.innerHTML = cards.join("");

  carouselStage.querySelectorAll(".scenario-card").forEach((card) => {
    card.addEventListener("click", () => {
      const index = Number(card.dataset.cardIndex);
      if (index !== state.focusIndex) {
        state.focusIndex = index;
      }
      state.selectedId = card.dataset.scenarioId;
      render();
    });
  });
}

function renderSelectedScenario() {
  const active = scenarios.find((item) => item.id === state.selectedId) || scenarios[state.focusIndex];

  contentTitle.textContent = active.title;
  contentSubtitle.textContent = active.description;
  panelBadge.textContent = active.shortLabel;
  statusMode.textContent = active.statusMode;
  statusVersion.textContent = active.statusVersion;
  metricCardList.innerHTML = active.metrics.map(buildMetricCard).join("");

  const chartVariants = {
    "scenario-a": {
      area: "polygon(0% 72%, 12% 68%, 26% 62%, 37% 66%, 48% 55%, 61% 49%, 72% 52%, 83% 39%, 100% 34%, 100% 100%, 0 100%)",
      line: "polygon(0% 72%, 12% 68%, 26% 62%, 37% 66%, 48% 55%, 61% 49%, 72% 52%, 83% 39%, 100% 34%)",
    },
    "scenario-b": {
      area: "polygon(0% 78%, 10% 76%, 24% 60%, 34% 65%, 46% 45%, 57% 51%, 70% 36%, 82% 43%, 100% 30%, 100% 100%, 0 100%)",
      line: "polygon(0% 78%, 10% 76%, 24% 60%, 34% 65%, 46% 45%, 57% 51%, 70% 36%, 82% 43%, 100% 30%)",
    },
    "scenario-c": {
      area: "polygon(0% 74%, 14% 69%, 28% 64%, 40% 61%, 52% 57%, 65% 52%, 78% 48%, 90% 42%, 100% 39%, 100% 100%, 0 100%)",
      line: "polygon(0% 74%, 14% 69%, 28% 64%, 40% 61%, 52% 57%, 65% 52%, 78% 48%, 90% 42%, 100% 39%)",
    },
  };

  scenarioMiniChart.style.setProperty("--area-clip", chartVariants[active.id].area);
  scenarioMiniChart.style.setProperty("--line-clip", chartVariants[active.id].line);
}

function render() {
  renderCarousel();
  renderSelectedScenario();
}

prevButton.addEventListener("click", () => {
  state.focusIndex = normalizeIndex(state.focusIndex - 1);
  render();
});

nextButton.addEventListener("click", () => {
  state.focusIndex = normalizeIndex(state.focusIndex + 1);
  render();
});

chatComposer.addEventListener("focus", () => {
  setOverlayVisible(false);
});

overlayRecallButton.addEventListener("click", () => {
  setOverlayVisible(true);
});

chatComposer.addEventListener("input", () => {
  chatComposer.style.height = "auto";
  chatComposer.style.height = `${Math.min(chatComposer.scrollHeight, 140)}px`;
});

let pointerStartX = null;
carouselStage.addEventListener("pointerdown", (event) => {
  pointerStartX = event.clientX;
});

carouselStage.addEventListener("pointerup", (event) => {
  if (pointerStartX === null) {
    return;
  }

  const deltaX = event.clientX - pointerStartX;
  if (deltaX > 40) {
    state.focusIndex = normalizeIndex(state.focusIndex - 1);
    render();
  } else if (deltaX < -40) {
    state.focusIndex = normalizeIndex(state.focusIndex + 1);
    render();
  }
  pointerStartX = null;
});

carouselStage.addEventListener("pointerleave", () => {
  pointerStartX = null;
});

render();

