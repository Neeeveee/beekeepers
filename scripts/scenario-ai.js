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
      { label: "Impact", value: "+12%", fill: 68 }
    ]
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
      { label: "Impact", value: "+19%", fill: 76 }
    ]
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
      { label: "Impact", value: "+7%", fill: 49 }
    ]
  }
];

const SLOT_CONFIG = {
  "-3": { x: -760, scale: 0.68, opacity: 0, blur: 8, z: 0 },
  "-2": { x: -510, scale: 0.78, opacity: 0, blur: 5, z: 0 },
  "-1": { x: -255, scale: 0.88, opacity: 0.44, blur: 1.4, z: 2 },
  "0": { x: 0, scale: 1, opacity: 1, blur: 0, z: 4 },
  "1": { x: 255, scale: 0.88, opacity: 0.44, blur: 1.4, z: 2 },
  "2": { x: 510, scale: 0.78, opacity: 0, blur: 5, z: 0 },
  "3": { x: 760, scale: 0.68, opacity: 0, blur: 8, z: 0 }
};

const ACTIVE_OFFSETS = [-2, -1, 0, 1, 2];
const TRANSITION_MS = 460;

const state = {
  focusIndex: 0,
  selectedId: scenarios[0].id,
  overlayVisible: true,
  isAnimating: false
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

function getWindowScenarioIndex(offsetFromFocus) {
  return normalizeIndex(state.focusIndex + offsetFromFocus);
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

function createCardElement(scenarioIndex) {
  const scenario = scenarios[scenarioIndex];
  const card = document.createElement("button");
  card.type = "button";
  card.className = "scenario-card";
  card.dataset.scenarioId = scenario.id;
  card.dataset.scenarioIndex = String(scenarioIndex);
  card.setAttribute("aria-label", scenario.title);
  card.innerHTML = `
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
  `;
  return card;
}

function applyCardVisual(card, slot) {
  const config = SLOT_CONFIG[String(slot)];
  card.dataset.slot = String(slot);
  card.style.transform = `translateX(calc(-50% + ${config.x}px)) scale(${config.scale})`;
  card.style.opacity = String(config.opacity);
  card.style.filter = `blur(${config.blur}px)`;
  card.style.zIndex = String(config.z);
  card.classList.toggle("is-center", slot === 0);
  card.classList.toggle("is-side", slot === -1 || slot === 1);
  card.classList.toggle("is-selected", card.dataset.scenarioId === state.selectedId);
}

function mountCarouselWindow() {
  carouselStage.innerHTML = "";
  ACTIVE_OFFSETS.forEach((offset) => {
    const scenarioIndex = getWindowScenarioIndex(offset);
    const card = createCardElement(scenarioIndex);
    applyCardVisual(card, offset);
    carouselStage.appendChild(card);
  });
  bindCardEvents();
}

function bindCardEvents() {
  carouselStage.querySelectorAll(".scenario-card").forEach((card) => {
    card.onclick = () => {
      if (state.isAnimating) {
        return;
      }

      const clickedIndex = Number(card.dataset.scenarioIndex);
      if (clickedIndex === state.focusIndex) {
        state.selectedId = card.dataset.scenarioId;
        syncSelectedCardState();
        renderSelectedScenario();
        return;
      }

      const delta = (clickedIndex - state.focusIndex + scenarios.length) % scenarios.length;
      if (delta === 1) {
        animateCarousel(1);
      } else if (delta === scenarios.length - 1) {
        animateCarousel(-1);
      }
    };
  });
}

function syncSelectedCardState() {
  carouselStage.querySelectorAll(".scenario-card").forEach((card) => {
    card.classList.toggle("is-selected", card.dataset.scenarioId === state.selectedId);
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
      line: "polygon(0% 72%, 12% 68%, 26% 62%, 37% 66%, 48% 55%, 61% 49%, 72% 52%, 83% 39%, 100% 34%)"
    },
    "scenario-b": {
      area: "polygon(0% 78%, 10% 76%, 24% 60%, 34% 65%, 46% 45%, 57% 51%, 70% 36%, 82% 43%, 100% 30%, 100% 100%, 0 100%)",
      line: "polygon(0% 78%, 10% 76%, 24% 60%, 34% 65%, 46% 45%, 57% 51%, 70% 36%, 82% 43%, 100% 30%)"
    },
    "scenario-c": {
      area: "polygon(0% 74%, 14% 69%, 28% 64%, 40% 61%, 52% 57%, 65% 52%, 78% 48%, 90% 42%, 100% 39%, 100% 100%, 0 100%)",
      line: "polygon(0% 74%, 14% 69%, 28% 64%, 40% 61%, 52% 57%, 65% 52%, 78% 48%, 90% 42%, 100% 39%)"
    }
  };

  scenarioMiniChart.style.setProperty("--area-clip", chartVariants[active.id].area);
  scenarioMiniChart.style.setProperty("--line-clip", chartVariants[active.id].line);
}

function animateCarousel(direction) {
  if (state.isAnimating) {
    return;
  }

  state.isAnimating = true;
  prevButton.disabled = true;
  nextButton.disabled = true;

  const enteringScenarioIndex = getWindowScenarioIndex(direction > 0 ? 3 : -3);
  const enteringCard = createCardElement(enteringScenarioIndex);
  applyCardVisual(enteringCard, direction > 0 ? 2 : -2);
  carouselStage.appendChild(enteringCard);

  requestAnimationFrame(() => {
    [...carouselStage.children].forEach((card) => {
      const currentSlot = Number(card.dataset.slot);
      applyCardVisual(card, currentSlot - direction);
    });
  });

  window.setTimeout(() => {
    state.focusIndex = normalizeIndex(state.focusIndex + direction);
    state.selectedId = scenarios[state.focusIndex].id;
    mountCarouselWindow();
    renderSelectedScenario();
    state.isAnimating = false;
    prevButton.disabled = false;
    nextButton.disabled = false;
  }, TRANSITION_MS + 30);
}

prevButton.addEventListener("click", () => {
  animateCarousel(-1);
});

nextButton.addEventListener("click", () => {
  animateCarousel(1);
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
  if (pointerStartX === null || state.isAnimating) {
    pointerStartX = null;
    return;
  }

  const deltaX = event.clientX - pointerStartX;
  if (deltaX > 40) {
    animateCarousel(-1);
  } else if (deltaX < -40) {
    animateCarousel(1);
  }
  pointerStartX = null;
});

carouselStage.addEventListener("pointerleave", () => {
  pointerStartX = null;
});

mountCarouselWindow();
renderSelectedScenario();
