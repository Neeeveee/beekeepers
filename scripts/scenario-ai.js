const scenarios = [
  {
    id: "scenario-a",
    shortLabel: "A",
    title: "Scenario A"
  },
  {
    id: "scenario-b",
    shortLabel: "B",
    title: "Scenario B"
  },
  {
    id: "scenario-c",
    shortLabel: "C",
    title: "Scenario C"
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
  isAnimating: false,
  detailOpen: false
};

const overlay = document.getElementById("scenarioOverlay");
const carouselStage = document.getElementById("carouselStage");
const chatComposer = document.getElementById("chatComposer");
const overlayRecallButton = document.getElementById("overlayRecallButton");
const prevButton = document.getElementById("carouselPrev");
const nextButton = document.getElementById("carouselNext");
const detailOverlay = document.getElementById("detailOverlay");
const detailBackdrop = document.getElementById("detailBackdrop");
const detailCloseButton = document.getElementById("detailCloseButton");

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

function setDetailOpen(open) {
  state.detailOpen = open;
  detailOverlay.classList.toggle("is-visible", open);
  detailOverlay.setAttribute("aria-hidden", open ? "false" : "true");
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

function syncSelectedCardState() {
  carouselStage.querySelectorAll(".scenario-card").forEach((card) => {
    card.classList.toggle("is-selected", card.dataset.scenarioId === state.selectedId);
  });
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
        return;
      }

      const delta = (clickedIndex - state.focusIndex + scenarios.length) % scenarios.length;
      if (delta === 1) {
        animateCarousel(1);
      } else if (delta === scenarios.length - 1) {
        animateCarousel(-1);
      }
    };

    card.ondblclick = () => {
      if (state.isAnimating) {
        return;
      }
      state.selectedId = card.dataset.scenarioId;
      syncSelectedCardState();
      setDetailOpen(true);
    };
  });
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

detailCloseButton.addEventListener("click", () => {
  setDetailOpen(false);
});

detailBackdrop.addEventListener("click", () => {
  setDetailOpen(false);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && state.detailOpen) {
    setDetailOpen(false);
  }
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
