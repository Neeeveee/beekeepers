(function () {
  const CONFIG = {
    tileUrl: "./map_embed_flower/qgis tiles 02/{z}/{x}/{y}.png",
    geojsonUrl: "./data/nectar-distribution.geojson",
    bounds: {
      south: 30.407044,
      west: 120.148915,
      north: 30.460942,
      east: 120.260395
    },
    minZoom: 13,
    maxZoom: 17,
    slideMs: 4800
  };

  const COLORS = {
    "枇杷": "#f39c12",
    "油菜": "#82d8c4",
    "柃木": "#e0a09b",
    "果梅": "#b45aa1",
    "五倍子": "#c6b347",
    "苦槠": "#a8c9d7",
    "栾树": "#a9d07a",
    "冬青": "#f3ea2a",
    "蜜源空档": "#9aa6a0"
  };

  const MONTHS = [
    { number: "01", label: "月", title: "枇杷花源区", sources: ["枇杷"], summary: "冬季早春蜜源斑块，为蜂群越冬后恢复提供连续补充。" },
    { number: "02", label: "月", title: "早春复合花源区", sources: ["枇杷", "果梅"], summary: "展示果木类蜜源开始接续，说明早春资源由点状向片区过渡。" },
    { number: "03", label: "月", title: "油菜主花源区", sources: ["油菜", "柃木", "果梅", "枇杷"], summary: "油菜进入主要供给期，叠加早春果木花源，形成春季高密度蜜源窗口。" },
    { number: "04", label: "月", title: "苦槠冬青复合区", sources: ["苦槠", "冬青", "油菜"], summary: "春末蜜源由油菜转向木本花源，体现资源接续和蜂群活动窗口变化。" },
    { number: "05", label: "月", title: "冬青主花源区", sources: ["冬青", "苦槠", "五倍子"], summary: "冬青维持主要蜜源供给，苦槠和五倍子作为辅助斑块补充空间连续性。" },
    { number: "06", label: "月", title: "栾树过渡花源区", sources: ["栾树", "五倍子"], summary: "初夏花源转入较分散的树种斑块，用于说明夏季资源的过渡特征。" },
    { number: "07", label: "月", title: "栾树花源区", sources: ["栾树"], summary: "栾树成为主要可视化蜜源，展示夏季局部斑块供给和空间分布。" },
    { number: "08", label: "月", title: "复合花源区", sources: ["蜜源空档"], summary: "展示多类型蜜源混合斑块，用于说明夏季资源的分散性和连续性。" },
    { number: "09", label: "月", title: "复合花源区", sources: ["蜜源空档"], summary: "突出零散斑块与潜在补充区域，呈现秋季前后蜜源连接关系。" },
    { number: "10", label: "月", title: "复合花源区", sources: ["蜜源空档"], summary: "以混合斑块表达蜜源低位期，为后续冬季枇杷花源接续做铺垫。" },
    { number: "11", label: "月", title: "枇杷花源区", sources: ["枇杷"], summary: "枇杷进入冬季蜜源前段，形成低温季节的重要早花资源。" },
    { number: "12", label: "月", title: "枇杷主花源区", sources: ["枇杷"], summary: "冬季枇杷花源持续供给，展示全年蜜源轮转的末端与新一轮开端。" }
  ];

  const bounds = L.latLngBounds(
    [CONFIG.bounds.south, CONFIG.bounds.west],
    [CONFIG.bounds.north, CONFIG.bounds.east]
  );

  const map = L.map("nectarMap", {
    zoomControl: false,
    attributionControl: false,
    maxBounds: bounds,
    maxBoundsViscosity: 1,
    maxZoom: CONFIG.maxZoom,
    zoomSnap: 0.1,
    zoomDelta: 0.2,
    wheelPxPerZoomLevel: 120,
    preferCanvas: true,
    keyboard: false
  });

  L.tileLayer(CONFIG.tileUrl, {
    minNativeZoom: CONFIG.minZoom,
    maxNativeZoom: CONFIG.maxZoom,
    maxZoom: CONFIG.maxZoom,
    noWrap: true,
    opacity: 0.92,
    keepBuffer: 2
  }).addTo(map);

  const fittedZoom = Math.min(
    CONFIG.maxZoom,
    map.getBoundsZoom(bounds, false)
  );
  const minimumDisplayZoom = Math.min(CONFIG.maxZoom, fittedZoom + 0.5);

  map.setMinZoom(minimumDisplayZoom);
  map.fitBounds(bounds, {
    animate: false,
    padding: [0, 0],
    maxZoom: minimumDisplayZoom
  });
  map.setZoom(minimumDisplayZoom, { animate: false });

  const monthNumber = document.getElementById("monthNumber");
  const monthLabel = document.getElementById("monthLabel");
  const mainSources = document.getElementById("mainSources");
  const sourceSummary = document.getElementById("sourceSummary");
  const sourceChips = document.getElementById("sourceChips");
  const progressBar = document.getElementById("progressBar");

  let nectarLayer = null;
  let labelLayer = L.layerGroup().addTo(map);
  let currentIndex = 0;
  let startedAt = Date.now();
  let timer = null;
  let progressTimer = null;
  let paused = false;

  function showNote(message) {
    const note = document.createElement("div");
    note.className = "loading-note";
    note.textContent = message;
    document.body.appendChild(note);
    return note;
  }

  const loadingNote = showNote("正在加载蜜源分布图...");

  function getSpecies(feature) {
    return feature && feature.properties
      ? feature.properties.display_species || feature.properties.species || ""
      : "";
  }

  function getStyle(activeSources) {
    return function (feature) {
      const species = getSpecies(feature);
      const active = activeSources.includes(species);
      const gap = activeSources.includes("蜜源空档");
      const color = COLORS[species] || "#7fb08d";

      return {
        color,
        weight: active || gap ? 1.1 : 0.2,
        opacity: active || gap ? 0.92 : 0.08,
        fillColor: color,
        fillOpacity: active ? 0.68 : gap ? 0.34 : 0.06,
        interactive: false
      };
    };
  }

  function renderChips(sources) {
    sourceChips.innerHTML = sources.map((source) => {
      const color = COLORS[source] || "#9aa6a0";
      return `<span class="source-chip"><span class="source-dot" style="background:${color}"></span>${source}</span>`;
    }).join("");
  }

  function renderLabels(sources) {
    labelLayer.clearLayers();
    if (!nectarLayer) {
      return;
    }

    const placed = new Set();
    nectarLayer.eachLayer((layer) => {
      const species = getSpecies(layer.feature);
      const showAll = sources.includes("蜜源空档");
      if ((!showAll && !sources.includes(species)) || placed.has(species)) {
        return;
      }

      placed.add(species);
      const center = layer.getBounds().getCenter();
      L.marker(center, {
        interactive: false,
        icon: L.divIcon({
          className: "",
          html: `<span class="map-label">${species}</span>`,
          iconSize: [80, 24],
          iconAnchor: [40, 12]
        })
      }).addTo(labelLayer);
    });
  }

  function renderSlide(index) {
    const slide = MONTHS[index];
    monthNumber.textContent = slide.number;
    monthLabel.textContent = slide.label;
    mainSources.textContent = slide.title;
    sourceSummary.textContent = slide.summary;
    renderChips(slide.sources);

    if (nectarLayer) {
      nectarLayer.setStyle(getStyle(slide.sources));
      renderLabels(slide.sources);
    }

    startedAt = Date.now();
    updateProgress(0);
  }

  function updateProgress(ratio) {
    const radius = 19;
    const circumference = 2 * Math.PI * radius;
    progressBar.style.strokeDasharray = `${circumference}`;
    progressBar.style.strokeDashoffset = `${circumference * (1 - ratio)}`;
  }

  function goTo(index) {
    currentIndex = (index + MONTHS.length) % MONTHS.length;
    renderSlide(currentIndex);
  }

  function startTimers() {
    clearInterval(timer);
    clearInterval(progressTimer);

    timer = setInterval(() => {
      if (!paused) {
        goTo(currentIndex + 1);
      }
    }, CONFIG.slideMs);

    progressTimer = setInterval(() => {
      if (paused) {
        return;
      }
      const elapsed = Date.now() - startedAt;
      const ratio = Math.min(1, elapsed / CONFIG.slideMs);
      updateProgress(ratio);
    }, 120);
  }

  function bindKeys() {
    window.addEventListener("keydown", (event) => {
      if (event.key === "ArrowRight") {
        goTo(currentIndex + 1);
      } else if (event.key === "ArrowLeft") {
        goTo(currentIndex - 1);
      } else if (event.code === "Space") {
        paused = !paused;
        startedAt = Date.now();
        event.preventDefault();
      }
    });
  }

  fetch(CONFIG.geojsonUrl)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return response.json();
    })
    .then((geojson) => {
      loadingNote.remove();
      nectarLayer = L.geoJSON(geojson, {
        style: getStyle(MONTHS[currentIndex].sources)
      }).addTo(map);
      nectarLayer.bringToFront();
      renderSlide(currentIndex);
    })
    .catch((error) => {
      loadingNote.textContent = `蜜源分布数据加载失败：${error.message}`;
    });

  renderSlide(currentIndex);
  startTimers();
  bindKeys();
}());
