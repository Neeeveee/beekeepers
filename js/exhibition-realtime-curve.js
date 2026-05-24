(function () {
  const params = new URLSearchParams(window.location.search);
  const DATA_BASE = params.get("data") || "./data";
  const UPDATE_SECONDS = Math.max(1, Number(params.get("speed")) || 60);
  const WINDOW_MINUTES = Math.max(30, Number(params.get("window")) || 180);
  const DEFAULT_METRIC = params.get("metric") || "flowering";
  const CYCLE_SECONDS = Math.max(0, Number(params.get("cycle")) || 0);

  const METRICS = {
    flowering: {
      label: "开花状态",
      title: "生态序列曲线",
      subtitle: "开花状态基于历史实际数据按分钟动态更新",
      source: "flowering-overview.json",
      valueId: "floweringValue"
    },
    nectar: {
      label: "蜜源供给",
      title: "蜜源供给曲线",
      subtitle: "蜜源供给强度基于历史实际数据按分钟动态更新",
      source: "nectar-supply-overview.json",
      valueId: "nectarValue"
    },
    mismatch: {
      label: "错配风险",
      title: "错配风险曲线",
      subtitle: "生态错配风险基于历史实际数据按分钟动态更新",
      source: "mismatch-overview.json",
      valueId: "mismatchValue"
    },
    activity: {
      label: "蜂群活跃",
      title: "蜂群活跃曲线",
      subtitle: "蜂群活跃度基于历史实际数据按分钟动态更新",
      source: "bee-activity-forecast.json",
      valueId: "activityValue"
    }
  };

  const metricOrder = Object.keys(METRICS);
  let activeMetric = METRICS[DEFAULT_METRIC] ? DEFAULT_METRIC : "flowering";
  let sourceCache = {};
  let displayMinute = getInitialDisplayMinute();
  let chart = null;
  let lastCycleAt = Date.now();

  function clamp01(value) {
    if (!Number.isFinite(value)) {
      return null;
    }
    return Math.max(0, Math.min(1, value));
  }

  function parseTime(value) {
    if (!value) {
      return null;
    }
    const normalized = String(value).length === 10 ? `${value}T12:00:00` : String(value).replace(" ", "T");
    const parsed = new Date(normalized);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  function addMinutes(date, minutes) {
    return new Date(date.getTime() + minutes * 60000);
  }

  function getInitialDisplayMinute() {
    const now = new Date();
    now.setSeconds(0, 0);
    return now;
  }

  function seededNoise(index, strength) {
    const value = Math.sin(index * 12.9898 + 78.233) * 43758.5453;
    return (value - Math.floor(value) - 0.5) * strength;
  }

  function ease(t) {
    return t * t * (3 - 2 * t);
  }

  function collectSourcePoints(payload) {
    return [...(payload?.actual || [])]
      .map(item => ({
        time: parseTime(item?.time),
        value: clamp01(Number(item?.value))
      }))
      .filter(item => item.time && item.value !== null)
      .sort((a, b) => a.time - b.time);
  }

  function normalizeTimeline(points, targetMinute) {
    if (!points.length) {
      return [];
    }

    const latestSource = points[points.length - 1].time;
    const offsetMs = targetMinute.getTime() - latestSource.getTime();
    return points.map(point => ({
      time: new Date(point.time.getTime() + offsetMs),
      value: point.value
    }));
  }

  function valueAt(points, time) {
    if (!points.length) {
      return null;
    }
    if (time <= points[0].time) {
      return points[0].value;
    }
    for (let index = 1; index < points.length; index += 1) {
      const prev = points[index - 1];
      const next = points[index];
      if (time <= next.time) {
        const span = next.time - prev.time || 1;
        const progress = ease((time - prev.time) / span);
        const base = prev.value + (next.value - prev.value) * progress;
        const minuteIndex = Math.floor(time.getTime() / 60000);
        const sourceStepMinutes = Math.max(60, span / 60000);
        const noise = seededNoise(minuteIndex, sourceStepMinutes >= 720 ? 0.018 : 0.012);
        return clamp01(base + noise);
      }
    }
    return points[points.length - 1].value;
  }

  function formatTime(date) {
    return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
  }

  function formatDateTime(date) {
    return `${String(date.getMonth() + 1).padStart(2, "0")}/${String(date.getDate()).padStart(2, "0")} ${formatTime(date)}`;
  }

  function percent(value) {
    return value === null ? "--%" : `${Math.round(value * 100)}%`;
  }

  async function fetchMetric(metricKey) {
    if (sourceCache[metricKey]) {
      return sourceCache[metricKey];
    }
    const metric = METRICS[metricKey];
    const response = await fetch(`${DATA_BASE}/${metric.source}?t=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Failed to load ${metric.source}`);
    }
    const payload = await response.json();
    sourceCache[metricKey] = collectSourcePoints(payload);
    return sourceCache[metricKey];
  }

  function buildSeries(points, nowMinute) {
    const shifted = normalizeTimeline(points, nowMinute);
    const rows = [];
    for (let offset = -WINDOW_MINUTES + 1; offset <= 0; offset += 1) {
      const time = addMinutes(nowMinute, offset);
      rows.push({
        time,
        value: valueAt(shifted, time)
      });
    }
    return rows;
  }

  function buildOption(rows, metric) {
    const values = rows.map(row => row.value);
    const currentIndex = values.length - 1;
    const fill = new echarts.graphic.LinearGradient(0, 0, 0, 1, [
      { offset: 0, color: "rgba(0, 0, 0, 0.075)" },
      { offset: 1, color: "rgba(0, 0, 0, 0.018)" }
    ]);

    return {
      backgroundColor: "transparent",
      animationDuration: 760,
      animationEasing: "cubicOut",
      grid: {
        top: 12,
        right: 0,
        bottom: 18,
        left: 68,
        containLabel: false
      },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(245,245,247,0.96)",
        borderColor: "rgba(0,0,0,0.18)",
        borderWidth: 1,
        extraCssText: "box-shadow:none;border-radius:6px;font-weight:700;",
        textStyle: { color: "#000000" },
        formatter(items) {
          const item = Array.isArray(items) ? items[0] : null;
          if (!item) {
            return "";
          }
          return `${formatDateTime(rows[item.dataIndex].time)}<br>${metric.label}: ${percent(item.data)}`;
        }
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: rows.map(row => formatTime(row.time)),
        axisLine: {
          show: true,
          lineStyle: { color: "rgba(0,0,0,0.18)", width: 1 }
        },
        axisTick: { show: false },
        axisLabel: {
          show: false,
          color: "#000000",
          fontSize: 11,
          fontWeight: 700,
          margin: 14
        },
        splitLine: {
          show: true,
          lineStyle: { color: "rgba(0,0,0,0.075)", width: 1 }
        }
      },
      yAxis: {
        type: "value",
        min: 0,
        max: 1,
        interval: 0.25,
        axisLine: {
          show: true,
          lineStyle: { color: "rgba(0,0,0,0.38)", width: 1 }
        },
        axisTick: { show: false },
        axisLabel: {
          color: "#000000",
          fontSize: 26,
          fontWeight: 800,
          margin: 16,
          formatter: value => `${Math.round(value * 100)}`
        },
        splitLine: {
          show: true,
          lineStyle: { color: "rgba(0,0,0,0.105)", width: 1 }
        },
        minorTick: {
          show: false
        },
        minorSplitLine: {
          show: true,
          lineStyle: { color: "rgba(0,0,0,0.052)", width: 1 }
        }
      },
      series: [
        {
          name: metric.label,
          type: "line",
          data: values,
          smooth: true,
          showSymbol: false,
          connectNulls: true,
          lineStyle: {
            width: 1.35,
            color: "rgba(0,0,0,0.58)"
          },
          areaStyle: {
            color: fill
          },
          markPoint: {
            symbol: "circle",
            symbolSize: 26,
            data: currentIndex >= 0 ? [{ coord: [currentIndex, values[currentIndex]] }] : [],
            itemStyle: {
              color: "#d7d7d7",
              borderColor: "#000000",
              borderWidth: 1.3
            },
            label: { show: false }
          },
          markLine: {
            silent: true,
            symbol: "none",
            data: currentIndex >= 0 ? [{ xAxis: currentIndex }] : [],
            lineStyle: {
              color: "#000000",
              type: "solid",
              width: 1.2
            },
            label: { show: false }
          },
          emphasis: { focus: "none" }
        }
      ]
    };
  }

  function updateMetricCards(currentValues) {
    Object.entries(METRICS).forEach(([key, metric]) => {
      const node = document.getElementById(metric.valueId);
      if (node) {
        node.textContent = percent(currentValues[key] ?? null);
      }
      const card = document.querySelector(`[data-metric-card="${key}"]`);
      if (card) {
        card.classList.toggle("is-active", key === activeMetric);
      }
    });
  }

  async function render() {
    const points = await fetchMetric(activeMetric);
    const metric = METRICS[activeMetric];
    const rows = buildSeries(points, displayMinute);
    const currentValues = {};

    await Promise.all(metricOrder.map(async key => {
      const metricPoints = await fetchMetric(key);
      const metricRows = buildSeries(metricPoints, displayMinute);
      currentValues[key] = metricRows[metricRows.length - 1]?.value ?? null;
    }));

    document.getElementById("screenTitle").textContent = metric.title;
    document.getElementById("screenSubtitle").textContent = `${metric.subtitle}，当前窗口${WINDOW_MINUTES}分钟`;
    document.getElementById("clockText").textContent = formatTime(displayMinute);
    updateMetricCards(currentValues);
    chart.setOption(buildOption(rows, metric), true);
  }

  function bindMetricSwitching() {
    document.querySelectorAll("[data-metric-card]").forEach(button => {
      button.addEventListener("click", () => {
        const nextMetric = button.dataset.metricCard;
        if (!METRICS[nextMetric] || nextMetric === activeMetric) {
          return;
        }
        activeMetric = nextMetric;
        lastCycleAt = Date.now();
        render().catch(error => console.warn(error));
      });
    });
  }

  function maybeCycleMetric() {
    if (!CYCLE_SECONDS) {
      return;
    }
    const now = Date.now();
    if (now - lastCycleAt < CYCLE_SECONDS * 1000) {
      return;
    }
    lastCycleAt = now;
    const currentIndex = metricOrder.indexOf(activeMetric);
    activeMetric = metricOrder[(currentIndex + 1) % metricOrder.length];
  }

  async function tick() {
    displayMinute = addMinutes(displayMinute, 1);
    maybeCycleMetric();
    try {
      await render();
    } catch (error) {
      console.warn(error);
    }
  }

  async function init() {
    chart = echarts.init(document.getElementById("liveChart"));
    bindMetricSwitching();
    await render();
    setInterval(tick, UPDATE_SECONDS * 1000);
    setInterval(() => {
      sourceCache = {};
    }, 5 * 60 * 1000);
    window.addEventListener("resize", () => chart.resize());
  }

  init();
})();
