(function () {
  const params = new URLSearchParams(window.location.search);
  const DATA_BASE = params.get("data") || "./data";
  const UPDATE_SECONDS = Math.max(1, Number(params.get("speed")) || 10);
  const WINDOW_MINUTES = Math.max(30, Number(params.get("window")) || 180);

  const METRICS = {
    flowering: {
      label: "开花状态",
      title: "生态时间序列",
      subtitle: "开花状态基于历史实际数据按分钟动态更新",
      source: "flowering-overview.json",
      valueId: "floweringValue",
      color: "#BDFF52",
      area: ["rgba(189, 255, 82, 0.18)", "rgba(189, 255, 82, 0.02)"]
    },
    nectar: {
      label: "蜜源供给强度",
      title: "生态时间序列",
      subtitle: "蜜源供给强度基于历史实际数据按分钟动态更新",
      source: "nectar-supply-overview.json",
      valueId: "nectarValue",
      color: "#797979",
      area: ["rgba(121, 121, 121, 0.12)", "rgba(121, 121, 121, 0.015)"]
    },
    mismatch: {
      label: "错配风险",
      title: "生态时间序列",
      subtitle: "生态错配风险基于历史实际数据按分钟动态更新",
      source: "mismatch-overview.json",
      valueId: "mismatchValue",
      color: "#FFE500",
      area: ["rgba(255, 229, 0, 0.16)", "rgba(255, 229, 0, 0.02)"]
    }
  };

  const metricOrder = Object.keys(METRICS);
  let sourceCache = {};
  let displayMinute = getInitialDisplayMinute();
  let chart = null;

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

  function buildOption(rowsByMetric) {
    const firstRows = rowsByMetric[metricOrder[0]] || [];
    const currentIndex = Math.max(0, firstRows.length - 1);

    return {
      backgroundColor: "transparent",
      animationDuration: 760,
      animationEasing: "cubicOut",
      grid: {
        top: 18,
        right: 28,
        bottom: 24,
        left: 76,
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
          const list = Array.isArray(items) ? items : [];
          if (!list.length) {
            return "";
          }
          const time = firstRows[list[0].dataIndex]?.time;
          const lines = list
            .map(item => `${item.marker}${item.seriesName}: ${percent(item.data)}`)
            .join("<br>");
          return `${formatDateTime(time)}<br>${lines}`;
        }
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: firstRows.map(row => formatTime(row.time)),
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
      series: metricOrder.map(key => {
        const metric = METRICS[key];
        const rows = rowsByMetric[key] || [];
        const values = rows.map(row => row.value);
        const fill = new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: metric.area[0] },
          { offset: 1, color: metric.area[1] }
        ]);

        return {
          name: metric.label,
          type: "line",
          data: values,
          smooth: true,
          clip: false,
          showSymbol: false,
          connectNulls: true,
          lineStyle: {
            width: 1.25,
            color: metric.color
          },
          areaStyle: {
            color: fill
          },
          markPoint: {
            symbol: "circle",
            symbolSize: 26,
            data: currentIndex >= 0 ? [{ coord: [currentIndex, values[currentIndex]] }] : [],
            itemStyle: {
              color: metric.color,
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
        };
      })
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
        card.classList.add("is-active");
      }
    });
  }

  async function render() {
    const currentValues = {};
    const rowsByMetric = {};

    await Promise.all(metricOrder.map(async key => {
      const metricPoints = await fetchMetric(key);
      const metricRows = buildSeries(metricPoints, displayMinute);
      rowsByMetric[key] = metricRows;
      currentValues[key] = metricRows[metricRows.length - 1]?.value ?? null;
    }));

    document.getElementById("screenTitle").textContent = "生态时间序列";
    document.getElementById("screenSubtitle").textContent = `开花状态、蜜源供给强度、错配风险基于历史实际数据按分钟动态更新，当前窗口${WINDOW_MINUTES}分钟`;
    document.getElementById("clockText").textContent = formatTime(displayMinute);
    updateMetricCards(currentValues);
    chart.setOption(buildOption(rowsByMetric), true);
  }

  async function tick() {
    displayMinute = getInitialDisplayMinute();
    sourceCache = {};
    try {
      await render();
    } catch (error) {
      console.warn(error);
    }
  }

  async function init() {
    chart = echarts.init(document.getElementById("liveChart"));
    await render();
    setInterval(tick, UPDATE_SECONDS * 1000);
    window.addEventListener("resize", () => chart.resize());
  }

  init();
})();
