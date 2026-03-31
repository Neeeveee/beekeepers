const beeChart = echarts.init(document.getElementById("beeChart"));
const ecoChart = echarts.init(document.getElementById("ecoChart"));
const currentPie = echarts.init(document.getElementById("currentPie"));
const futurePie = echarts.init(document.getElementById("futurePie"));

const DEFAULT_DATA_MODE = "api";
const API_BASE_URL = "http://127.0.0.1:5000";
const STATIC_DATA_BASE_URL = "./data";
const PAST_DAYS_VISIBLE = 5;
const FUTURE_DAYS_VISIBLE = 7;

const CHART_COLORS = {
    history: "#bfbfbf",
    forecast: "#ffef3a",
    axisText: "#758094",
    gridLine: "#edf1f5",
    tooltipBorder: "#dbe0e8",
    tooltipBackground: "rgba(255,255,255,0.96)"
};

const ECO_METRICS = {
    flowering_overview: {
        source: "floweringOverview",
        title: "综合开花状态趋势（过去 5 天 + 未来 7 天）",
        yAxisName: "开花状态",
        legend: ["历史开花状态", "未来开花状态"],
        showSidePanel: true,
        hintId: "floweringHint",
        currentPieTitle: "当前开花主导植物",
        futurePieTitle: "未来开花主导植物"
    },
    nectar_supply_overview: {
        source: "nectarSupplyOverview",
        title: "综合蜜源供给强度（过去 5 天 + 未来 7 天）",
        yAxisName: "蜜源供给强度",
        legend: ["历史蜜源供给", "未来蜜源供给"],
        showSidePanel: true,
        hintId: "nectarHint",
        currentPieTitle: "当前主要供给植物",
        futurePieTitle: "未来主要供给植物"
    },
    mismatch_overview: {
        source: "mismatchOverview",
        title: "综合错配风险趋势（过去 5 天 + 未来 7 天）",
        yAxisName: "错配风险",
        legend: ["历史错配风险", "未来错配风险"],
        showSidePanel: false,
        hintId: "mismatchHint"
    }
};

let beeRawData = null;
let beeTimeScale = "hour";
let beeDailyMetric = "mean";
let currentEcoMetric = "flowering_overview";
let ecoRequestId = 0;

function getDataMode() {
    const params = new URLSearchParams(window.location.search);
    const mode = params.get("mode");
    if (mode === "api" || mode === "static") {
        return mode;
    }
    return DEFAULT_DATA_MODE;
}

const DATA_MODE = getDataMode();

function buildDataSourceMap() {
    if (DATA_MODE === "static") {
        return {
            beeActivity: `${STATIC_DATA_BASE_URL}/bee-activity-forecast.json`,
            floweringOverview: `${STATIC_DATA_BASE_URL}/flowering-overview.json`,
            nectarSupplyOverview: `${STATIC_DATA_BASE_URL}/nectar-supply-overview.json`,
            mismatchOverview: `${STATIC_DATA_BASE_URL}/mismatch-overview.json`
        };
    }

    return {
        beeActivity: `${API_BASE_URL}/api/bee-activity-forecast`,
        floweringOverview: `${API_BASE_URL}/api/flowering-overview`,
        nectarSupplyOverview: `${API_BASE_URL}/api/nectar-supply-overview`,
        mismatchOverview: `${API_BASE_URL}/api/mismatch-overview`
    };
}

const DATA_SOURCES = buildDataSourceMap();

function getEcoMetricConfig() {
    const config = ECO_METRICS[currentEcoMetric];
    return {
        ...config,
        source: DATA_SOURCES[config.source]
    };
}

function parseChartTime(value) {
    if (!value) {
        return null;
    }

    const normalized = value.length === 10 ? `${value}T00:00:00` : value.replace(" ", "T");
    const parsed = new Date(normalized);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function roundValue(value) {
    return Math.round(value * 10000) / 10000;
}

function addDays(date, days) {
    const next = new Date(date);
    next.setDate(next.getDate() + days);
    return next;
}

function getVisibleWindow() {
    const now = new Date();
    const start = addDays(now, -PAST_DAYS_VISIBLE);
    const end = addDays(now, FUTURE_DAYS_VISIBLE);
    start.setHours(0, 0, 0, 0);
    end.setHours(23, 59, 59, 999);
    return { start, end, now };
}

function filterSeriesByWindow(items, kind = "all") {
    const { start, end, now } = getVisibleWindow();

    return (items || []).filter(item => {
        const time = parseChartTime(item?.time);
        if (!time) {
            return false;
        }

        if (time < start || time > end) {
            return false;
        }

        if (kind === "actual" && time > now) {
            return false;
        }

        if (kind === "forecast" && time < start) {
            return false;
        }

        return true;
    });
}

function filterBridgeData(dataObj) {
    return {
        ...dataObj,
        actual: filterSeriesByWindow(dataObj.actual, "actual"),
        forecast: filterSeriesByWindow(dataObj.forecast, "forecast")
    };
}

function aggregateBeeDaily(items, metric) {
    const groups = {};

    (items || []).forEach(item => {
        if (!item || item.value == null || !item.time) {
            return;
        }

        const day = item.time.slice(0, 10);
        if (!groups[day]) {
            groups[day] = [];
        }
        groups[day].push(Number(item.value));
    });

    return Object.keys(groups).sort().map(day => {
        const values = groups[day];
        const aggregatedValue = metric === "peak"
            ? Math.max(...values)
            : values.reduce((sum, value) => sum + value, 0) / values.length;

        return {
            time: day,
            value: roundValue(aggregatedValue)
        };
    });
}

function dropOverlappingForecastDays(actualItems, forecastItems) {
    const actual = actualItems || [];
    const forecast = forecastItems || [];

    if (!actual.length) {
        return forecast;
    }

    const lastActualDay = actual[actual.length - 1].time;
    return forecast.filter(item => item.time > lastActualDay);
}

function buildBridgeSeries(dataObj, maxGapMs = 6 * 3600 * 1000) {
    const actual = (dataObj.actual || []).map(item => ({ ...item }));
    const forecast = (dataObj.forecast || []).map(item => ({ ...item }));
    const rawForecast = forecast.map(item => ({ ...item }));
    const lastActual = actual.length >= 1 ? actual[actual.length - 1] : null;
    const firstForecast = rawForecast.length >= 1 ? rawForecast[0] : null;
    const actualTime = parseChartTime(lastActual?.time);
    const forecastTime = parseChartTime(firstForecast?.time);
    const canTransition = actualTime && forecastTime
        && (forecastTime.getTime() - actualTime.getTime()) >= 0
        && (forecastTime.getTime() - actualTime.getTime()) <= maxGapMs;

    if (canTransition) {
        forecast.unshift({
            time: lastActual.time,
            value: lastActual.value
        });
    }

    const allTimes = [
        ...actual.map(item => item.time),
        ...forecast.map(item => item.time)
    ];

    const uniqueTimes = [...new Set(allTimes)].sort((a, b) => new Date(a) - new Date(b));

    const actualMap = {};
    actual.forEach(item => {
        actualMap[item.time] = item.value;
    });

    const forecastMap = {};
    forecast.forEach(item => {
        forecastMap[item.time] = item.value;
    });

    return {
        xData: uniqueTimes,
        actualSeries: uniqueTimes.map(time => (
            Object.prototype.hasOwnProperty.call(actualMap, time) ? actualMap[time] : null
        )),
        forecastSeries: uniqueTimes.map(time => (
            Object.prototype.hasOwnProperty.call(forecastMap, time) ? forecastMap[time] : null
        ))
    };
}

function formatDateLabel(value) {
    if (!value) {
        return "";
    }

    const text = String(value);
    if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
        return text.slice(5, 10).replace("-", "/");
    }
    return text;
}

function buildSharedLineOption(built, seriesNames) {
    return {
        animationDuration: 500,
        color: [CHART_COLORS.history, CHART_COLORS.forecast],
        title: { show: false },
        legend: { show: false },
        tooltip: {
            trigger: "axis",
            backgroundColor: CHART_COLORS.tooltipBackground,
            borderColor: CHART_COLORS.tooltipBorder,
            borderWidth: 1,
            textStyle: {
                color: "#292e38"
            },
            valueFormatter: value => (
                typeof value === "number" ? `${Math.round(value * 100)}%` : value
            )
        },
        grid: {
            top: 18,
            right: 28,
            bottom: 32,
            left: 44,
            containLabel: false
        },
        xAxis: {
            type: "category",
            boundaryGap: false,
            data: built.xData,
            axisLine: {
                show: false
            },
            axisTick: {
                show: false
            },
            axisLabel: {
                color: CHART_COLORS.axisText,
                fontSize: 11,
                margin: 16,
                formatter: value => formatDateLabel(value)
            },
            splitLine: {
                show: true,
                lineStyle: {
                    color: CHART_COLORS.gridLine,
                    width: 1
                }
            }
        },
        yAxis: {
            type: "value",
            min: 0,
            max: 1,
            interval: 0.25,
            axisLine: {
                show: false
            },
            axisTick: {
                show: false
            },
            axisLabel: {
                color: CHART_COLORS.axisText,
                fontSize: 11,
                formatter: value => `${Math.round(value * 100)}`
            },
            splitLine: {
                show: true,
                lineStyle: {
                    color: CHART_COLORS.gridLine,
                    width: 1
                }
            }
        },
        series: [
            {
                name: seriesNames[0],
                type: "line",
                data: built.actualSeries,
                smooth: true,
                connectNulls: false,
                showSymbol: false,
                symbol: "circle",
                symbolSize: 6,
                lineStyle: {
                    color: CHART_COLORS.history,
                    width: 2
                }
            },
            {
                name: seriesNames[1],
                type: "line",
                data: built.forecastSeries,
                smooth: true,
                connectNulls: false,
                showSymbol: false,
                symbol: "circle",
                symbolSize: 6,
                lineStyle: {
                    color: CHART_COLORS.forecast,
                    width: 2
                }
            }
        ]
    };
}

function updateBeeButtonState() {
    const hourBtn = document.getElementById("btn-bee-hour");
    const dayBtn = document.getElementById("btn-bee-day");
    const dayMeanBtn = document.getElementById("btn-bee-day-mean");
    const dayPeakBtn = document.getElementById("btn-bee-day-peak");

    hourBtn.classList.toggle("active", beeTimeScale === "hour");
    dayBtn.classList.toggle("active", beeTimeScale === "day");

    const showDailyButtons = beeTimeScale === "day";
    dayMeanBtn.style.display = showDailyButtons ? "inline-block" : "none";
    dayPeakBtn.style.display = showDailyButtons ? "inline-block" : "none";
    dayMeanBtn.classList.toggle("active", showDailyButtons && beeDailyMetric === "mean");
    dayPeakBtn.classList.toggle("active", showDailyButtons && beeDailyMetric === "peak");
}

function updateEcoButtonState() {
    const buttonIds = [
        "btn-flowering_overview",
        "btn-nectar_supply_overview",
        "btn-mismatch_overview"
    ];

    buttonIds.forEach(id => {
        const button = document.getElementById(id);
        if (button) {
            button.classList.remove("active");
        }
    });

    const activeButton = document.getElementById(`btn-${currentEcoMetric}`);
    if (activeButton) {
        activeButton.classList.add("active");
    }
}

function getBeeDisplaySeries() {
    if (!beeRawData) {
        return {
            built: { xData: [], actualSeries: [], forecastSeries: [] },
            seriesNames: ["历史活跃度", "预测活跃度"]
        };
    }

    const visibleBeeData = filterBridgeData(beeRawData);

    if (beeTimeScale === "day") {
        const dailyActual = aggregateBeeDaily(visibleBeeData.actual || [], beeDailyMetric);
        const dailyForecast = dropOverlappingForecastDays(
            dailyActual,
            aggregateBeeDaily(visibleBeeData.forecast || [], beeDailyMetric)
        );

        return {
            built: buildBridgeSeries({
                actual: dailyActual,
                forecast: dailyForecast
            }, 36 * 3600 * 1000),
            seriesNames: ["历史活跃度", "预测活跃度"]
        };
    }

    return {
        built: buildBridgeSeries(visibleBeeData),
        seriesNames: ["历史活跃度", "预测活跃度"]
    };
}

function renderBeeChart() {
    const { built, seriesNames } = getBeeDisplaySeries();
    beeChart.clear();
    beeChart.setOption(buildSharedLineOption(built, seriesNames), true);
    beeChart.resize();
}

function renderPie(chart, title, items) {
    const pieData = (items || []).map(item => ({
        name: item.plant_name,
        value: item.contribution_value ?? item.flowering_index ?? item.nectar_supply_index ?? 0
    }));

    chart.clear();
    chart.setOption({
        title: {
            text: title,
            left: "center",
            top: 5,
            textStyle: { fontSize: 14 }
        },
        tooltip: { trigger: "item" },
        legend: {
            bottom: 5,
            left: "center",
            itemWidth: 10,
            itemHeight: 10,
            textStyle: { fontSize: 12 }
        },
        series: [
            {
                name: title,
                type: "pie",
                radius: ["30%", "50%"],
                center: ["50%", "52%"],
                avoidLabelOverlap: true,
                label: { formatter: "{b}" },
                data: pieData
            }
        ]
    }, true);
}

async function fetchJson(source) {
    const response = await fetch(source, { cache: "no-store" });
    if (!response.ok) {
        throw new Error(`Failed to load ${source}: ${response.status}`);
    }
    return response.json();
}

async function loadBeeChart() {
    beeRawData = await fetchJson(DATA_SOURCES.beeActivity);
    updateBeeButtonState();
    renderBeeChart();
}

function setBeeTimeScale(scale) {
    beeTimeScale = scale;
    updateBeeButtonState();
    if (beeRawData) {
        renderBeeChart();
    }
}

function setBeeDailyMetric(metric) {
    beeDailyMetric = metric;
    updateBeeButtonState();
    if (beeRawData && beeTimeScale === "day") {
        renderBeeChart();
    }
}

function updateEcoLayout(metricConfig) {
    const ecoLayout = document.querySelector(".eco-layout");
    const ecoFlex = document.querySelector(".eco-flex");
    const floweringSidePanel = document.getElementById("floweringSidePanel");
    const floweringHint = document.getElementById("floweringHint");
    const nectarHint = document.getElementById("nectarHint");
    const mismatchHint = document.getElementById("mismatchHint");

    if (ecoLayout) {
        ecoLayout.classList.toggle("show-side-panel", metricConfig.showSidePanel);
    }
    if (ecoFlex) {
        ecoFlex.style.display = metricConfig.showSidePanel ? "block" : "none";
    }
    if (floweringSidePanel) {
        floweringSidePanel.style.display = metricConfig.showSidePanel ? "flex" : "none";
    }
    if (floweringHint) {
        floweringHint.style.display = metricConfig.hintId === "floweringHint" ? "block" : "none";
    }
    if (nectarHint) {
        nectarHint.style.display = metricConfig.hintId === "nectarHint" ? "block" : "none";
    }
    if (mismatchHint) {
        mismatchHint.style.display = metricConfig.hintId === "mismatchHint" ? "block" : "none";
    }
}

function renderEcoMetric(data, metricConfig) {
    const built = buildBridgeSeries(filterBridgeData(data), 36 * 3600 * 1000);
    ecoChart.clear();
    ecoChart.setOption(buildSharedLineOption(built, ["历史曲线", "预测曲线"]), true);
    ecoChart.resize();

    if (metricConfig.showSidePanel) {
        renderPie(currentPie, metricConfig.currentPieTitle, data.current_top || []);
        renderPie(futurePie, metricConfig.futurePieTitle, data.future_top || []);
        currentPie.resize();
        futurePie.resize();
    }
}

async function loadEcoChart() {
    const requestId = ++ecoRequestId;
    const metricConfig = getEcoMetricConfig();
    updateEcoLayout(metricConfig);
    const data = await fetchJson(metricConfig.source);
    if (requestId !== ecoRequestId) {
        return;
    }
    renderEcoMetric(data, metricConfig);
    updateEcoButtonState();
}

function setEcoMetric(metric) {
    currentEcoMetric = metric;
    updateEcoButtonState();
    loadEcoChart();
}

async function refreshCharts() {
    await loadBeeChart();
    await loadEcoChart();
}

window.setBeeTimeScale = setBeeTimeScale;
window.setBeeDailyMetric = setBeeDailyMetric;
window.setEcoMetric = setEcoMetric;

refreshCharts();
setInterval(loadBeeChart, 30000);
setInterval(loadEcoChart, 300000);

window.addEventListener("resize", function () {
    beeChart.resize();
    ecoChart.resize();
    currentPie.resize();
    futurePie.resize();
});
