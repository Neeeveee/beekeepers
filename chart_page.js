const beeChart = echarts.init(document.getElementById("beeChart"));
const ecoChart = echarts.init(document.getElementById("ecoChart"));
const currentPie = echarts.init(document.getElementById("currentPie"));
const futurePie = echarts.init(document.getElementById("futurePie"));

const DEFAULT_DATA_MODE = "api";
const API_BASE_URL = "http://127.0.0.1:5000";
const STATIC_DATA_BASE_URL = "./data";
const PAST_DAYS_VISIBLE = 5;
const FUTURE_DAYS_VISIBLE = 7;

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

const ECO_METRICS = {
    flowering_overview: {
        source: DATA_SOURCES.floweringOverview,
        title: "综合开花状态趋势（过去 5 天 + 未来 7 天）",
        yAxisName: "开花状态",
        legend: ["历史开花状态", "未来开花状态"],
        showSidePanel: true,
        hintId: "floweringHint",
        currentPieTitle: "当前开花主导植物",
        futurePieTitle: "未来开花主导植物"
    },
    nectar_supply_overview: {
        source: DATA_SOURCES.nectarSupplyOverview,
        title: "综合蜜源供给强度（过去 5 天 + 未来 7 天）",
        yAxisName: "蜜源供给强度",
        legend: ["历史蜜源供给", "未来蜜源供给"],
        showSidePanel: true,
        hintId: "nectarHint",
        currentPieTitle: "当前主要供给植物",
        futurePieTitle: "未来主要供给植物"
    },
    mismatch_overview: {
        source: DATA_SOURCES.mismatchOverview,
        title: "综合错配风险趋势（过去 5 天 + 未来 7 天）",
        yAxisName: "错配风险",
        legend: ["历史错配风险", "未来错配风险"],
        showSidePanel: false,
        hintId: "mismatchHint"
    }
};

let currentEcoMetric = "flowering_overview";
let beeTimeScale = "hour";
let beeDailyMetric = "mean";
let beeRawData = null;

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

function updateButtonState() {
    document.querySelectorAll(".button-group button").forEach(btn => btn.classList.remove("active"));

    let activeId = `btn-${currentEcoMetric}`;
    if (currentEcoMetric === "flowering_overview") {
        activeId = "btn-flowering_overview";
    } else if (currentEcoMetric === "nectar_supply_overview") {
        activeId = "btn-nectar_supply_overview";
    } else if (currentEcoMetric === "mismatch_overview") {
        activeId = "btn-mismatch_overview";
    }

    const activeButton = document.getElementById(activeId);
    if (activeButton) {
        activeButton.classList.add("active");
    }

    updateBeeButtonState();
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

    const actualSeries = uniqueTimes.map(time => (
        Object.prototype.hasOwnProperty.call(actualMap, time) ? actualMap[time] : null
    ));

    const forecastSeries = uniqueTimes.map(time => (
        Object.prototype.hasOwnProperty.call(forecastMap, time) ? forecastMap[time] : null
    ));

    return {
        xData: uniqueTimes,
        actualSeries,
        forecastSeries
    };
}

function buildTrendOption(title, yAxisName, legend, built) {
    return {
        title: { text: title },
        tooltip: { trigger: "axis" },
        legend: { data: legend },
        xAxis: {
            type: "category",
            data: built.xData,
            axisLabel: { rotate: 30 }
        },
        yAxis: {
            type: "value",
            name: yAxisName,
            min: 0,
            max: 1
        },
        series: [
            {
                name: legend[0],
                type: "line",
                data: built.actualSeries,
                smooth: true,
                connectNulls: false,
                smoothMonotone: "x"
            },
            {
                name: legend[1],
                type: "line",
                data: built.forecastSeries,
                smooth: true,
                connectNulls: false,
                lineStyle: { type: "dashed" },
                smoothMonotone: "x"
            }
        ]
    };
}

function getBeeDisplaySeries() {
    if (!beeRawData) {
        return {
            built: { xData: [], actualSeries: [], forecastSeries: [] },
            titleSuffix: "按小时"
        };
    }

    const visibleBeeData = filterBridgeData(beeRawData);

    if (beeTimeScale === "day") {
        const dailyActual = aggregateBeeDaily(visibleBeeData.actual || [], beeDailyMetric);
        const dailyForecast = dropOverlappingForecastDays(
            dailyActual,
            aggregateBeeDaily(visibleBeeData.forecast || [], beeDailyMetric)
        );

        const aggregated = {
            actual: dailyActual,
            forecast: dailyForecast
        };

        return {
            built: buildBridgeSeries(aggregated, 36 * 3600 * 1000),
            titleSuffix: beeDailyMetric === "peak" ? "按天（日峰值）" : "按天（日均值）"
        };
    }

    return {
        built: buildBridgeSeries(visibleBeeData),
        titleSuffix: "按小时"
    };
}

function renderBeeChart() {
    const { built, titleSuffix } = getBeeDisplaySeries();

    beeChart.clear();
    beeChart.setOption({
        title: { text: `蜜蜂活跃度曲线（${titleSuffix}，过去 5 天 + 未来 7 天）` },
        tooltip: { trigger: "axis" },
        legend: { data: ["历史实测", "未来预测"] },
        xAxis: {
            type: "category",
            data: built.xData,
            axisLabel: { rotate: 30 }
        },
        yAxis: {
            type: "value",
            name: "活跃度"
        },
        series: [
            {
                name: "历史实测",
                type: "line",
                data: built.actualSeries,
                smooth: true,
                connectNulls: false
            },
            {
                name: "未来预测",
                type: "line",
                data: built.forecastSeries,
                smooth: true,
                connectNulls: false,
                lineStyle: { type: "dashed" }
            }
        ]
    }, true);
}

function renderPie(chart, title, items) {
    const pieData = (items || []).map(item => ({
        name: item.plant_name,
        value: item.flowering_index ?? item.nectar_supply_index ?? 0
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
    const floweringSidePanel = document.getElementById("floweringSidePanel");
    const floweringHint = document.getElementById("floweringHint");
    const nectarHint = document.getElementById("nectarHint");
    const mismatchHint = document.getElementById("mismatchHint");
    const ecoFlexContainer = document.querySelector(".eco-flex");

    floweringSidePanel.style.display = metricConfig.showSidePanel ? "flex" : "none";
    ecoFlexContainer.style.gridTemplateColumns = metricConfig.showSidePanel ? "2fr 1fr" : "1fr";

    floweringHint.style.display = metricConfig.hintId === "floweringHint" ? "block" : "none";
    nectarHint.style.display = metricConfig.hintId === "nectarHint" ? "block" : "none";
    mismatchHint.style.display = metricConfig.hintId === "mismatchHint" ? "block" : "none";
}

function renderEcoMetric(data, metricConfig) {
    const built = buildBridgeSeries(filterBridgeData(data), 36 * 3600 * 1000);
    ecoChart.clear();
    ecoChart.setOption(
        buildTrendOption(metricConfig.title, metricConfig.yAxisName, metricConfig.legend, built),
        true
    );
    ecoChart.resize();

    if (metricConfig.showSidePanel) {
        renderPie(currentPie, metricConfig.currentPieTitle, data.current_top || []);
        renderPie(futurePie, metricConfig.futurePieTitle, data.future_top || []);
        currentPie.resize();
        futurePie.resize();
    }
}

async function loadEcoChart() {
    const metricConfig = ECO_METRICS[currentEcoMetric];
    updateEcoLayout(metricConfig);
    const data = await fetchJson(metricConfig.source);
    renderEcoMetric(data, metricConfig);
    updateButtonState();
}

function setEcoMetric(metric) {
    currentEcoMetric = metric;
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
