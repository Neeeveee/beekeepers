const express = require("express");
const fs = require("fs");
const path = require("path");
const fetch = require("node-fetch");
const { createContextualScenarios } = require("./scenario-strategy");

const app = express();
loadLocalEnv();
const PORT = process.env.PORT || 3000;
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;
const DEEPSEEK_URL = "https://api.deepseek.com/chat/completions";
const STATIC_ROOT = __dirname;
const DATA_ROOT = path.join(__dirname, "data");
const DATA_RAW_ROOT = path.join(__dirname, "data_raw");
const CACHE_ROOT = path.join(__dirname, ".cache");
const SCENARIO_CACHE_FILE = path.join(CACHE_ROOT, "scenarios.json");
const SCENARIO_CACHE_TTL_MS = 30 * 60 * 1000;
const DEEPSEEK_TIMEOUT_MS = 45000;
const SCENARIO_PROMPT_VERSION = "strategy-title-v2";
let scenarioRefreshPromise = null;

function loadLocalEnv() {
  const envPath = path.join(__dirname, ".env");
  if (!fs.existsSync(envPath)) {
    return;
  }

  const lines = fs.readFileSync(envPath, "utf8").split(/\r?\n/);
  lines.forEach(line => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) {
      return;
    }

    const separatorIndex = trimmed.indexOf("=");
    const key = trimmed.slice(0, separatorIndex).trim();
    const rawValue = trimmed.slice(separatorIndex + 1).trim();
    if (!key || process.env[key]) {
      return;
    }
    process.env[key] = rawValue.replace(/^["']|["']$/g, "");
  });
}

// ---- Current-data DeepSeek scenario overrides ----
// These definitions intentionally override the older mojibake prompt helpers below.
generateScenariosWithDeepSeek = async function(analysisContext) {
  const systemPrompt = [
    "你是一个农业授粉与蜂群管理策略设计助手。",
    "请严格基于系统提供的当前监测数据、预测数据和最新天气生成 4 个前端策略卡片方案。",
    "必须保留现有卡片框架：标题、摘要、策略说明、详情字段，以及 metrics 中的“匹配度 / 风险等级 / 建议动作”形式。",
    "只返回 JSON 对象，格式为 {\"scenarios\":[...]}，不要输出 Markdown，不要输出解释。",
    "scenarios 长度必须为 4，对应 scenario-a、scenario-b、scenario-c、scenario-d，shortLabel 分别为 A、B、C、D。",
    "detailSections 的 label 固定为：应对问题、建议类型、建议内容、具体操作、成本、效果、收益导向。",
    "detailHighlights 的 label 固定为：成本、效果、收益导向。",
    "metrics 必须是长度为 3 的数组，label 固定为：匹配度、风险等级、建议动作。",
    "匹配度用百分比表达，结合花期指数、蜜源指数、蜂群活跃度和错配风险估算；风险等级只能使用低、中、高或待评估。",
    "内容使用简体中文。不要写泛泛而谈的模板文案；每张卡至少引用一个当前数据事实。"
  ].join("\n");

  const userPrompt = [
    "项目背景：这是一个蜜蜂授粉、花期匹配、蜜源供给和生态错配分析界面。",
    "请输出 4 个彼此区分但保持原卡片格式的策略方案：稳态授粉、天气/活跃时段应对、错配恢复、未来资源窗口。",
    "",
    "当前监测分析结果：",
    formatAnalysisContextForPrompt(analysisContext)
  ].join("\n");

  const content = await requestDeepSeek([
    { role: "system", content: systemPrompt },
    { role: "user", content: userPrompt }
  ], {
    temperature: 0.65,
    max_tokens: 2600,
    response_format: { type: "json_object" }
  });

  return normalizeScenarioList(parseScenarioPayload(content), analysisContext);
};

normalizeScenarioList = function(items, analysisContext = buildAnalysisContext()) {
  const source = Array.isArray(items) ? items : [];
  const localFallback = createContextualScenarios(analysisContext, readLatestWeatherFile());
  const ids = ["scenario-a", "scenario-b", "scenario-c", "scenario-d"];
  const labels = ["A", "B", "C", "D"];

  return ids.map((id, index) => {
    const item = source[index] || localFallback[index] || {};
    return {
      id,
      shortLabel: labels[index],
      title: cleanString(item.title, localFallback[index]?.title || `方案 ${index + 1}`),
      kicker: cleanString(item.kicker, localFallback[index]?.kicker || "STRATEGY"),
      summary: cleanString(item.summary, localFallback[index]?.summary || "该方案基于当前监测数据生成。"),
      sectionTitle: cleanString(item.sectionTitle, "策略说明"),
      detailPoints: normalizeStringArray(item.detailPoints, 3, "请结合当前数据继续细化该策略。"),
      detailSections: normalizeDetailSections(item.detailSections, localFallback[index]?.detailSections),
      detailHighlights: normalizeHighlights(item.detailHighlights, localFallback[index]?.detailHighlights),
      metrics: normalizeMetrics(item.metrics, localFallback[index]?.metrics)
    };
  });
};

normalizeDetailSections = function(sections, fallbackSections = []) {
  const labels = ["应对问题", "建议类型", "建议内容", "具体操作", "成本", "效果", "收益导向"];
  const list = Array.isArray(sections) ? sections : [];
  return labels.map((label, index) => ({
    label,
    value: cleanString(list[index]?.value, fallbackSections[index]?.value || `待补充：${label}`)
  }));
};

normalizeHighlights = function(highlights, fallbackHighlights = []) {
  const labels = ["成本", "效果", "收益导向"];
  const list = Array.isArray(highlights) ? highlights : [];
  return labels.map((label, index) => ({
    label,
    value: cleanString(list[index]?.value, fallbackHighlights[index]?.value || "--"),
    note: cleanString(list[index]?.note, fallbackHighlights[index]?.note || "基于当前数据估算")
  }));
};

normalizeMetrics = function(metrics, fallbackMetrics = []) {
  const labels = ["匹配度", "风险等级", "建议动作"];
  const list = Array.isArray(metrics) ? metrics : [];
  return labels.map((label, index) => ({
    label,
    value: cleanString(list[index]?.value, fallbackMetrics[index]?.value || "--"),
    note: cleanString(list[index]?.note, fallbackMetrics[index]?.note || "基于当前数据生成")
  }));
};

writeScenarioCache = function(scenarios, analysisContext = buildAnalysisContext()) {
  try {
    fs.mkdirSync(CACHE_ROOT, { recursive: true });
    fs.writeFileSync(
      SCENARIO_CACHE_FILE,
      JSON.stringify({ cachedAt: new Date().toISOString(), dataSignature: analysisContext.dataSignature, scenarios }, null, 2),
      "utf8"
    );
  } catch (error) {
    console.warn("[scenario] 写入缓存失败:", error.message);
  }
};

readScenarioCache = function() {
  try {
    if (!fs.existsSync(SCENARIO_CACHE_FILE)) return null;
    const parsed = JSON.parse(fs.readFileSync(SCENARIO_CACHE_FILE, "utf8"));
    if (!Array.isArray(parsed?.scenarios) || !parsed.scenarios.length) return null;
    return {
      scenarios: normalizeScenarioList(parsed.scenarios),
      cachedAt: parsed.cachedAt || null,
      dataSignature: parsed.dataSignature || null
    };
  } catch (error) {
    console.warn("[scenario] 读取缓存失败:", error.message);
    return null;
  }
};

function isSameScenarioSignature(cachedScenarios, analysisContext) {
  return Boolean(
    cachedScenarios
    && cachedScenarios.dataSignature
    && cachedScenarios.dataSignature === analysisContext.dataSignature
  );
}

function refreshScenarioCacheInBackground(analysisContext) {
  if (!DEEPSEEK_API_KEY) return null;
  if (scenarioRefreshPromise) return scenarioRefreshPromise;

  scenarioRefreshPromise = (async () => {
    try {
      const scenarios = await generateScenariosWithDeepSeek(analysisContext);
      writeScenarioCache(scenarios, analysisContext);
      return scenarios;
    } catch (error) {
      console.error("[scenario] background DeepSeek refresh failed:\n", error);
      return null;
    } finally {
      scenarioRefreshPromise = null;
    }
  })();

  return scenarioRefreshPromise;
}

function scenarioFallbackPayload(analysisContext, source, warning) {
  return {
    source,
    scenarios: createContextualScenarios(analysisContext, readLatestWeatherFile()),
    analysisContext,
    warning
  };
}

buildAnalysisContext = function() {
  const floweringOverview = readJsonFile("flowering-overview.json");
  const nectarOverview = readJsonFile("nectar-supply-overview.json");
  const mismatchOverview = readJsonFile("mismatch-overview.json");
  const activityForecast = readJsonFile("bee-activity-forecast.json");
  const latestWeather = readLatestWeatherFile();

  const context = {
    generatedAt: new Date().toISOString(),
    flowering: summarizeDailySeries(floweringOverview, "flowering_index"),
    nectar: summarizeDailySeries(nectarOverview, "nectar_supply_index"),
    mismatch: summarizeMismatchOverview(mismatchOverview),
    activity: summarizeActivityForecast(activityForecast),
    weather: summarizeWeatherSnapshot(latestWeather)
  };

  context.snapshotDate = latestOf([
    context.flowering.latestActualDate,
    context.nectar.latestActualDate,
    context.mismatch.latestActualDate,
    context.activity.latestActualTime
  ]);
  context.forecastDate = latestOf([
    context.flowering.latestForecastDate,
    context.nectar.latestForecastDate,
    context.mismatch.latestForecastDate,
    context.activity.latestForecastTime
  ]);
  context.dataSignature = JSON.stringify({
    promptVersion: SCENARIO_PROMPT_VERSION,
    snapshotDate: context.snapshotDate,
    forecastDate: context.forecastDate,
    flowering: context.flowering,
    nectar: context.nectar,
    mismatch: context.mismatch,
    activity: context.activity,
    weather: context.weather
  });

  return context;
};

formatAnalysisContextForPrompt = function(context) {
  return [
    `数据快照日期：${context?.snapshotDate || "无日期"}；预测窗口：${context?.forecastDate || "无日期"}。`,
    `花期指数：最近实测 ${formatNumber(context?.flowering?.latestActualValue)}（${context?.flowering?.latestActualDate || "无日期"}），最近预测 ${formatNumber(context?.flowering?.latestForecastValue)}（${context?.flowering?.latestForecastDate || "无日期"}）。`,
    `主要花源：${context?.flowering?.topPlantName || "暂无"}，贡献值 ${formatNumber(context?.flowering?.topPlantContribution)}，对应花期指数 ${formatNumber(context?.flowering?.topPlantIndex)}。`,
    `花蜜量指数：最近实测 ${formatNumber(context?.nectar?.latestActualValue)}（${context?.nectar?.latestActualDate || "无日期"}），最近预测 ${formatNumber(context?.nectar?.latestForecastValue)}（${context?.nectar?.latestForecastDate || "无日期"}）。`,
    `主要蜜源来源：${context?.nectar?.topPlantName || "暂无"}，贡献值 ${formatNumber(context?.nectar?.topPlantContribution)}，对应花蜜量指数 ${formatNumber(context?.nectar?.topPlantIndex)}。`,
    `蜂群活跃度：最近实测 ${formatNumber(context?.activity?.latestActualValue)}（${context?.activity?.latestActualTime || "无日期"}），最近预测 ${formatNumber(context?.activity?.latestForecastValue)}（${context?.activity?.latestForecastTime || "无日期"}）。`,
    `错配风险：最近预测偏差 ${formatNumber(context?.mismatch?.latestForecastGap)}（${context?.mismatch?.latestForecastDate || "无日期"}），等级 ${context?.mismatch?.mismatchLevel || "暂无"}，类型 ${context?.mismatch?.mismatchType || "unknown"}。`,
    `最新天气：${context?.weather?.text || "暂无"}，温度 ${context?.weather?.temp ?? "--"}°C，降水概率 ${context?.weather?.pop ?? "--"}%，风速 ${context?.weather?.windSpeed ?? "--"}km/h。`
  ].join("\n");
};

deriveMismatchLevel = function(gap, rawLevel) {
  if (typeof rawLevel === "string" && rawLevel.trim()) return rawLevel.trim();
  if (typeof gap !== "number" || !Number.isFinite(gap)) return "暂无";
  if (gap < 0.15) return "基本匹配";
  if (gap < 0.3) return "轻度错配";
  if (gap < 0.5) return "中度错配";
  return "显著错配";
};

deriveMismatchType = function(rawType) {
  if (rawType === "resource_ahead") return "花源先行";
  if (rawType === "behavior_ahead") return "蜂群先行";
  if (rawType === "matched") return "基本匹配";
  if (rawType === "no_data") return "无数据";
  return rawType || "unknown";
};

function summarizeWeatherSnapshot(payload) {
  const latest = Array.isArray(payload?.hourly) ? payload.hourly[0] : null;
  return {
    text: latest?.text || null,
    temp: toSafeNumber(Number(latest?.temp)),
    pop: toSafeNumber(Number(latest?.pop)),
    windSpeed: toSafeNumber(Number(latest?.windSpeed)),
    sourceFile: payload?.sourceFile || null
  };
}

function latestOf(values) {
  const sorted = values.filter(Boolean).map((value) => String(value)).sort();
  return sorted.length ? sorted[sorted.length - 1] : null;
}

function formatNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? String(Number(value.toFixed(3))) : "暂无";
}

function cleanString(value, fallbackValue) {
  return typeof value === "string" && value.trim() ? value.trim() : fallbackValue;
}

// ---- Unified current-stage strategy basis ----
// All four strategy cards share this same factual basis; only the trade-off changes.
buildAnalysisContext = function() {
  const floweringOverview = readJsonFile("flowering-overview.json");
  const nectarOverview = readJsonFile("nectar-supply-overview.json");
  const mismatchOverview = readJsonFile("mismatch-overview.json");
  const activityForecast = readJsonFile("bee-activity-forecast.json");
  const latestWeather = readLatestWeatherFile();

  const context = {
    generatedAt: new Date().toISOString(),
    flowering: summarizeDailySeries(floweringOverview, "flowering_index"),
    nectar: summarizeDailySeries(nectarOverview, "nectar_supply_index"),
    mismatch: summarizeMismatchOverview(mismatchOverview),
    activity: summarizeActivityForecast(activityForecast),
    weather: summarizeWeatherSnapshot(latestWeather)
  };

  context.strategyBasis = buildStrategyBasis({
    floweringOverview,
    nectarOverview,
    mismatchOverview,
    activityForecast,
    weather: context.weather
  });
  context.snapshotDate = context.strategyBasis.date;
  context.forecastDate = context.strategyBasis.date;
  context.dataSignature = JSON.stringify({
    promptVersion: SCENARIO_PROMPT_VERSION,
    strategyBasis: context.strategyBasis,
    weather: context.weather
  });

  return context;
};

formatAnalysisContextForPrompt = function(context) {
  const basis = context?.strategyBasis || {};
  return [
    "统一事实底座（四张策略卡必须全部基于这一组数据，不允许各自换日期或换口径）：",
    `基准日期：${basis.date || "无日期"}。`,
    `主花源：${basis.topFlower || "暂无"}；主要蜜源：${basis.topNectar || "暂无"}。`,
    `花期指数：${formatNumber(basis.floweringValue)}；花蜜量指数：${formatNumber(basis.nectarValue)}。`,
    `蜂群核心活跃度：${formatNumber(basis.activityValue)}（${basis.activityWindow || "核心授粉时段"}，峰值 ${formatNumber(basis.activityPeak)}）。`,
    `错配风险：${formatNumber(basis.mismatchGap)}，等级 ${basis.mismatchLevel || "暂无"}，类型 ${basis.mismatchType || "unknown"}。`,
    `统一卡面匹配度：${basis.matchPercent || "--"}；统一卡面风险等级：${basis.riskLevel || "待评估"}。`,
    `最新天气：${context?.weather?.text || "暂无"}，温度 ${context?.weather?.temp ?? "--"}°C，降水概率 ${context?.weather?.pop ?? "--"}%，风速 ${context?.weather?.windSpeed ?? "--"}km/h。`,
    "",
    "生成要求：四张卡是同一当下阶段的四种解决方案，数据来源和诊断依据必须完全一致；差异只体现在成本、干预强度、见效速度、长期收益、生态稳健性等策略取向。"
  ].join("\n");
};

generateScenariosWithDeepSeek = async function(analysisContext) {
  const basis = analysisContext?.strategyBasis || {};
  const systemPrompt = [
    "你是一个农业授粉与蜂群管理策略设计助手。",
    "请基于同一个当前事实底座，生成 4 个可供用户自由选择的策略方案。",
    "四张卡不是四个不同诊断，而是同一真实当下情况的四种取向：低成本保守、天气/时段保护、短期增效、长期收益/生态韧性。",
    "必须保留现有卡片框架：title, kicker, summary, sectionTitle, detailPoints, detailSections, detailHighlights, metrics。",
    "title 必须是中文短标题，并且必须以“策略”结尾；不要使用“方案”“布局”“执行”“维持”作为标题结尾。",
    "四张 title 推荐分别体现：稳态守护策略、时段防护策略、短期增效策略、韧性提升策略。可以结合当前事实微调，但必须保持“XX策略”的命名形式。",
    "只返回 JSON 对象，格式为 {\"scenarios\":[...]}，不要输出 Markdown，不要解释。",
    "scenarios 长度必须为 4，对应 scenario-a、scenario-b、scenario-c、scenario-d，shortLabel 分别为 A、B、C、D。",
    "四张卡的 metrics 必须都是 3 项，label 固定为：匹配度、风险等级、建议动作。",
    `四张卡的“匹配度”必须统一使用 ${basis.matchPercent || "--"}，不要自行改写。`,
    `四张卡的“风险等级”必须统一使用 ${basis.riskLevel || "待评估"}，note 可解释不同策略对风险的处理方式。`,
    "detailSections 必须是 7 项，label 固定为：应对问题、建议类型、建议内容、具体操作、成本、效果、收益导向。",
    "detailHighlights 必须是 3 项，label 固定为：成本、效果、收益导向。",
    "每张卡都要引用当前事实底座中的至少两个事实，例如基准日期、主花源、花期指数、花蜜量、核心活跃度、错配风险或天气。",
    "不要写成泛泛模板，不要把其中某张卡改成未来末端预测诊断。"
  ].join("\n");

  const userPrompt = [
    "项目背景：这是一个蜜蜂授粉、花期匹配、蜜源供给和生态错配分析界面。",
    "请输出 4 个侧重不同但依据一致的解决方案：",
    "A. 稳态守护策略：少干预、低成本、保持当前效率。",
    "B. 时段防护策略：围绕降雨/温度/核心活跃时段安排操作。",
    "C. 短期增效策略：投入适中、提升近期授粉与采集效率。",
    "D. 韧性提升策略：更重视生态韧性、持续收益和未来窗口承接。",
    "",
    formatAnalysisContextForPrompt(analysisContext)
  ].join("\n");

  const content = await requestDeepSeek([
    { role: "system", content: systemPrompt },
    { role: "user", content: userPrompt }
  ], {
    temperature: 0.55,
    max_tokens: 2800,
    response_format: { type: "json_object" }
  });

  return normalizeScenarioList(parseScenarioPayload(content), analysisContext);
};

function buildStrategyBasis({ floweringOverview, nectarOverview, mismatchOverview, activityForecast, weather }) {
  const today = chinaTodayString();
  const basisDate = chooseStrategyDate(today, floweringOverview, nectarOverview, mismatchOverview, activityForecast);
  const floweringPoint = findDailyPoint(floweringOverview, basisDate);
  const nectarPoint = findDailyPoint(nectarOverview, basisDate);
  const mismatchPoint = findDailyPoint(mismatchOverview, basisDate);
  const mismatchInfo = findInfoPoint(mismatchOverview, basisDate);
  const activity = summarizeActivityForDate(activityForecast, basisDate);
  const topFlower = Array.isArray(floweringOverview?.current_top) ? floweringOverview.current_top[0] : null;
  const topNectar = Array.isArray(nectarOverview?.current_top) ? nectarOverview.current_top[0] : null;
  const matchPercent = estimateStrategyMatchPercent(
    floweringPoint?.value,
    nectarPoint?.value,
    activity.value,
    mismatchPoint?.value
  );
  const mismatchLevel = deriveMismatchLevel(mismatchPoint?.value ?? null, mismatchInfo?.mismatch_level);
  const mismatchType = deriveMismatchType(mismatchInfo?.mismatch_type);

  return {
    date: basisDate,
    floweringValue: floweringPoint?.value ?? null,
    nectarValue: nectarPoint?.value ?? null,
    activityValue: activity.value,
    activityPeak: activity.peak,
    activityWindow: activity.window,
    mismatchGap: mismatchPoint?.value ?? null,
    mismatchLevel,
    mismatchType,
    topFlower: topFlower?.plant_name || "暂无",
    topNectar: topNectar?.plant_name || topFlower?.plant_name || "暂无",
    matchPercent,
    riskLevel: strategyRiskLevel(mismatchPoint?.value, weather)
  };
}

function chinaTodayString() {
  return new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

function chooseStrategyDate(today, ...payloads) {
  const dates = [];
  payloads.forEach((payload) => {
    ["forecast", "actual"].forEach((key) => {
      (payload?.[key] || []).forEach((item) => {
        const day = dayPart(item?.time);
        if (day) dates.push(day);
      });
    });
  });
  const unique = [...new Set(dates)].sort();
  if (unique.includes(today)) return today;
  return unique.find((day) => day > today) || unique[unique.length - 1] || today;
}

function findDailyPoint(payload, date) {
  const exactForecast = (payload?.forecast || []).find((item) => dayPart(item?.time) === date && typeof item.value === "number");
  if (exactForecast) return exactForecast;
  const exactActual = (payload?.actual || []).find((item) => dayPart(item?.time) === date && typeof item.value === "number");
  if (exactActual) return exactActual;
  return getLatestPoint(payload?.forecast) || getLatestPoint(payload?.actual) || null;
}

function findInfoPoint(payload, date) {
  const exact = (payload?.forecast_info || []).find((item) => dayPart(item?.time) === date)
    || (payload?.history_info || []).find((item) => dayPart(item?.time) === date);
  return exact || getLatestInfoPoint(payload?.forecast_info) || getLatestInfoPoint(payload?.history_info) || null;
}

function summarizeActivityForDate(payload, date) {
  const rows = [...(payload?.forecast || []), ...(payload?.actual || [])]
    .filter((item) => dayPart(item?.time) === date && typeof item.value === "number");
  const coreRows = rows.filter((item) => {
    const hour = hourPart(item.time);
    return hour >= 10 && hour <= 15;
  });
  const chosen = coreRows.length >= 3 ? coreRows : rows;
  if (!chosen.length) {
    const latest = getLatestPoint(payload?.forecast) || getLatestPoint(payload?.actual);
    return { value: latest?.value ?? null, peak: latest?.value ?? null, window: latest?.time || "暂无" };
  }
  const values = chosen.map((item) => item.value);
  return {
    value: round3(values.reduce((sum, value) => sum + value, 0) / values.length),
    peak: round3(Math.max(...values)),
    window: coreRows.length >= 3 ? `${date} 10:00-15:00` : `${date} 可用时段`
  };
}

function dayPart(value) {
  return typeof value === "string" && value.length >= 10 ? value.slice(0, 10) : null;
}

function hourPart(value) {
  if (typeof value !== "string" || value.length < 13) return NaN;
  return Number(value.slice(11, 13));
}

function round3(value) {
  return typeof value === "number" && Number.isFinite(value) ? Number(value.toFixed(3)) : null;
}

function estimateStrategyMatchPercent(flowering, nectar, activity, mismatch) {
  const f = numericOrZero(flowering);
  const n = numericOrZero(nectar);
  const a = numericOrZero(activity);
  const m = numericOrZero(mismatch);
  const score = f * 0.28 + n * 0.24 + a * 0.28 + (1 - m) * 0.2;
  return `${Math.round(Math.max(0, Math.min(1, score)) * 100)}%`;
}

function strategyRiskLevel(mismatch, weather) {
  const value = numericOrZero(mismatch);
  const weatherStress = Number(weather?.pop) >= 50 || Number(weather?.windSpeed) >= 20 || Number(weather?.temp) >= 30;
  if (value >= 0.5) return "高";
  if (value >= 0.3 || weatherStress) return "中";
  return "低";
}

function numericOrZero(value) {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0;
}

// 兜底场景数据：当 DeepSeek 不可用时，页面仍然能正常展示。
const fallbackScenarios = [
  {
    id: "scenario-a",
    shortLabel: "A",
    title: "稳态授粉策略",
    kicker: "Pollination Outlook",
    summary: "当前花期与蜂群活跃时段较为匹配，建议以稳定监测和轻干预为主。",
    sectionTitle: "策略详情",
    detailPoints: [
      "授粉窗口与日间活动峰值重合度较高，当前风险较低。",
      "建议优先保持蜂箱稳定、减少不必要干扰，并持续观察温度与花蜜变化。",
      "若未来 48 小时天气稳定，可将该方案作为默认运行策略。"
    ],
    detailSections: [
      { label: "应对问题", value: "当前场地处于低风险稳态期，需要避免过度干预造成蜂群额外波动。" },
      { label: "建议类型", value: "战略性建议。适合作为当前阶段的默认运行方案。" },
      { label: "建议内容", value: "以稳定监测为主，维持现有蜂箱布置与巡查节奏，并持续记录授粉窗口变化。" },
      { label: "具体操作", value: "保持蜂箱位置不变；每日两次记录温湿度与蜂群活动；仅在异常天气出现时做轻度调整。" },
      { label: "成本", value: "低成本。主要投入为监测与日常巡查时间，不需要额外物资。" },
      { label: "效果", value: "有助于维持当前授粉效率，降低人为干预带来的扰动风险。" },
      { label: "收益导向", value: "以生态稳定和管理效率为主，同时兼顾基础授粉收益。" }
    ],
    detailHighlights: [
      { label: "成本", value: "低", note: "以巡查与记录为主" },
      { label: "效果", value: "稳态维持", note: "优先保持当前效率" },
      { label: "收益导向", value: "生态 / 管理", note: "强调稳定与低扰动" }
    ],
    metrics: [
      { label: "花蜂匹配", value: "82%", note: "匹配度较高" },
      { label: "风险等级", value: "低", note: "暂无明显错配风险" },
      { label: "建议动作", value: "监测", note: "持续跟踪并维持当前节奏" }
    ]
  },
  {
    id: "scenario-b",
    shortLabel: "B",
    title: "高温应对策略",
    kicker: "Heat Adjustment",
    summary: "高温可能压缩蜂群有效活动时段，需要提前做遮阴和补水预案。",
    sectionTitle: "策略详情",
    detailPoints: [
      "预计午间高温会抑制一部分采集活动，导致有效授粉时长缩短。",
      "建议加强场地补水、局部遮阴，并关注上午时段的工作强度。",
      "该方案适合用于短时天气压力下的快速运营调整。"
    ],
    detailSections: [
      { label: "应对问题", value: "高温导致蜂群在午间活动下降，授粉有效时段被压缩。" },
      { label: "建议类型", value: "临时性建议。适合应对短期天气压力与波动。" },
      { label: "建议内容", value: "缩短高温时段作业，优先把重点观察与辅助操作前移到上午时段。" },
      { label: "具体操作", value: "设置遮阴点；增加补水；调整巡查到清晨与傍晚；必要时减少午间开箱频率。" },
      { label: "成本", value: "中成本。需要增加临时遮阴、补水与人员调度。" },
      { label: "效果", value: "可以缓解高温造成的活动断崖，尽量保住关键授粉窗口。" },
      { label: "收益导向", value: "以经济收益保护为主，同时兼顾蜂群短期健康。" }
    ],
    detailHighlights: [
      { label: "成本", value: "中", note: "需要临时物资和调度" },
      { label: "效果", value: "热压缓解", note: "保住上午关键窗口" },
      { label: "收益导向", value: "经济 / 健康", note: "减少热损失带来的效率下降" }
    ],
    metrics: [
      { label: "花蜂匹配", value: "64%", note: "开始出现时间漂移" },
      { label: "风险等级", value: "中", note: "需提前调整作业节奏" },
      { label: "建议动作", value: "调整", note: "优先优化高温时段策略" }
    ]
  },
  {
    id: "scenario-c",
    shortLabel: "C",
    title: "错配恢复策略",
    kicker: "Recovery Strategy",
    summary: "花期与蜂群活动存在明显错配，建议进入恢复性决策模式。",
    sectionTitle: "策略详情",
    detailPoints: [
      "当前花源供给与蜂群活动峰值重叠不足，整体效率偏低。",
      "建议结合场地条件评估补充花源、调整蜂箱位置或优化作业时段。",
      "该方案更适合作为 AI 辅助决策时的重点讨论上下文。"
    ],
    detailSections: [
      { label: "应对问题", value: "花期与蜂群活动峰值错位，现有授粉效率不足，需要恢复性调整。" },
      { label: "建议类型", value: "战略性建议。适合进入恢复与重新配置阶段。" },
      { label: "建议内容", value: "围绕花源补充、蜂箱位置调整和作业时间重排，建立新的匹配关系。" },
      { label: "具体操作", value: "评估补充花源可能性；重新布置蜂箱与通道；按新授粉高峰重排巡查与作业时间。" },
      { label: "成本", value: "高成本。涉及场地调整、额外投入和更密集的管理协同。" },
      { label: "效果", value: "若执行得当，可逐步恢复花蜂匹配度并降低持续错配带来的损失。" },
      { label: "收益导向", value: "兼顾生态恢复与中长期经济回报，更偏向长期治理。" }
    ],
    detailHighlights: [
      { label: "成本", value: "高", note: "涉及重配置和额外投入" },
      { label: "效果", value: "恢复匹配", note: "面向中长期纠偏" },
      { label: "收益导向", value: "生态 / 经济", note: "强调长期恢复收益" }
    ],
    metrics: [
      { label: "花蜂匹配", value: "43%", note: "错配较明显" },
      { label: "风险等级", value: "高", note: "建议尽快采取恢复措施" },
      { label: "建议动作", value: "恢复", note: "优先讨论补救与替代方案" }
    ]
  },
  {
    id: "scenario-d",
    shortLabel: "D",
    title: "协同优化策略",
    kicker: "Collaborative Plan",
    summary: "当既有方案都不完全满意时，可把多种措施组合成可讨论的协同方案。",
    sectionTitle: "策略详情",
    detailPoints: [
      "整合稳态监测、高温应对和错配恢复的关键动作。",
      "适合多因素叠加、现场不确定性较高的管理情境。",
      "可作为进入自由 AI 对话前的开放式方案。"
    ],
    detailSections: [
      { label: "应对问题", value: "单一策略难以覆盖所有现场变化，需要更灵活的组合决策。" },
      { label: "建议类型", value: "组合型建议。适合继续与 AI 共同细化。" },
      { label: "建议内容", value: "把低成本监测、短期天气应对和中长期恢复动作组合为分阶段方案。" },
      { label: "具体操作", value: "先确认主要风险；再选择低成本动作；最后评估是否需要高投入恢复。" },
      { label: "成本", value: "可控成本。根据风险级别逐步投入。" },
      { label: "效果", value: "提升策略适配性和执行弹性，避免过度依赖单一路径。" },
      { label: "收益导向", value: "兼顾管理效率、生态稳定和经济回报。" }
    ],
    detailHighlights: [
      { label: "成本", value: "可控", note: "按风险分层投入" },
      { label: "效果", value: "弹性优化", note: "支持组合决策" },
      { label: "收益导向", value: "综合", note: "适合继续对话细化" }
    ],
    metrics: [
      { label: "花蜂匹配", value: "--", note: "需结合当前选择判断" },
      { label: "风险等级", value: "待评估", note: "适合 AI 继续分析" },
      { label: "建议动作", value: "协同", note: "进入组合讨论" }
    ]
  }
];

app.use(express.json({ limit: "1mb" }));

app.get("/", (req, res) => {
  res.sendFile(path.join(STATIC_ROOT, "dashboard1.html"));
});

app.use(express.static(STATIC_ROOT, { index: false }));

app.get("/api/health", (req, res) => {
  res.json({ ok: true, hasDeepSeekKey: Boolean(DEEPSEEK_API_KEY) });
});

app.get("/api/current-weather", (req, res) => {
  const latestWeather = readLatestWeatherFile();
  if (!latestWeather) {
    return res.status(404).json({ error: "Latest weather snapshot not found." });
  }

  res.set("Cache-Control", "no-store");
  res.json(latestWeather);
});

// 获取策略卡片数据。
app.get("/api/scenarios", async (req, res) => {
  const analysisContext = buildAnalysisContext();
  const cachedScenarios = readScenarioCache();

  if (!DEEPSEEK_API_KEY) {
    return res.json({
      source: "fallback",
      scenarios: createContextualScenarios(analysisContext, readLatestWeatherFile()),
      analysisContext,
      warning: "DeepSeek API key is unavailable, serving data-driven local scenarios."
    });
  }

  if (isSameScenarioSignature(cachedScenarios, analysisContext)) {
    const cacheExpired = isScenarioCacheExpired(cachedScenarios.cachedAt);
    if (cacheExpired) {
      refreshScenarioCacheInBackground(analysisContext);
    }

    return res.json({
      source: cacheExpired ? "cache-refreshing" : "cache",
      scenarios: cachedScenarios.scenarios,
      analysisContext,
      cachedAt: cachedScenarios.cachedAt,
      refreshing: cacheExpired,
      warning: cacheExpired ? "Serving cached AI scenarios while refreshing DeepSeek in the background." : undefined
    });
  }

  refreshScenarioCacheInBackground(analysisContext);
  return res.json(scenarioFallbackPayload(
    analysisContext,
    "fallback-refreshing",
    "Current data changed, serving data-driven local scenarios while DeepSeek refreshes in the background."
  ));

  try {
    const scenarios = await generateScenariosWithDeepSeek(analysisContext);
    writeScenarioCache(scenarios, analysisContext);
    res.json({ source: "deepseek", scenarios, analysisContext });
  } catch (error) {
    console.error("[scenario] DeepSeek 生成失败，已回退到本地数据:\n", error);

    if (
      cachedScenarios
      && cachedScenarios.dataSignature
      && cachedScenarios.dataSignature === analysisContext.dataSignature
      && !isScenarioCacheExpired(cachedScenarios.cachedAt)
    ) {
      return res.json({
        source: "cache",
        scenarios: cachedScenarios.scenarios,
        analysisContext,
        cachedAt: cachedScenarios.cachedAt,
        warning: "DeepSeek failed, serving the last cached AI scenarios."
      });
    }

    res.json({
      source: "fallback",
      scenarios: createContextualScenarios(analysisContext, readLatestWeatherFile()),
      analysisContext,
      warning: "DeepSeek 生成失败，已使用本地兜底方案。"
    });
  }
});

// 对话接口：前端将当前方案上下文和历史消息传过来，后端统一转发给 DeepSeek。
app.post("/api/chat", async (req, res) => {
  const { message, history = [], scenarioContext = null } = req.body || {};
  const analysisContext = buildAnalysisContext();

  if (!message || typeof message !== "string" || !message.trim()) {
    return res.status(400).json({ error: "消息不能为空。" });
  }

  if (!DEEPSEEK_API_KEY) {
    return res.status(500).json({ error: "服务器未配置 DEEPSEEK_API_KEY。" });
  }

  try {
    const reply = await chatWithDeepSeek({
      message: message.trim(),
      history,
      scenarioContext,
      analysisContext
    });

    res.json({ reply });
  } catch (error) {
    console.error("[chat] DeepSeek 对话失败:\n", error);
    res.status(500).json({ error: error.message || "DeepSeek 对话失败。" });
  }
});

app.listen(PORT, () => {
  console.log(`Scenario AI server is running at http://localhost:${PORT}`);
});

// 调用 DeepSeek 生成 scenario 卡片。
async function generateScenariosWithDeepSeek(analysisContext) {
  const systemPrompt = [
    "你是一个农业授粉与蜂群管理策略设计助手。",
    "请围绕蜜蜂活动、花期匹配、环境压力和策略建议，生成 3 个可用于前端卡片展示的方案。",
    "你必须只返回 JSON 数组，不要输出 Markdown，不要输出解释。",
    "每个对象必须包含以下字段：id, shortLabel, title, kicker, summary, sectionTitle, detailPoints, detailSections, detailHighlights, metrics。",
    "detailPoints 必须是长度为 3 的字符串数组。",
    "detailSections 必须是长度为 7 的数组，每项包含 label 和 value，对应字段为：应对问题、建议类型、建议内容、具体操作、成本、效果、收益导向。",
    "detailHighlights 必须是长度为 3 的数组，每项包含 label, value, note。",
    "metrics 必须是长度为 3 的数组，每项包含 label, value, note。",
    "id 必须分别使用 scenario-a, scenario-b, scenario-c。",
    "shortLabel 必须分别使用 A, B, C。",
    "内容语言请使用简体中文，kicker 可保留简短英文方向词。",
    "请优先依据提供的真实监测分析结果生成策略，不要脱离数据泛泛而谈。"
  ].join("\n");

  const userPrompt = [
    "项目背景：这是一个蜜蜂授粉与生态错配场景分析界面。",
    "请输出 3 个互相区分的策略方案：",
    "1. 稳态监测型",
    "2. 高温/天气压力应对型",
    "3. 错配恢复型",
    "",
    "当前监测分析结果：",
    formatAnalysisContextForPrompt(analysisContext)
  ].join("\n");

  const content = await requestDeepSeek([
    { role: "system", content: systemPrompt },
    { role: "user", content: userPrompt }
  ], {
    temperature: 0.9,
    max_tokens: 1400
  });

  const parsed = parseScenarioPayload(content);
  return normalizeScenarioList(parsed);
}

// 调用 DeepSeek 对话。
async function chatWithDeepSeek({ message, history, scenarioContext, analysisContext }) {
  const systemPrompt = [
    "你是一个面向蜜蜂授粉、花期匹配和生态错配分析的 AI 助手。",
    "回答要直接、专业、可执行，优先给出建议、原因和下一步行动。",
    "如果用户已经选中了策略方案，请把该方案当作当前对话上下文。",
    "除非用户要求，否则不要编造监测数据。",
    "回答时优先基于系统提供的花期、花蜜量、蜂群活跃性和错配风险结果进行判断。"
  ].join("\n");

  const messages = [{ role: "system", content: systemPrompt }];

  messages.push({
    role: "system",
    content: ["当前监测分析结果如下：", formatAnalysisContextForPrompt(analysisContext)].join("\n")
  });

  if (scenarioContext) {
    messages.push({
      role: "system",
      content: [
        "当前已锁定的策略上下文如下：",
        `标题：${scenarioContext.title}`,
        `方向：${scenarioContext.kicker}`,
        `摘要：${scenarioContext.summary}`,
        `要点：${(scenarioContext.detailPoints || []).join("；")}`,
        `详情字段：${(scenarioContext.detailSections || []).map((item) => `${item.label}=${item.value}`).join("；")}`,
        `摘要卡：${(scenarioContext.detailHighlights || []).map((item) => `${item.label}=${item.value}（${item.note}）`).join("；")}`,
        `指标：${(scenarioContext.metrics || []).map((item) => `${item.label}=${item.value}（${item.note}）`).join("；")}`
      ].join("\n")
    });
  }

  const safeHistory = Array.isArray(history)
    ? history
        .filter((item) => item && typeof item.role === "string" && typeof item.content === "string")
        .slice(-8)
    : [];

  messages.push(...safeHistory);
  messages.push({ role: "user", content: message });

  return requestDeepSeek(messages, {
    temperature: 0.7,
    max_tokens: 1200
  });
}

// 统一的 DeepSeek 请求函数。
async function requestDeepSeek(messages, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? DEEPSEEK_TIMEOUT_MS);
  let response;
  let data;

  try {
    response = await fetch(DEEPSEEK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${DEEPSEEK_API_KEY}`
      },
      body: JSON.stringify({
        model: "deepseek-chat",
        messages,
        temperature: options.temperature ?? 0.7,
        max_tokens: options.max_tokens ?? 1200,
        ...(options.response_format ? { response_format: options.response_format } : {})
      }),
      signal: controller.signal
    });

    data = await response.json().catch(() => ({}));
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error(`DeepSeek request timed out after ${options.timeoutMs ?? DEEPSEEK_TIMEOUT_MS}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    const errorMessage = data?.error?.message || `DeepSeek 请求失败，HTTP ${response.status}`;
    throw new Error(errorMessage);
  }

  const content = data?.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error("DeepSeek 返回内容为空。");
  }

  return String(content).trim();
}

// 解析 DeepSeek 返回的 JSON 内容。
function parseScenarioPayload(content) {
  const cleaned = stripCodeFence(content);
  const parsed = JSON.parse(cleaned);

  if (Array.isArray(parsed)) {
    return parsed;
  }

  if (Array.isArray(parsed.scenarios)) {
    return parsed.scenarios;
  }

  throw new Error("DeepSeek 返回的 scenario JSON 结构无效。");
}

// 标准化 scenario 列表，确保前端字段齐全。
function normalizeScenarioList(items) {
  const normalized = items.slice(0, 4).map((item, index) => ({
    id: ["scenario-a", "scenario-b", "scenario-c", "scenario-d"][index],
    shortLabel: ["A", "B", "C", "D"][index],
    title: safeString(item.title, `方案 ${index + 1}`),
    kicker: safeString(item.kicker, "Strategy"),
    summary: safeString(item.summary, "该方案用于当前授粉与蜂群场景的策略分析。"),
    sectionTitle: safeString(item.sectionTitle, "策略解读"),
    detailPoints: normalizeStringArray(item.detailPoints, 3, "建议补充该策略的关键说明。"),
    detailSections: normalizeDetailSections(item.detailSections),
    detailHighlights: normalizeHighlights(item.detailHighlights),
    metrics: normalizeMetrics(item.metrics)
  }));

  const localFallback = createContextualScenarios(buildAnalysisContext(), readLatestWeatherFile());
  while (normalized.length < 4) {
    normalized.push(localFallback[normalized.length]);
  }

  return normalized;
}

function normalizeDetailSections(sections) {
  const fallbackLabels = ["应对问题", "建议类型", "建议内容", "具体操作", "成本", "效果", "收益导向"];
  const list = Array.isArray(sections) ? sections : [];

  return fallbackLabels.map((label, index) => ({
    label,
    value: safeString(list[index]?.value, `待补充：${label}`)
  }));
}

function normalizeHighlights(highlights) {
  if (!Array.isArray(highlights) || highlights.length === 0) {
    return [
      { label: "成本", value: "--", note: "暂无数据" },
      { label: "效果", value: "--", note: "暂无数据" },
      { label: "收益导向", value: "--", note: "暂无数据" }
    ];
  }

  return highlights.slice(0, 3).map((item, index) => ({
    label: safeString(item?.label, ["成本", "效果", "收益导向"][index] || `摘要 ${index + 1}`),
    value: safeString(item?.value, "--"),
    note: safeString(item?.note, "暂无说明")
  }));
}

function normalizeMetrics(metrics) {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    return [
      { label: "指标 1", value: "--", note: "暂无数据" },
      { label: "指标 2", value: "--", note: "暂无数据" },
      { label: "指标 3", value: "--", note: "暂无数据" }
    ];
  }

  return metrics.slice(0, 3).map((item, index) => ({
    label: safeString(item?.label, `指标 ${index + 1}`),
    value: safeString(item?.value, "--"),
    note: safeString(item?.note, "暂无说明")
  }));
}

function normalizeStringArray(value, expectedLength, fallbackText) {
  const list = Array.isArray(value)
    ? value.filter((item) => typeof item === "string" && item.trim())
    : [];

  while (list.length < expectedLength) {
    list.push(fallbackText);
  }

  return list.slice(0, expectedLength);
}

function safeString(value, fallbackValue) {
  return typeof value === "string" && value.trim() ? value.trim() : fallbackValue;
}

function stripCodeFence(text) {
  return String(text)
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```$/i, "")
    .trim();
}

function readScenarioCache() {
  try {
    if (!fs.existsSync(SCENARIO_CACHE_FILE)) {
      return null;
    }

    const raw = fs.readFileSync(SCENARIO_CACHE_FILE, "utf8");
    const parsed = JSON.parse(raw);

    if (!Array.isArray(parsed?.scenarios) || !parsed.scenarios.length) {
      return null;
    }

    return {
      scenarios: normalizeScenarioList(parsed.scenarios),
      cachedAt: parsed.cachedAt || null
    };
  } catch (error) {
    console.warn("[scenario] 读取缓存失败:", error.message);
    return null;
  }
}

function writeScenarioCache(scenarios) {
  try {
    fs.mkdirSync(CACHE_ROOT, { recursive: true });
    fs.writeFileSync(
      SCENARIO_CACHE_FILE,
      JSON.stringify(
        {
          cachedAt: new Date().toISOString(),
          scenarios
        },
        null,
        2
      ),
      "utf8"
    );
  } catch (error) {
    console.warn("[scenario] 写入缓存失败:", error.message);
  }
}

function isScenarioCacheExpired(cachedAt) {
  if (!cachedAt) {
    return true;
  }

  const timestamp = new Date(cachedAt).getTime();
  if (Number.isNaN(timestamp)) {
    return true;
  }

  return Date.now() - timestamp > SCENARIO_CACHE_TTL_MS;
}

function buildAnalysisContext() {
  const floweringOverview = readJsonFile("flowering-overview.json");
  const nectarOverview = readJsonFile("nectar-supply-overview.json");
  const mismatchOverview = readJsonFile("mismatch-overview.json");
  const activityForecast = readJsonFile("bee-activity-forecast.json");

  return {
    generatedAt: new Date().toISOString(),
    flowering: summarizeDailySeries(floweringOverview, "flowering_index"),
    nectar: summarizeDailySeries(nectarOverview, "nectar_supply_index"),
    mismatch: summarizeMismatchOverview(mismatchOverview),
    activity: summarizeActivityForecast(activityForecast)
  };
}

function readJsonFile(fileName) {
  try {
    const filePath = path.join(DATA_ROOT, fileName);
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    console.warn(`[analysis] 读取 ${fileName} 失败:`, error.message);
    return null;
  }
}

function readLatestWeatherFile() {
  try {
    const candidates = fs
      .readdirSync(DATA_RAW_ROOT)
      .filter((fileName) => /^qweather_24h_\d{8}_\d{6}\.json$/.test(fileName))
      .sort()
      .reverse();

    for (const fileName of candidates) {
      const filePath = path.join(DATA_RAW_ROOT, fileName);
      const raw = fs.readFileSync(filePath, "utf8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed?.hourly) && parsed.hourly.length) {
        return {
          ...parsed,
          sourceFile: fileName
        };
      }
    }

    return null;
  } catch (error) {
    console.warn("[weather] 璇诲彇鏈€鏂板ぉ姘旀枃浠跺け璐:", error.message);
    return null;
  }
}

function summarizeDailySeries(payload, contributionKey) {
  const latestActual = getLatestPoint(payload?.actual);
  const latestForecast = getLatestPoint(payload?.forecast);
  const topItem = Array.isArray(payload?.current_top) ? payload.current_top[0] : null;

  return {
    latestActualDate: latestActual?.time || null,
    latestActualValue: latestActual?.value ?? null,
    latestForecastDate: latestForecast?.time || null,
    latestForecastValue: latestForecast?.value ?? null,
    topPlantName: safeString(topItem?.plant_name, "暂无"),
    topPlantContribution: toSafeNumber(topItem?.contribution_value),
    topPlantIndex: toSafeNumber(topItem?.[contributionKey])
  };
}

function summarizeMismatchOverview(payload) {
  const latestActual = getLatestPoint(payload?.actual);
  const latestForecast = getLatestPoint(payload?.forecast);
  const latestForecastInfo = getLatestInfoPoint(payload?.forecast_info);
  const normalizedGap = latestForecast?.value ?? null;

  return {
    latestActualDate: latestActual?.time || null,
    latestActualGap: latestActual?.value ?? null,
    latestForecastDate: latestForecast?.time || null,
    latestForecastGap: normalizedGap,
    mismatchLevel: deriveMismatchLevel(normalizedGap, latestForecastInfo?.mismatch_level),
    mismatchType: deriveMismatchType(latestForecastInfo?.mismatch_type)
  };
}

function summarizeActivityForecast(payload) {
  const latestActual = getLatestPoint(payload?.actual);
  const latestForecast = getLatestPoint(payload?.forecast);

  return {
    latestActualTime: latestActual?.time || null,
    latestActualValue: latestActual?.value ?? null,
    latestForecastTime: latestForecast?.time || null,
    latestForecastValue: latestForecast?.value ?? null
  };
}

function getLatestPoint(series) {
  if (!Array.isArray(series)) {
    return null;
  }

  const validItems = series.filter((item) => item && item.time && typeof item.value === "number");
  return validItems.length ? validItems[validItems.length - 1] : null;
}

function getLatestInfoPoint(series) {
  if (!Array.isArray(series)) {
    return null;
  }

  const validItems = series.filter((item) => item && item.time);
  return validItems.length ? validItems[validItems.length - 1] : null;
}

function toSafeNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function deriveMismatchLevel(gap, rawLevel) {
  if (typeof rawLevel === "string" && /匹配|错配|无数据/.test(rawLevel)) {
    return rawLevel;
  }

  if (typeof gap !== "number" || !Number.isFinite(gap)) {
    return "暂无";
  }

  if (gap < 0.15) {
    return "基本匹配";
  }

  if (gap < 0.3) {
    return "轻度错配";
  }

  if (gap < 0.5) {
    return "中度错配";
  }

  return "显著错配";
}

function deriveMismatchType(rawType) {
  if (rawType === "resource_ahead") {
    return "资源先行";
  }

  if (rawType === "behavior_ahead") {
    return "蜂群先行";
  }

  if (rawType === "matched") {
    return "基本匹配";
  }

  if (rawType === "no_data") {
    return "无数据";
  }

  return "unknown";
}

function formatAnalysisContextForPrompt(context) {
  return [
    `花期指数：最近实测 ${formatNullable(context?.flowering?.latestActualValue)}（${context?.flowering?.latestActualDate || "无日期"}），最近预测 ${formatNullable(context?.flowering?.latestForecastValue)}（${context?.flowering?.latestForecastDate || "无日期"}）。`,
    `主要花源：${context?.flowering?.topPlantName || "暂无"}，贡献值 ${formatNullable(context?.flowering?.topPlantContribution)}，对应花期指数 ${formatNullable(context?.flowering?.topPlantIndex)}。`,
    `花蜜量指数：最近实测 ${formatNullable(context?.nectar?.latestActualValue)}（${context?.nectar?.latestActualDate || "无日期"}），最近预测 ${formatNullable(context?.nectar?.latestForecastValue)}（${context?.nectar?.latestForecastDate || "无日期"}）。`,
    `主要花蜜来源：${context?.nectar?.topPlantName || "暂无"}，贡献值 ${formatNullable(context?.nectar?.topPlantContribution)}，对应花蜜量指数 ${formatNullable(context?.nectar?.topPlantIndex)}。`,
    `蜂群活跃度：最近实测 ${formatNullable(context?.activity?.latestActualValue)}（${context?.activity?.latestActualTime || "无日期"}），最近预测 ${formatNullable(context?.activity?.latestForecastValue)}（${context?.activity?.latestForecastTime || "无日期"}）。`,
    `错配风险：最近预测偏差 ${formatNullable(context?.mismatch?.latestForecastGap)}（${context?.mismatch?.latestForecastDate || "无日期"}），等级 ${context?.mismatch?.mismatchLevel || "暂无"}，类型 ${context?.mismatch?.mismatchType || "unknown"}。`
  ].join("\n");
}

function formatNullable(value) {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "暂无";
}
