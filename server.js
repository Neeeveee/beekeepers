const express = require("express");
const fs = require("fs");
const path = require("path");
const fetch = require("node-fetch");

const app = express();
const PORT = process.env.PORT || 3000;
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;
const DEEPSEEK_URL = "https://api.deepseek.com/chat/completions";
const STATIC_ROOT = __dirname;
const DATA_ROOT = path.join(__dirname, "data");
const DATA_RAW_ROOT = path.join(__dirname, "data_raw");

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

  if (!DEEPSEEK_API_KEY) {
    return res.json({ source: "fallback", scenarios: fallbackScenarios, analysisContext });
  }

  try {
    const scenarios = await generateScenariosWithDeepSeek(analysisContext);
    res.json({ source: "deepseek", scenarios, analysisContext });
  } catch (error) {
    console.error("[scenario] DeepSeek 生成失败，已回退到本地数据:\n", error);
    res.json({
      source: "fallback",
      scenarios: fallbackScenarios,
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
  const response = await fetch(DEEPSEEK_URL, {
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
    })
  });

  const data = await response.json().catch(() => ({}));

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
  const normalized = items.slice(0, 3).map((item, index) => ({
    id: ["scenario-a", "scenario-b", "scenario-c"][index],
    shortLabel: ["A", "B", "C"][index],
    title: safeString(item.title, `方案 ${index + 1}`),
    kicker: safeString(item.kicker, "Strategy"),
    summary: safeString(item.summary, "该方案用于当前授粉与蜂群场景的策略分析。"),
    sectionTitle: safeString(item.sectionTitle, "策略解读"),
    detailPoints: normalizeStringArray(item.detailPoints, 3, "建议补充该策略的关键说明。"),
    detailSections: normalizeDetailSections(item.detailSections),
    detailHighlights: normalizeHighlights(item.detailHighlights),
    metrics: normalizeMetrics(item.metrics)
  }));

  while (normalized.length < 3) {
    normalized.push(fallbackScenarios[normalized.length]);
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
