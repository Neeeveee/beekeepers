const DEFAULT_SCENARIOS = [
  {
    id: "scenario-a",
    shortLabel: "A",
    title: "稳态授粉策略",
    kicker: "POLLINATION OUTLOOK",
    summary: "当前花期与蜂群活跃较匹配，建议稳定监测。",
    sectionTitle: "策略说明",
    detailSections: [
      { label: "应对问题", value: "当前处于低风险稳态期，重点是保持系统稳定。" },
      { label: "建议类型", value: "稳态维持" },
      { label: "建议内容", value: "保持蜂箱位置与巡检节奏，观察温湿度和花期波动。" },
      { label: "具体操作", value: "保持蜂箱位置不变；每日早晚巡查一次；异常天气时微调蜂箱通风。" }
    ],
    detailHighlights: [
      { label: "成本", value: "低", note: "以日常巡检为主" },
      { label: "效果", value: "稳态维持", note: "优先保持当前效率" },
      { label: "收益导向", value: "生态/管理", note: "降低扰动风险" }
    ],
    metrics: [
      { label: "匹配度", value: "82%" },
      { label: "风险等级", value: "低" }
    ]
  },
  {
    id: "scenario-b",
    shortLabel: "B",
    title: "高温应对策略",
    kicker: "HEAT ADJUSTMENT",
    summary: "高温会压缩蜂群有效活动时段，需要遮阴和补水。",
    sectionTitle: "策略说明",
    detailSections: [
      { label: "应对问题", value: "高温导致午间活跃下降，授粉窗口被压缩。" },
      { label: "建议类型", value: "短期调整" },
      { label: "建议内容", value: "将关键作业前移到上午，午后加强降温补水。" },
      { label: "具体操作", value: "设置临时遮阴点；增加场地补水；改为清晨与傍晚巡查。" }
    ],
    detailHighlights: [
      { label: "成本", value: "中", note: "需要临时物资" },
      { label: "效果", value: "热压缓解", note: "保住有效作业时段" },
      { label: "收益导向", value: "经济/健康", note: "减少热损失" }
    ],
    metrics: [
      { label: "匹配度", value: "64%" },
      { label: "风险等级", value: "中" }
    ]
  },
  {
    id: "scenario-c",
    shortLabel: "C",
    title: "错配恢复策略",
    kicker: "RECOVERY STRATEGY",
    summary: "花期与蜂群活动存在明显错配，建议进入恢复模式。",
    sectionTitle: "策略说明",
    detailSections: [
      { label: "应对问题", value: "花期与蜂群活动峰值错位，整体效率偏低。" },
      { label: "建议类型", value: "恢复纠偏" },
      { label: "建议内容", value: "围绕花源补充与作业重排，建立新的时间匹配。" },
      { label: "具体操作", value: "评估补充花源；重排巡检时段；调整蜂箱布局。" }
    ],
    detailHighlights: [
      { label: "成本", value: "高", note: "涉及重配置" },
      { label: "效果", value: "恢复匹配", note: "中长期改善" },
      { label: "收益导向", value: "生态/经济", note: "长期回报导向" }
    ],
    metrics: [
      { label: "匹配度", value: "43%" },
      { label: "风险等级", value: "高" }
    ]
  },
  {
    id: "scenario-d",
    shortLabel: "D",
    title: "夜间保守策略",
    kicker: "NIGHT CONSERVATION",
    summary: "夜间与阴雨窗口建议减少扰动，保护蜂群状态。",
    sectionTitle: "策略说明",
    detailSections: [
      { label: "应对问题", value: "连续阴雨与低温阶段，蜂群状态脆弱。" },
      { label: "建议类型", value: "保守防护" },
      { label: "建议内容", value: "减少开箱频率，集中做外围环境保障。" },
      { label: "具体操作", value: "降低夜间干预；加强保温防潮；次日再进行重点检查。" }
    ],
    detailHighlights: [
      { label: "成本", value: "低", note: "管理动作为主" },
      { label: "效果", value: "稳定状态", note: "避免过度干预" },
      { label: "收益导向", value: "稳态优先", note: "先保健康再提效" }
    ],
    metrics: [
      { label: "匹配度", value: "71%" },
      { label: "风险等级", value: "低" }
    ]
  }
];

const FROZEN_AI_SCENARIOS = [
  {
    id: "scenario-a",
    shortLabel: "A",
    title: "稳态守护策略",
    kicker: "低成本保守",
    summary: "基于当前冬青花期指数0.818与花蜜量0.763，蜂群核心活跃度0.577，错配风险0.476（基本匹配），建议维持现有管理，减少干预，以最低成本保持当前授粉效率。",
    sectionTitle: "策略详情",
    detailPoints: [
      "当前冬青花期指数0.818，花蜜量0.763，蜂群活跃度0.577，错配风险低，无需额外干预。",
      "天气多云，温度30°C，降水概率0%，风速10km/h，适合蜂群正常采集。",
      "保持蜂群自然状态，仅进行常规检查，避免打扰。"
    ],
    detailSections: [
      { label: "应对问题", value: "当前授粉匹配良好，无需应对显著问题，主要维持现状。" },
      { label: "建议类型", value: "保守型管理" },
      { label: "建议内容", value: "不增加额外投入，仅执行常规蜂群检查与病虫害监控。" },
      { label: "具体操作", value: "每日10:00-15:00观察蜂群出勤，记录活跃度变化；无需补充饲喂或调整蜂箱。" },
      { label: "成本", value: "极低，仅需人工观察时间。" },
      { label: "效果", value: "维持当前授粉效率，预计冬青授粉覆盖率保持在68%左右。" },
      { label: "收益导向", value: "短期稳定，长期依赖自然条件。" }
    ],
    detailHighlights: [
      { label: "成本", value: "极低", note: "按当前场地条件估算" },
      { label: "效果", value: "维持现状", note: "围绕当下事实底座设计" },
      { label: "收益导向", value: "短期稳定", note: "用于选择不同管理取向" }
    ],
    metrics: [
      { label: "匹配度", value: "68%", note: "基于 2026-05-13 同一组花期、蜜源、活跃度和错配数据" },
      { label: "风险等级", value: "中", note: "风险可控，但若天气突变可能影响效率。" },
      { label: "建议动作", value: "持续监测，暂不干预。", note: "主花源：冬青" }
    ]
  },
  {
    id: "scenario-b",
    shortLabel: "B",
    title: "时段防护策略",
    kicker: "天气/时段保护",
    summary: "针对当前30°C高温与10km/h风速，结合蜂群活跃度峰值在10:00-15:00（峰值0.815），建议在核心时段加强保护，减少高温与风对采集的影响。",
    sectionTitle: "策略详情",
    detailPoints: [
      "当前温度30°C，风速10km/h，可能影响蜂群采集效率。",
      "蜂群核心活跃度0.577，峰值0.815出现在10:00-15:00。",
      "冬青花期指数0.818，花蜜量0.763，需确保采集窗口充分利用。"
    ],
    detailSections: [
      { label: "应对问题", value: "高温与风可能降低蜂群采集效率，需在核心时段提供保护。" },
      { label: "建议类型", value: "时段性防护" },
      { label: "建议内容", value: "在10:00-15:00核心时段，为蜂箱提供遮荫或防风屏障。" },
      { label: "具体操作", value: "在蜂箱上方搭建遮阳网，或设置挡风板；确保水源充足。" },
      { label: "成本", value: "低，遮阳网或挡风板材料费约50元。" },
      { label: "效果", value: "预计提升核心时段采集效率10-15%。" },
      { label: "收益导向", value: "短期效率提升，保护蜂群健康。" }
    ],
    detailHighlights: [
      { label: "成本", value: "低", note: "按当前场地条件估算" },
      { label: "效果", value: "提升核心时段效率", note: "围绕当下事实底座设计" },
      { label: "收益导向", value: "短期效率与健康", note: "用于选择不同管理取向" }
    ],
    metrics: [
      { label: "匹配度", value: "68%", note: "基于 2026-05-13 同一组花期、蜜源、活跃度和错配数据" },
      { label: "风险等级", value: "中", note: "防护措施降低天气风险，但需及时调整。" },
      { label: "建议动作", value: "立即实施遮荫与防风。", note: "主花源：冬青" }
    ]
  },
  {
    id: "scenario-c",
    shortLabel: "C",
    title: "短期增效策略",
    kicker: "短期增效",
    summary: "利用当前冬青花蜜量0.763与蜂群活跃度0.577，通过补充饲喂与蜂群激励，短期内提升授粉与采集效率，投入适中。",
    sectionTitle: "策略详情",
    detailPoints: [
      "冬花花蜜量0.763，仍有提升空间；蜂群活跃度0.577可被激励。",
      "天气多云，30°C，无降水，适合进行补充饲喂。",
      "错配风险0.476，基本匹配，短期增效可进一步优化匹配。"
    ],
    detailSections: [
      { label: "应对问题", value: "蜂群活跃度未达峰值，可通过激励提升采集效率。" },
      { label: "建议类型", value: "激励型管理" },
      { label: "建议内容", value: "补充糖浆或花粉替代物，刺激蜂群出勤。" },
      { label: "具体操作", value: "每日清晨在蜂箱内放置1:1糖浆饲喂器，连续3天。" },
      { label: "成本", value: "中等，糖浆成本约100元。" },
      { label: "效果", value: "预计提升活跃度15%，授粉效率提高10%。" },
      { label: "收益导向", value: "短期增产，快速见效。" }
    ],
    detailHighlights: [
      { label: "成本", value: "中等", note: "按当前场地条件估算" },
      { label: "效果", value: "提升活跃度与授粉", note: "围绕当下事实底座设计" },
      { label: "收益导向", value: "短期增产", note: "用于选择不同管理取向" }
    ],
    metrics: [
      { label: "匹配度", value: "68%", note: "基于 2026-05-13 同一组花期、蜜源、活跃度和错配数据" },
      { label: "风险等级", value: "中", note: "补充饲喂可能引起盗蜂，需谨慎操作。" },
      { label: "建议动作", value: "执行补充饲喂，监控蜂群反应。", note: "主花源：冬青" }
    ]
  },
  {
    id: "scenario-d",
    shortLabel: "D",
    title: "韧性提升策略",
    kicker: "长期收益/生态韧性",
    summary: "基于当前冬青花期与蜂群状态，通过种植辅助蜜源与优化蜂箱布局，提升生态韧性，确保未来窗口承接与长期收益。",
    sectionTitle: "策略详情",
    detailPoints: [
      "当前冬青花期指数0.818，花蜜量0.763，但单一蜜源存在风险。",
      "蜂群核心活跃度0.577，错配风险0.476，需增强系统韧性。",
      "天气适宜，可进行辅助蜜源种植或蜂箱迁移准备。"
    ],
    detailSections: [
      { label: "应对问题", value: "单一蜜源依赖，未来窗口可能错配，需提升生态韧性。" },
      { label: "建议类型", value: "生态优化" },
      { label: "建议内容", value: "在周边种植早春或晚秋蜜源植物，并调整蜂箱位置以利用微气候。" },
      { label: "具体操作", value: "选择紫云英或向日葵种子，在非冬青区域播种；将部分蜂箱移至向阳坡地。" },
      { label: "成本", value: "较高，种子与人工约300元。" },
      { label: "效果", value: "长期提升蜜源多样性，减少错配风险。" },
      { label: "收益导向", value: "长期生态韧性，可持续收益。" }
    ],
    detailHighlights: [
      { label: "成本", value: "较高", note: "按当前场地条件估算" },
      { label: "效果", value: "提升生态韧性", note: "围绕当下事实底座设计" },
      { label: "收益导向", value: "长期可持续", note: "用于选择不同管理取向" }
    ],
    metrics: [
      { label: "匹配度", value: "68%", note: "基于 2026-05-13 同一组花期、蜜源、活跃度和错配数据" },
      { label: "风险等级", value: "中", note: "长期投资，短期效果不明显，但降低未来风险。" },
      { label: "建议动作", value: "规划种植与蜂箱调整。", note: "主花源：冬青" }
    ]
  }
];

const $ = (id) => document.getElementById(id);
const stage = $("carouselStage");
const backButton = $("backDashboardButton");
const freeChatButton = $("freeChatButton");
const scenarioOverlay = document.querySelector(".scenario-overlay");
const chatShell = document.querySelector(".chat-shell");
const chatStage = document.querySelector(".chat-stage");
const chatContextPanel = $("chatContextPanel");
const chatContextKicker = $("chatContextKicker");
const chatContextTitle = $("chatContextTitle");
const chatContextSummary = $("chatContextSummary");
const chatContextPoints = $("chatContextPoints");
const chatContextClear = $("chatContextClear");
const chatComposer = $("chatComposer");
const composerTip = $("composerTip");
const sendButton = document.querySelector(".send-placeholder");
const detailOverlay = $("detailOverlay");
const detailBackdrop = $("detailBackdrop");
const detailClose = $("detailCloseButton");
const detailKicker = $("detailKicker");
const detailTitle = $("detailTitle");
const detailSummary = $("detailSummary");
const detailSectionTitle = $("detailSectionTitle");
const detailCopy = $("detailCopy");
const detailMetrics = $("detailMetrics");
const detailPrimary = $("detailPrimaryAction");
const detailModal = document.querySelector(".detail-modal");
const corridorPanel = $("corridorPanel");
const corridorButton = $("detailCorridorButton");
const corridorBackButton = $("corridorBackButton");
const corridorCloseButton = $("corridorCloseButton");
const siteInfoToggle = $("siteInfoToggle");
const siteDetailCard = $("siteDetailCard");
const infoDate = $("infoDate");
const infoTime = $("infoTime");
const infoActivity = $("infoActivity");
const infoNectar = $("infoNectar");
const infoMismatch = $("infoMismatch");
const siteDateRange = $("siteDateRange");

const CARD_IMAGE_COUNT = 4;

const GAUGE_DATA_BASE_URL = "https://neeeveee.github.io/beekeepers/data";
const BEE_CORE_START_HOUR = 10;
const BEE_CORE_END_HOUR = 15;
const MIN_CORE_DAY_SAMPLES = 3;
const MIN_FALLBACK_DAY_SAMPLES = 6;

const state = {
  scenarios: [...FROZEN_AI_SCENARIOS],
  selectedIndex: 0,
  activeScenario: null,
  messages: [],
  isSending: false,
  source: "frozen",
  analysisContext: null
};

const SCENARIO_CACHE_KEY = "beeScenarioCardsCache:v2";

function fmtDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function fmtTime(d) {
  const h = String(d.getHours()).padStart(2, "0");
  const m = String(d.getMinutes()).padStart(2, "0");
  return `${h}:${m}`;
}

function parseGaugeTime(value) {
  if (!value) return null;
  const normalized = value.length === 10 ? `${value}T00:00:00` : value.replace(" ", "T");
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function updateClock() {
  if (!infoDate || !infoTime) return;
  const now = new Date();
  infoDate.textContent = fmtDate(now);
  infoTime.textContent = fmtTime(now);
}

function aggregateBeeDaily(items, metric) {
  const groups = {};
  (items || []).forEach(item => {
    if (!item || item.value == null || !item.time) return;
    const day = item.time.slice(0, 10);
    if (!groups[day]) groups[day] = [];
    const hour = Number(item.time.slice(11, 13));
    groups[day].push({
      value: Number(item.value),
      hour: Number.isFinite(hour) ? hour : null
    });
  });

  return Object.keys(groups).sort().map(day => {
    const entries = groups[day].filter(entry => Number.isFinite(entry.value));
    const coreEntries = entries.filter(entry => (
      entry.hour != null
      && entry.hour >= BEE_CORE_START_HOUR
      && entry.hour <= BEE_CORE_END_HOUR
    ));
    const metricEntries = coreEntries.length >= MIN_CORE_DAY_SAMPLES
      ? coreEntries
      : entries.length >= MIN_FALLBACK_DAY_SAMPLES
        ? entries
        : [];

    if (!metricEntries.length) return null;
    const values = metricEntries.map(entry => entry.value);
    if (metric === "peak") {
      return { time: day, value: Math.max(...values) };
    }
    const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
    return { time: day, value: mean };
  }).filter(Boolean);
}

function getLatestActualEntry(items) {
  return (items || [])
    .filter(item => item && item.value != null && item.time)
    .map(item => ({ ...item, parsedTime: parseGaugeTime(item.time) }))
    .filter(item => item.parsedTime)
    .sort((a, b) => a.parsedTime - b.parsedTime)
    .pop() || null;
}

function normalizeGaugeValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(100, Math.round(numeric * 100)));
}

function getTimeBounds(items) {
  return (items || [])
    .filter(item => item && item.time)
    .map(item => parseGaugeTime(item.time))
    .filter(Boolean)
    .sort((a, b) => a - b);
}

async function refreshScenarioMetrics() {
  if (!infoActivity || !infoNectar || !infoMismatch) return;

  try {
    const cacheBust = Date.now();
    const [nectarData, mismatchData, activityData] = await Promise.all([
      fetch(`${GAUGE_DATA_BASE_URL}/nectar-supply-overview.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json()),
      fetch(`${GAUGE_DATA_BASE_URL}/mismatch-overview.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json()),
      fetch(`${GAUGE_DATA_BASE_URL}/bee-activity-forecast.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json())
    ]);

    const latestNectar = getLatestActualEntry(nectarData.actual);
    const latestMismatch = getLatestActualEntry(mismatchData.actual);
    const latestActivity = getLatestActualEntry(aggregateBeeDaily(activityData.actual, "mean"));

    infoActivity.textContent = `${normalizeGaugeValue(latestActivity?.value ?? 0)}%`;
    infoNectar.textContent = `${normalizeGaugeValue(latestNectar?.value ?? 0)}%`;
    infoMismatch.textContent = `${normalizeGaugeValue(latestMismatch?.value ?? 0)}%`;
  } catch (error) {
    console.error("Failed to refresh scenario metrics:", error);
    infoActivity.textContent = "--";
    infoNectar.textContent = "--";
    infoMismatch.textContent = "--";
  }
}

function setSiteCardVisible(visible) {
  if (!siteInfoToggle || !siteDetailCard) return;
  siteInfoToggle.classList.toggle("is-expanded", visible);
  siteInfoToggle.setAttribute("aria-expanded", String(visible));
  siteDetailCard.classList.toggle("is-visible", visible);
}

async function refreshSiteDateRange() {
  if (!siteDateRange) return;

  try {
    const cacheBust = Date.now();
    const [floweringData, nectarData, mismatchData, activityData] = await Promise.all([
      fetch(`${GAUGE_DATA_BASE_URL}/flowering-overview.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json()),
      fetch(`${GAUGE_DATA_BASE_URL}/nectar-supply-overview.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json()),
      fetch(`${GAUGE_DATA_BASE_URL}/mismatch-overview.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json()),
      fetch(`${GAUGE_DATA_BASE_URL}/bee-activity-forecast.json?t=${cacheBust}`, { cache: "no-store" }).then(response => response.json())
    ]);

    const allDates = [
      ...getTimeBounds(floweringData.actual),
      ...getTimeBounds(floweringData.forecast),
      ...getTimeBounds(nectarData.actual),
      ...getTimeBounds(nectarData.forecast),
      ...getTimeBounds(mismatchData.actual),
      ...getTimeBounds(mismatchData.forecast),
      ...getTimeBounds(activityData.actual),
      ...getTimeBounds(activityData.forecast)
    ].sort((a, b) => a - b);

    if (!allDates.length) {
      siteDateRange.textContent = "--";
      return;
    }

    siteDateRange.textContent = `${fmtDate(allDates[0])} 至 ${fmtDate(allDates[allDates.length - 1])}`;
  } catch (error) {
    console.error("Failed to refresh site date range:", error);
    siteDateRange.textContent = "--";
  }
}

async function initializeInfoPanel() {
  updateClock();
  await Promise.all([refreshScenarioMetrics(), refreshSiteDateRange()]);
}

function ensureChatElements() {
  if (!chatStage) return {};

  let list = $("chatMessageList");
  if (!list) {
    list = document.createElement("div");
    list.id = "chatMessageList";
    list.className = "chat-message-list";
    chatStage.appendChild(list);
  }

  let status = $("chatStatusBar");
  if (!status) {
    status = document.createElement("div");
    status.id = "chatStatusBar";
    status.className = "chat-status-bar";
    chatStage.appendChild(status);
  }

  return { list, status };
}

function scenarioContext(scenario) {
  return {
    id: scenario.id,
    shortLabel: scenario.shortLabel,
    title: scenario.title,
    summary: scenario.summary,
    kicker: scenario.kicker,
    sectionTitle: scenario.sectionTitle,
    detailPoints: scenario.detailPoints || [],
    metrics: scenario.metrics || [],
    detailSections: scenario.detailSections || [],
    detailHighlights: scenario.detailHighlights || []
  };
}

function renderChatContext(scenario) {
  if (!scenario || !chatContextPanel) return;

  chatContextPanel.classList.remove("is-hidden");
  if (chatContextKicker) chatContextKicker.textContent = `${scenario.shortLabel} / Active Strategy Context`;
  if (chatContextTitle) chatContextTitle.textContent = scenario.title;
  if (chatContextSummary) chatContextSummary.textContent = scenario.summary;

  if (chatContextPoints) {
    chatContextPoints.innerHTML = (scenario.detailSections || [])
      .map((section) => `<p class="chat-context-point"><strong>${section.label}</strong>：${section.value}</p>`)
      .join("");
  }

  if (composerTip) composerTip.textContent = `Context: ${scenario.title}`;
  if (chatComposer) chatComposer.placeholder = `继续深入聊「${scenario.title}」...`;
}

function enterStrategyChat(index) {
  const scenario = state.scenarios[index];
  if (!scenario) return;

  state.selectedIndex = index;
  state.activeScenario = scenario;
  state.messages = [];

  sessionStorage.setItem("beeScenarioChatContext", JSON.stringify(scenarioContext(scenario)));
  window.location.href = `./scenario_chat.html?scenario=${encodeURIComponent(scenario.id)}`;
}

function appendMessage(role, content) {
  const { list } = ensureChatElements();
  if (!list) return null;

  const item = document.createElement("article");
  item.className = `chat-message ${role}`;
  item.textContent = content;
  list.appendChild(item);
  item.scrollIntoView({ behavior: "smooth", block: "end" });
  return item;
}

function setChatStatus(message, isError = false) {
  const { status } = ensureChatElements();
  if (!status) return;
  status.textContent = message || "";
  status.classList.toggle("is-error", isError);
}

async function sendChatMessage() {
  const text = chatComposer?.value.trim();
  if (!text || state.isSending) return;

  const scenario = state.activeScenario;
  state.isSending = true;
  chatComposer.value = "";

  appendMessage("user", text);
  state.messages.push({ role: "user", content: text });
  const pending = appendMessage("assistant", "正在结合当前策略背景思考...");
  setChatStatus("AI 正在基于当前策略继续分析...");

  try {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: text,
        history: state.messages,
        scenarioContext: scenario ? scenarioContext(scenario) : null
      })
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `发送失败，HTTP ${response.status}`);

    const reply = data.reply || "AI 暂时没有返回内容。";
    if (pending) pending.textContent = reply;
    state.messages.push({ role: "assistant", content: reply });
    setChatStatus("AI 回复完成。");
  } catch (error) {
    if (pending) {
      pending.className = "chat-message system";
      pending.textContent = error.message || "对话失败，请稍后再试。";
    }
    setChatStatus(error.message || "对话失败，请稍后再试。", true);
  } finally {
    state.isSending = false;
  }
}

function metricHtml(metric) {
  const label = metric?.label || "指标";
  const value = metric?.value || "--";
  return `<span class="card-number-placeholder" data-label="${label}">${value}</span>`;
}

function cardHtml(scenario, index) {
  const serial = String(index + 1).padStart(3, "0");
  const cardImageClass = `card-image-${(index % CARD_IMAGE_COUNT) + 1}`;
  return `
    <button class="scenario-card ${cardImageClass}" type="button" data-index="${index}">
      <div class="card-topline"><span class="card-chip">${serial}</span><span class="card-kpi"></span></div>
      <strong class="card-title">${scenario.title}</strong>
      <span class="card-text">${scenario.summary}</span>
      <span class="card-text short">${scenario.kicker}</span>
      <div class="card-number-row">
        ${metricHtml(scenario.metrics?.[0])}
        ${metricHtml(scenario.metrics?.[1])}
      </div>
      <span class="card-footer">${scenario.sectionTitle || "策略说明"}</span>
    </button>
  `;
}

function loadingCardHtml(index) {
  const serial = String(index + 1).padStart(3, "0");
  return `
    <button class="scenario-card is-loading" type="button" disabled aria-busy="true">
      <div class="card-topline"><span class="card-chip">${serial}</span><span class="card-kpi"></span></div>
      <strong class="card-title">正在生成策略方案</strong>
      <span class="card-text">正在读取当下花期、蜜源、蜂群活跃度、错配和天气数据。</span>
      <span class="card-text short">CURRENT DATA</span>
      <div class="card-number-row">
        <span class="card-number-placeholder" data-label="匹配度">--</span>
        <span class="card-number-placeholder" data-label="风险等级">--</span>
      </div>
      <span class="card-footer">请稍候</span>
    </button>
  `;
}

function renderCards() {
  if (!stage) return;
  stage.innerHTML = state.scenarios.slice(0, 4).map((s, i) => cardHtml(s, i)).join("");
  stage.querySelectorAll(".scenario-card").forEach((el) => {
    el.addEventListener("click", () => {
      const idx = Number(el.dataset.index || 0);
      state.selectedIndex = idx;
      openDetail(idx);
    });
  });
}

function renderLoadingCards() {
  if (!stage) return;
  stage.innerHTML = [0, 1, 2, 3].map(loadingCardHtml).join("");
}

function asPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return String(Number(numeric.toFixed(3)));
}

function currentDetailSummary(scenario) {
  const basis = state.analysisContext?.strategyBasis || {};
  const weather = state.analysisContext?.weather || {};
  if (!basis.date) return scenario.summary;

  const common = `${basis.date} 实时数据：${basis.topFlower || "当前主花源"}花期指数${asPercent(basis.floweringValue)}、花蜜量${asPercent(basis.nectarValue)}，蜂群核心活跃度${asPercent(basis.activityValue)}，错配风险${asPercent(basis.mismatchGap)}（${basis.mismatchLevel || "待评估"}），天气${weather.text || "暂无"}、${weather.temp ?? "--"}°C、降水概率${weather.pop ?? "--"}%、风速${weather.windSpeed ?? "--"}km/h。`;

  const endings = {
    "scenario-a": "因此当前详情摘要只提示维持低扰动监测，下面的保守型管理策略保持不变。",
    "scenario-b": "因此当前详情摘要只提示关注核心活动时段的遮荫、防风与补水，下面的时段防护策略保持不变。",
    "scenario-c": "因此当前详情摘要只提示是否需要短期激励采集效率，下面的补充饲喂与增效策略保持不变。",
    "scenario-d": "因此当前详情摘要只提示长期蜜源承接与生态韧性压力，下面的生态优化策略保持不变。"
  };

  return `${common}${endings[scenario.id] || "下面的策略说明保持不变。"}`;
}

function renderDetail(index) {
  const s = state.scenarios[index];
  if (!s) return;

  if (detailKicker) detailKicker.textContent = `${s.shortLabel} / ${s.kicker}`;
  if (detailTitle) detailTitle.textContent = s.title;
  if (detailSummary) detailSummary.textContent = currentDetailSummary(s);
  if (detailSectionTitle) detailSectionTitle.textContent = s.sectionTitle || "策略说明";

  if (detailCopy) {
    const sections = s.detailSections || [];
    detailCopy.innerHTML = sections
      .map((sec) => {
        const isSteps = /具体操作|操作|步骤/.test(sec.label || "");
        const steps = String(sec.value || "")
          .split(/[;；。]/)
          .map((item) => item.trim())
          .filter(Boolean);

        if (isSteps && steps.length > 1) {
          return `
            <article class="detail-layout-card detail-layout-card-steps">
              <span class="detail-field-label">${sec.label}</span>
              <div class="detail-step-list">
                ${steps
                  .map(
                    (step, i) => `
                      <div class="detail-step-item">
                        <span class="detail-step-index">${i + 1}</span>
                        <p class="detail-field-text">${step}</p>
                      </div>`
                  )
                  .join("")}
              </div>
            </article>`;
        }

        return `
          <article class="detail-layout-card">
            <span class="detail-field-label">${sec.label}</span>
            <p class="detail-field-text">${sec.value}</p>
          </article>`;
      })
      .join("");
  }

  if (detailMetrics) {
    const hs = s.detailHighlights || [];
    detailMetrics.innerHTML = hs
      .map(
        (x) => `
          <article class="detail-metric-card glass-subpanel">
            <span class="detail-metric-label">${x.label}</span>
            <strong class="detail-metric-value">${x.value}</strong>
            <p class="detail-metric-note">${x.note || ""}</p>
          </article>`
      )
      .join("");
  }

  if (detailPrimary) detailPrimary.textContent = `Continue ${s.title}`;
}

function openDetail(index) {
  renderDetail(index);
  showStrategyDetail();
  if (detailOverlay) {
    detailOverlay.classList.add("is-visible");
    detailOverlay.setAttribute("aria-hidden", "false");
  }
}

function closeDetail() {
  if (detailOverlay) {
    detailOverlay.classList.remove("is-visible");
    detailOverlay.setAttribute("aria-hidden", "true");
  }
  showStrategyDetail();
}

function showStrategyDetail() {
  detailModal?.classList.remove("is-hidden");
  corridorPanel?.classList.add("is-hidden");
  corridorPanel?.setAttribute("aria-hidden", "true");
}

function showCorridorPanel() {
  detailModal?.classList.add("is-hidden");
  corridorPanel?.classList.remove("is-hidden");
  corridorPanel?.setAttribute("aria-hidden", "false");
}

function readScenarioCache() {
  try {
    const raw = localStorage.getItem(SCENARIO_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed?.analysisContext) return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeScenarioCache(payload) {
  try {
    localStorage.setItem(
      SCENARIO_CACHE_KEY,
      JSON.stringify({
        source: payload.source || "unknown",
        cachedAt: payload.cachedAt || new Date().toISOString(),
        analysisContext: payload.analysisContext || null
      })
    );
  } catch {
    // Ignore cache write failures; rendering can continue.
  }
}

function applyScenarioPayload(payload) {
  if (!payload) return;
  state.analysisContext = payload.analysisContext || payload;
  state.source = payload.source || "summary-only";
  renderCards();
  renderDetail(state.selectedIndex || 0);
}

async function fetchScenarios() {
  let timeout;
  try {
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), 8000);
    const res = await fetch(`/api/scenarios?summaryOnly=1&t=${Date.now()}`, { cache: "no-store", signal: controller.signal });
    if (!res.ok) throw new Error("bad response");
    const data = await res.json();
    if (!data?.analysisContext) throw new Error("empty");
    return {
      source: data.source || "server",
      cachedAt: data.cachedAt || new Date().toISOString(),
      analysisContext: data.analysisContext
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

function bindEvents() {
  if (backButton) backButton.addEventListener("click", () => { window.location.href = "./dashboard1.html"; });
  if (freeChatButton) freeChatButton.addEventListener("click", () => { window.location.href = "./scenario_chat.html?mode=free"; });
  if (siteInfoToggle && siteDetailCard) {
    siteInfoToggle.addEventListener("click", (event) => {
      event.stopPropagation();
      setSiteCardVisible(!siteDetailCard.classList.contains("is-visible"));
    });
  }
  if (detailClose) detailClose.addEventListener("click", closeDetail);
  if (detailBackdrop) detailBackdrop.addEventListener("click", closeDetail);
  if (detailPrimary) detailPrimary.addEventListener("click", () => enterStrategyChat(state.selectedIndex));
  if (corridorButton) corridorButton.addEventListener("click", showCorridorPanel);
  if (corridorBackButton) corridorBackButton.addEventListener("click", showStrategyDetail);
  if (corridorCloseButton) corridorCloseButton.addEventListener("click", closeDetail);
  if (sendButton) sendButton.addEventListener("click", sendChatMessage);
  if (chatComposer) {
    chatComposer.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendChatMessage();
      }
    });
  }
  if (chatContextClear) {
    chatContextClear.addEventListener("click", () => {
      state.activeScenario = null;
      state.messages = [];
      chatContextPanel?.classList.add("is-hidden");
      document.body.classList.remove("is-scenario-chat-active");
      scenarioOverlay?.classList.remove("is-hidden");
      if (composerTip) composerTip.textContent = "Placeholder only";
      if (chatComposer) chatComposer.placeholder = "Type to continue this scenario conversation...";
    });
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeDetail();
    }
  });

  document.addEventListener("click", (event) => {
    if (!siteDetailCard || !siteInfoToggle || !siteDetailCard.classList.contains("is-visible")) return;
    if (siteDetailCard.contains(event.target) || siteInfoToggle.contains(event.target)) return;
    setSiteCardVisible(false);
  });
}

async function init() {
  updateClock();
  setInterval(updateClock, 60000);
  bindEvents();
  renderCards();

  const infoReady = initializeInfoPanel();
  setInterval(initializeInfoPanel, 300000);

  const remote = await fetchScenarios();
  if (remote) {
    applyScenarioPayload(remote);
    writeScenarioCache(remote);
  } else {
    applyScenarioPayload(readScenarioCache() || { source: "frozen" });
  }

  await infoReady;

  const params = new URLSearchParams(window.location.search);
  const returnScenarioId = params.get("scenario");
  if (returnScenarioId) {
    const returnIndex = state.scenarios.findIndex((scenario) => scenario.id === returnScenarioId);
    if (returnIndex >= 0) {
      state.selectedIndex = returnIndex;
      openDetail(returnIndex);
    }
  }
}

init();
