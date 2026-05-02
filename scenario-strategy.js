"use strict";

function createContextualScenarios(context, latestWeather) {
  const basis = normalizeBasis(context, latestWeather);
  const commonMetrics = (action, note) => [
    metric("匹配度", basis.matchPercent, `基于 ${basis.date} 同一组花期、蜜源、活跃度和错配数据`),
    metric("风险等级", basis.riskLevel, note || `${basis.mismatchLevel}，错配值 ${toPercent(basis.mismatchGap)}`),
    metric("建议动作", action, `主花源：${basis.topFlower}`)
  ];

  return [
    makeScenario({
      id: "scenario-a",
      shortLabel: "A",
      title: "低成本稳态维持方案",
      kicker: "LOW COST",
      summary: `${basis.date} 的事实底座显示，${basis.topFlower} 为主花源，花期 ${toPercent(basis.floweringValue)}、花蜜量 ${toPercent(basis.nectarValue)}，当前更适合用低成本动作守住既有授粉窗口。`,
      metrics: commonMetrics("维持", "风险按统一事实底座评估，本方案用低干预控制额外成本"),
      detailPoints: [
        `核心活跃窗口为 ${basis.activityWindow}，活跃度 ${toPercent(basis.activityValue)}，优先保护已经有效的时段。`,
        `错配等级为 ${basis.mismatchLevel}，暂不建议大规模移动蜂箱或追加高成本措施。`,
        "适合预算有限、希望先保持当前效率并持续观察的用户。"
      ],
      detailSections: sectionList(
        `当前 ${basis.topFlower} 与 ${basis.topNectar} 仍可支撑授粉作业，重点是避免过度干预造成成本上升。`,
        "低成本稳态维持",
        "保持蜂箱位置和巡检频率稳定，只围绕核心活跃窗口做小幅调整。",
        "早晚各巡检一次；记录花期、花蜜量和蜂群状态；当天不做大范围迁箱。",
        "低",
        "稳定当前产出窗口",
        "成本控制 / 稳定收益"
      ),
      detailHighlights: highlightList("低", "稳住窗口", "成本优先")
    }),
    makeScenario({
      id: "scenario-b",
      shortLabel: "B",
      title: "天气与时段保护方案",
      kicker: "WEATHER WINDOW",
      summary: `同样基于 ${basis.date} 数据，天气为 ${basis.weatherText}，温度 ${basis.tempText}、降水概率 ${basis.popText}，本方案把重点放在保护高活跃时段和降低天气扰动。`,
      metrics: commonMetrics("保护", "风险等级不另行改写，本方案主要降低天气和作业时段带来的执行风险"),
      detailPoints: [
        `把开箱、补水、遮阴或防潮操作集中到 ${basis.activityWindow} 前后。`,
        basis.weatherStress ? "当前天气存在扰动信号，应减少低效时段开箱和长时间暴露。" : "天气压力不高时，也要把关键动作放到蜂群更活跃的小时内。",
        "适合担心降雨、温度或风速影响当天执行质量的用户。"
      ],
      detailSections: sectionList(
        `天气条件会影响蜂群出勤稳定性，当前天气记录为：${basis.weatherText}。`,
        "天气 / 时段保护",
        "围绕高活跃时段安排必要操作，减少天气变化对授粉效率的压缩。",
        basis.weatherStress
          ? "优先补水、防潮或遮阴；缩短开箱时间；把重点观察留到较安全时段。"
          : "上午完成主要巡检；午后只保留必要观察；傍晚复核第二天调整点。",
        "低到中",
        "减少天气扰动造成的效率损失",
        "执行稳定 / 风险控制"
      ),
      detailHighlights: highlightList("低到中", "抗扰保护", "稳定优先")
    }),
    makeScenario({
      id: "scenario-c",
      shortLabel: "C",
      title: "短期增效执行方案",
      kicker: "SHORT TERM GAIN",
      summary: `${basis.date} 的同一事实底座下，核心活跃度为 ${toPercent(basis.activityValue)}，峰值 ${toPercent(basis.activityPeak)}；本方案用适中投入提升近期授粉和采集效率。`,
      metrics: commonMetrics("增效", "风险等级保持一致，本方案通过更密集执行换取短期效率"),
      detailPoints: [
        `围绕 ${basis.topFlower} 的主花源区，把巡检和辅助动作集中到高活跃窗口。`,
        `花期 ${toPercent(basis.floweringValue)}、花蜜量 ${toPercent(basis.nectarValue)}，具备做短期效率优化的基础。`,
        "适合愿意增加少量人力和管理频率、希望近期见效的用户。"
      ],
      detailSections: sectionList(
        "当前资源和蜂群活动已经有可利用窗口，但常规巡检可能无法充分放大短期效率。",
        "短期增效",
        "增加关键时段观察密度，微调蜂箱朝向、近场动线和辅助补给。",
        "高活跃时段前完成路线检查；必要时微调近场布置；当天结束前复盘授粉热点。",
        "中",
        "提高近期授粉与采集效率",
        "短期效率 / 快速反馈"
      ),
      detailHighlights: highlightList("中", "近期增效", "效率优先")
    }),
    makeScenario({
      id: "scenario-d",
      shortLabel: "D",
      title: "长期收益与生态韧性方案",
      kicker: "LONG TERM",
      summary: `仍以 ${basis.date} 的当前数据为基础，错配为 ${toPercent(basis.mismatchGap)}（${basis.mismatchLevel}）；本方案更重视后续花源承接、蜂群健康和持续收益。`,
      metrics: commonMetrics("布局", "风险等级来自同一事实底座，本方案把风险处理放到中长期韧性建设"),
      detailPoints: [
        `以 ${basis.topFlower} 和 ${basis.topNectar} 为核心，建立后续花源和蜂群状态的连续记录。`,
        "不追求当天最大化开箱，而是减少资源断档、蜂群消耗和后续错配放大的概率。",
        "适合看重持续收益、生态稳定和后续窗口承接的用户。"
      ],
      detailSections: sectionList(
        "当前阶段不仅要看当天效率，还要为后续花期和蜂群节律变化留下调整空间。",
        "长期收益布局",
        "建立连续监测和分区管理，逐步提高花源承接能力与蜂群抗扰能力。",
        "保留每日同口径记录；标记后续花源区；减少低收益扰动；每 2-3 天复核一次策略等级。",
        "中到高",
        "提升持续收益和生态韧性",
        "长期收益 / 韧性优先"
      ),
      detailHighlights: highlightList("中到高", "持续优化", "长期收益")
    })
  ];
}

function normalizeBasis(context, latestWeather) {
  const raw = context?.strategyBasis || {};
  const weather = context?.weather || summarizeLatestWeather(latestWeather);
  const mismatchGap = toRatio(raw.mismatchGap ?? context?.mismatch?.latestForecastGap);
  const activityValue = toRatio(raw.activityValue ?? context?.activity?.latestForecastValue ?? context?.activity?.latestActualValue);
  const floweringValue = toRatio(raw.floweringValue ?? context?.flowering?.latestForecastValue ?? context?.flowering?.latestActualValue);
  const nectarValue = toRatio(raw.nectarValue ?? context?.nectar?.latestForecastValue ?? context?.nectar?.latestActualValue);

  return {
    date: safeString(raw.date || context?.snapshotDate || context?.forecastDate, "当前阶段"),
    topFlower: safeString(raw.topFlower || context?.flowering?.topPlantName, "当前主花源"),
    topNectar: safeString(raw.topNectar || context?.nectar?.topPlantName, "当前蜜源"),
    floweringValue,
    nectarValue,
    activityValue,
    activityPeak: toRatio(raw.activityPeak ?? activityValue),
    activityWindow: safeString(raw.activityWindow, "核心授粉时段"),
    mismatchGap,
    mismatchLevel: safeString(raw.mismatchLevel || context?.mismatch?.mismatchLevel, deriveMismatchLevel(mismatchGap)),
    matchPercent: safeString(raw.matchPercent, estimateMatchPercent(floweringValue, nectarValue, activityValue, mismatchGap)),
    riskLevel: safeString(raw.riskLevel, riskLevel(mismatchGap, weather)),
    weatherText: safeString(weather?.text, "暂无天气数据"),
    tempText: Number.isFinite(Number(weather?.temp)) ? `${Number(weather.temp)}°C` : "--",
    popText: Number.isFinite(Number(weather?.pop)) ? `${Number(weather.pop)}%` : "--",
    weatherStress: Number(weather?.pop) >= 50 || Number(weather?.windSpeed) >= 20 || Number(weather?.temp) >= 30
  };
}

function makeScenario({ id, shortLabel, title, kicker, summary, metrics, detailPoints, detailSections, detailHighlights }) {
  return {
    id,
    shortLabel,
    title,
    kicker,
    summary,
    sectionTitle: "策略说明",
    detailPoints,
    detailSections,
    detailHighlights,
    metrics
  };
}

function metric(label, value, note) {
  return { label, value, note };
}

function highlightList(cost, effect, orientation) {
  return [
    { label: "成本", value: cost, note: "按当前场地条件估算" },
    { label: "效果", value: effect, note: "围绕当下事实底座设计" },
    { label: "收益导向", value: orientation, note: "用于选择不同管理取向" }
  ];
}

function sectionList(problem, type, content, operation, cost, effect, orientation) {
  return [
    { label: "应对问题", value: problem },
    { label: "建议类型", value: type },
    { label: "建议内容", value: content },
    { label: "具体操作", value: operation },
    { label: "成本", value: cost },
    { label: "效果", value: effect },
    { label: "收益导向", value: orientation }
  ];
}

function summarizeLatestWeather(payload) {
  const hourly = Array.isArray(payload?.hourly) ? payload.hourly : [];
  const latest = hourly[0] || {};
  return {
    text: latest.text,
    temp: latest.temp,
    pop: latest.pop,
    windSpeed: latest.windSpeed
  };
}

function safeString(value, fallback) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function toRatio(value) {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0;
}

function toPercent(value) {
  return `${Math.round(toRatio(value) * 100)}%`;
}

function estimateMatchPercent(flowering, nectar, activity, mismatch) {
  const score = toRatio(flowering) * 0.28 + toRatio(nectar) * 0.24 + toRatio(activity) * 0.28 + (1 - toRatio(mismatch)) * 0.2;
  return toPercent(score);
}

function riskLevel(mismatch, weather) {
  const value = toRatio(mismatch);
  const weatherStress = Number(weather?.pop) >= 50 || Number(weather?.windSpeed) >= 20 || Number(weather?.temp) >= 30;
  if (value >= 0.5) return "高";
  if (value >= 0.3 || weatherStress) return "中";
  return "低";
}

function deriveMismatchLevel(value) {
  const ratio = toRatio(value);
  if (ratio >= 0.5) return "高错配";
  if (ratio >= 0.3) return "中度错配";
  if (ratio >= 0.12) return "轻微错配";
  return "基本匹配";
}

module.exports = {
  createContextualScenarios
};
