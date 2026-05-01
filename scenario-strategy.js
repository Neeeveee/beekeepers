"use strict";

function createContextualScenarios(context, latestWeather) {
  const floweringNow = toRatio(context?.flowering?.latestActualValue);
  const floweringNext = toRatio(context?.flowering?.latestForecastValue);
  const nectarNow = toRatio(context?.nectar?.latestActualValue);
  const nectarNext = toRatio(context?.nectar?.latestForecastValue);
  const activityNow = toRatio(context?.activity?.latestActualValue);
  const activityNext = toRatio(context?.activity?.latestForecastValue);
  const mismatchGap = toRatio(context?.mismatch?.latestForecastGap);
  const mismatchLabel = safeString(context?.mismatch?.mismatchLevel, "待评估");
  const mismatchType = safeString(context?.mismatch?.mismatchType, "unknown");
  const topFlower = safeString(context?.flowering?.topPlantName, "当前主花源");
  const topNectar = safeString(context?.nectar?.topPlantName, topFlower);
  const weather = summarizeWeatherForStrategy(latestWeather);
  const snapshotDate = context?.snapshotDate || context?.generatedAt || "当前数据快照";
  const forecastDate = context?.forecastDate || context?.flowering?.latestForecastDate || "未来窗口";

  const stableWindow = floweringNow >= 0.65 && nectarNow >= 0.5 && mismatchGap < 0.35;
  const risingResource = floweringNext > floweringNow + 0.06 || nectarNext > nectarNow + 0.08;
  const currentStress = activityNow < 0.45 || mismatchGap >= 0.4;
  const weatherStress = weather.isHeatStress || weather.isRainStress || weather.isWindStress;
  const isResourceAhead = mismatchType === "resource_ahead" || mismatchType === "花源先行";
  const isBehaviorAhead = mismatchType === "behavior_ahead" || mismatchType === "蜂群先行";
  const mismatchDirection = isResourceAhead
    ? "花源变化快于蜂群节奏"
    : isBehaviorAhead
      ? "蜂群活动快于花源释放"
      : "花源与蜂群节奏仍需复核";

  return [
    makeScenario({
      id: "scenario-a",
      shortLabel: "A",
      title: stableWindow ? "当前授粉窗口维持方案" : "当前授粉窗口纠偏方案",
      kicker: "CURRENT WINDOW",
      summary: stableWindow
        ? `${snapshotDate} 快照显示花期 ${toPercent(floweringNow)}、花蜜量 ${toPercent(nectarNow)}，当前仍可维持低干预运行。`
        : `${snapshotDate} 快照显示活跃度 ${toPercent(activityNow)}、错配风险 ${toPercent(mismatchGap)}，需要先做日内节奏纠偏。`,
      metrics: [
        metric("匹配度", estimateMatchPercent(floweringNow, nectarNow, activityNow, mismatchGap), `主花源：${topFlower}`),
        metric("风险等级", riskLevel(mismatchGap, currentStress), mismatchLabel),
        metric("建议动作", stableWindow ? "监测" : "纠偏", `蜜源：${topNectar}`)
      ],
      detailPoints: [
        stableWindow ? "当前不是大改蜂箱布局的时机，重点是守住已形成的有效授粉窗口。" : "当前不宜沿用静态策略，需要先把巡检和干预集中到更有效的时段。",
        `蜂群活跃度为 ${toPercent(activityNow)}，日内调度比大规模重配置更划算。`,
        risingResource ? `到 ${forecastDate} 资源侧有抬升信号，应为后续放大窗口做准备。` : `到 ${forecastDate} 资源增量有限，应优先保住现有投入产出。`
      ],
      detailSections: sectionList(
        stableWindow ? "当前花期、蜜源与蜂群活动仍处于可执行窗口。" : "当前蜂群活动与资源释放存在偏差，继续静态维持会放大损失。",
        stableWindow ? "稳态维持" : "轻量纠偏",
        stableWindow ? "保持蜂箱位置与巡检频率稳定，只围绕当天窗口做微调。" : "把巡检与辅助动作集中到活跃度更高的时段，先恢复当天效率。",
        stableWindow ? "早晚各巡检一次；记录花期、花蜜量和蜂群活动变化；当天不做大规模迁移。" : "压缩低效时段开箱；把重点观察移到高活跃时段；当天结束前复核错配变化。",
        stableWindow ? "低" : "低到中",
        stableWindow ? "稳住当前产出窗口" : "快速止损并恢复可执行节奏",
        stableWindow ? "管理效率 / 稳定性" : "短期效率 / 风险控制"
      ),
      detailHighlights: highlightList(stableWindow ? "低" : "低到中", stableWindow ? "稳定窗口" : "轻量纠偏", stableWindow ? "稳态优先" : "先止损再优化")
    }),
    makeScenario({
      id: "scenario-b",
      shortLabel: "B",
      title: weatherStress ? "实时天气压力应对方案" : "活跃时段保护方案",
      kicker: "WEATHER PRESSURE",
      summary: weatherStress
        ? `最新天气显示${weather.summary}，应把开箱、巡检和补水等动作前移到更安全的时段。`
        : `最新天气未见强压力，但当前活跃度为 ${toPercent(activityNow)}，仍需保护高价值作业时段。`,
      metrics: [
        metric("匹配度", estimateMatchPercent(floweringNow, nectarNow, activityNow, mismatchGap), `活跃度：${toPercent(activityNow)}`),
        metric("风险等级", weatherStress ? "中" : riskLevel(mismatchGap, currentStress), weatherStress ? weather.summary : weather.riskNote),
        metric("建议动作", weatherStress ? "调整" : "保护", `温度：${weather.temperatureText}`)
      ],
      detailPoints: [
        weatherStress ? "天气变量正在直接压缩蜂群活动，不适合沿用平均化巡检节奏。" : "天气看似平稳，但活跃窗口偏窄，时间调度需要更精细。",
        weather.isHeatStress ? "热压下优先保住上午窗口，并处理补水与遮阴。" : weather.isRainStress ? "降雨风险下优先控制开箱和场地潮湿风险。" : "即使无极端天气，也要减少低效时段干预。",
        "这张卡关注当天执行节奏，不替代中长期错配修复。"
      ],
      detailSections: sectionList(
        weatherStress ? `天气侧出现${weather.summary}，蜂群活动容易被压缩。` : "当前活跃时段偏短，日内作业节奏需要重排。",
        "天气 / 时段应对",
        weatherStress ? "把关键观察、补水、遮阴或防潮动作集中到更安全时段。" : "把重点操作集中到活跃度更高的小时内，降低额外扰动。",
        weatherStress ? weather.actionText : "清晨完成主要巡检；午后只保留必要观察；傍晚复核蜂群状态并记录第二天调整点。",
        weatherStress ? "中" : "低到中",
        weatherStress ? "减轻天气压力造成的损失" : "保护有限的高活跃窗口",
        weatherStress ? "健康 / 风险控制" : "执行效率 / 稳定性"
      ),
      detailHighlights: highlightList(weatherStress ? "中" : "低到中", weatherStress ? "抗压保护" : "时段优化", weatherStress ? "蜂群健康" : "作业效率")
    }),
    makeScenario({
      id: "scenario-c",
      shortLabel: "C",
      title: "花蜂错配修复方案",
      kicker: "MISMATCH RECOVERY",
      summary: `${forecastDate} 预测错配为 ${toPercent(mismatchGap)}（${mismatchLabel}），当前判断为“${mismatchDirection}”。`,
      metrics: [
        metric("匹配度", estimateMatchPercent(floweringNext, nectarNext, activityNext, mismatchGap), `节奏：${mismatchTypeLabel(mismatchType)}`),
        metric("风险等级", riskLevel(mismatchGap, currentStress), mismatchLabel),
        metric("建议动作", "恢复", `预测错配：${toPercent(mismatchGap)}`)
      ],
      detailPoints: [
        `这不是单纯天气问题，而是花源与蜂群节奏本身存在 ${mismatchLabel}。`,
        isResourceAhead ? "花源走在前面，重点是让蜂群尽快跟上资源窗口。" : isBehaviorAhead ? "蜂群走在前面，重点是避免资源不足时继续消耗。" : "当前仍需通过两三天复核确认错配方向。",
        currentStress ? "当前已经有执行压力，这张卡适合作为主动调整的核心方案。" : "即使今天还撑得住，也应为未来错配上升提前准备。"
      ],
      detailSections: sectionList(
        `模型预测 ${forecastDate} 存在 ${mismatchLabel}，表现为${mismatchDirection}。`,
        "节奏修复",
        isResourceAhead ? "让蜂群活动尽快贴近花源高值时段，减少错过蜜源窗口。" : isBehaviorAhead ? "控制蜂群无效外出和资源空转，把活动拉回花源可支撑区间。" : "先通过短周期复核确认错配方向，再决定推动蜂群还是补足资源侧。",
        isResourceAhead ? "复核主花源变化；把巡检和辅助动作前移到花源高值期；必要时微调蜂箱朝向或近场布局。" : isBehaviorAhead ? "减少低资源时段开箱；评估补充蜜源或缓冲饲喂；控制无效出勤造成的消耗。" : "连续两天对照花期、花蜜量与活跃度；确认偏差来自资源侧还是行为侧；再决定下一步重配置。",
        mismatchGap >= 0.5 ? "中到高" : "中",
        "提升花蜂匹配度",
        "中期效率 / 中长期收益"
      ),
      detailHighlights: highlightList(mismatchGap >= 0.5 ? "中到高" : "中", "恢复匹配", "效率修复")
    }),
    makeScenario({
      id: "scenario-d",
      shortLabel: "D",
      title: risingResource ? "未来资源窗口前置布局方案" : "资源保守配置方案",
      kicker: "NEXT WINDOW",
      summary: risingResource
        ? `${forecastDate} 花期预计由 ${toPercent(floweringNow)} 升至 ${toPercent(floweringNext)}，蜜源也有抬升，应提前承接窗口。`
        : `${forecastDate} 资源增幅不明显，应控制投入，把蜂群配置到最稳定的窗口。`,
      metrics: [
        metric("匹配度", estimateMatchPercent(floweringNext, nectarNext, activityNext, mismatchGap), `${context?.flowering?.latestForecastDate || forecastDate}`),
        metric("风险等级", riskLevel(mismatchGap, currentStress), risingResource ? "机会窗口" : "保守配置"),
        metric("建议动作", risingResource ? "布局" : "保守", risingResource ? "准备放大窗口" : "避免过度投入")
      ],
      detailPoints: [
        risingResource ? "未来窗口变好时，现在就要为承接更高资源做准备。" : "未来窗口没有明显放大时，盲目加动作会摊薄管理收益。",
        risingResource ? "这张卡关注未来 2 到 5 天，而不是只救当天。" : "这张卡强调资源节制和优先级，而不是继续叠加动作。",
        `当前主花源是 ${topFlower}，后续策略要围绕它的持续性安排。`
      ],
      detailSections: sectionList(
        risingResource ? "未来几天资源侧正在抬升，需要提前准备承接。" : "未来几天资源侧提升有限，需要先守住投入产出比。",
        risingResource ? "前置布局" : "保守配置",
        risingResource ? "提前整理蜂箱周边环境、巡检路线和观察重点，确保资源抬升时能快速放大授粉效率。" : "把精力集中到当前最有效区域与时段，暂停低回报尝试，避免资源分散。",
        risingResource ? "提前两天建立重点观察清单；复核主花源持续性；把后续高价值时段排进固定巡检表。" : "缩减低回报巡检；只保留关键点位观察；每晚复核第二天是否值得升级干预。",
        risingResource ? "中" : "低",
        risingResource ? "放大未来窗口收益" : "守住当前投入产出",
        risingResource ? "未来收益 / 机会把握" : "资源效率 / 风险控制"
      ),
      detailHighlights: highlightList(risingResource ? "中" : "低", risingResource ? "窗口前置" : "保守守住", risingResource ? "机会优先" : "效率优先")
    })
  ];
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
    { label: "效果", value: effect, note: "围绕当下风险设计" },
    { label: "收益导向", value: orientation, note: "服务当前窗口决策" }
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

function summarizeWeatherForStrategy(payload) {
  const hourly = Array.isArray(payload?.hourly) ? payload.hourly : [];
  const latest = hourly[0] || null;
  const temp = Number(latest?.temp);
  const wind = Number(latest?.windSpeed);
  const text = safeString(latest?.text, "天气数据不足");
  const pop = Number(latest?.pop);
  const isHeatStress = Number.isFinite(temp) && temp >= 30;
  const isRainStress = (Number.isFinite(pop) && pop >= 50) || /雨|阵雨|雷/.test(text);
  const isWindStress = Number.isFinite(wind) && wind >= 20;

  let summary = "无明显天气压力";
  let riskLabel = "低";
  let riskNote = "可按常规节奏处理";
  let actionText = "按常规节奏巡检，并继续跟踪天气变化。";

  if (isHeatStress) {
    summary = `高温 ${Number.isFinite(temp) ? `${temp}°C` : ""}`.trim();
    riskLabel = "中";
    riskNote = "热压会压缩活跃窗口";
    actionText = "将主要巡检前移到上午；补水和遮阴优先处理；午后减少开箱。";
  } else if (isRainStress) {
    summary = `降水风险 ${Number.isFinite(pop) ? `${pop}%` : text}`;
    riskLabel = "中";
    riskNote = "阴雨会降低外出与采集效率";
    actionText = "减少无必要开箱；优先处理防潮和场地通行；把关键观察留到较干燥时段。";
  } else if (isWindStress) {
    summary = `风速 ${Number.isFinite(wind) ? `${wind}km/h` : ""}`.trim();
    riskLabel = "中";
    riskNote = "强风会干扰稳定飞行";
    actionText = "减少风口方向干预；复核蜂箱周边遮挡；把重点作业转到更稳时段。";
  }

  return {
    temperatureText: Number.isFinite(temp) ? `${temp}°C` : "--",
    riskLabel,
    riskNote,
    summary,
    actionText,
    isHeatStress,
    isRainStress,
    isWindStress
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
  const resourceScore = (toRatio(flowering) * 0.35) + (toRatio(nectar) * 0.25);
  const activityScore = toRatio(activity) * 0.25;
  const mismatchScore = (1 - toRatio(mismatch)) * 0.15;
  return toPercent(resourceScore + activityScore + mismatchScore);
}

function riskLevel(mismatch, hasStress) {
  const value = toRatio(mismatch);
  if (value >= 0.5 || hasStress) return "高";
  if (value >= 0.3) return "中";
  return "低";
}

function mismatchTypeLabel(type) {
  if (type === "resource_ahead" || type === "花源先行") return "花源先行";
  if (type === "behavior_ahead" || type === "蜂群先行") return "蜂群先行";
  if (type === "matched" || type === "基本匹配") return "基本匹配";
  if (type === "no_data" || type === "无数据") return "无数据";
  return "待确认";
}

module.exports = {
  createContextualScenarios
};
