const historyChart = echarts.init(document.getElementById("historyChart"));
const futureChart = echarts.init(document.getElementById("futureChart"));

function showError(message) {
    const box = document.getElementById("errorBox");
    box.style.display = "block";
    box.textContent = message;
}

async function loadMonitorData() {
    const response = await fetch("./data/ml-monitor.json", { cache: "no-store" });
    if (!response.ok) {
        throw new Error(`无法加载 ml-monitor.json，状态码 ${response.status}`);
    }
    return response.json();
}

function formatValue(value) {
    if (value == null || Number.isNaN(value)) {
        return "-";
    }
    return typeof value === "number" ? value.toFixed(4).replace(/0+$/, "").replace(/\.$/, "") : value;
}

function renderMetricCards(data) {
    const metricGrid = document.getElementById("metricGrid");
    const modelType = data.model?.target_name || "蜜蜂行为模型";
    const sampleCount = data.history?.summary?.sample_count ?? 0;
    const ruleMae = data.history?.summary?.rule_mae;
    const mlMae = data.history?.summary?.ml_mae;
    const improvement = data.history?.summary?.mae_improvement;

    const cards = [
        {
            label: "训练样本数",
            value: sampleCount,
            note: sampleCount < 24 ? "样本量还偏少，先把 ML 当成辅助参考。" : "样本量开始可用，但仍建议持续观察。"
        },
        {
            label: "所属模型",
            value: modelType,
            note: "当前这部分 ML 只作用在第 3 层蜜蜂行为模型。"
        },
        {
            label: "规则模型 MAE",
            value: formatValue(ruleMae),
            note: "越低越好，表示规则预测平均偏差越小。"
        },
        {
            label: "ML 修正后 MAE",
            value: formatValue(mlMae),
            note: "这是残差修正后与真实值之间的平均误差。"
        },
        {
            label: "MAE 改善值",
            value: formatValue(improvement),
            note: improvement != null && improvement > 0 ? "正数表示 ML 修正暂时有帮助。" : "如果不是正数，说明当前修正收益不稳定。"
        }
    ];

    metricGrid.innerHTML = cards.map(card => `
        <div class="card">
            <div class="label">${card.label}</div>
            <div class="value">${card.value}</div>
            <div class="note">${card.note}</div>
        </div>
    `).join("");
}

function renderAlerts(data) {
    const container = document.getElementById("alerts");
    const alerts = data.alerts || [];
    container.innerHTML = alerts.map(alert => `
        <div class="alert ${alert.level === "ok" ? "ok" : ""}">
            <div class="alert-title">${alert.title}</div>
            <div>${alert.message}</div>
        </div>
    `).join("");
}

function renderHistoryChart(data) {
    const items = data.history?.items || [];
    if (!items.length) {
        showError("历史回测数据为空，当前无法绘制左侧回测图。");
        return;
    }

    historyChart.setOption({
        animation: false,
        tooltip: { trigger: "axis" },
        legend: { data: ["实际值", "规则预测", "ML 回测值"] },
        grid: { left: 48, right: 20, top: 44, bottom: 58 },
        xAxis: {
            type: "category",
            data: items.map(item => item.time),
            axisLabel: { rotate: 25 }
        },
        yAxis: {
            type: "value",
            min: 0,
            max: 1,
            name: "活跃度"
        },
        series: [
            {
                name: "实际值",
                type: "line",
                data: items.map(item => item.actual_activity),
                smooth: true,
                symbolSize: 6
            },
            {
                name: "规则预测",
                type: "line",
                data: items.map(item => item.rule_expected_activity),
                smooth: true,
                lineStyle: { type: "dashed" },
                symbolSize: 5
            },
            {
                name: "ML 回测值",
                type: "line",
                data: items.map(item => item.ml_adjusted_activity),
                smooth: true,
                symbolSize: 5
            }
        ]
    });
}

function renderFutureChart(data) {
    const items = data.future?.items || [];
    if (!items.length) {
        showError("未来 ML 修正数据为空，当前无法绘制右侧未来图。");
        return;
    }

    futureChart.setOption({
        animation: false,
        tooltip: { trigger: "axis" },
        legend: { data: ["规则预测", "ML 修正预测", "修正量"] },
        grid: { left: 48, right: 46, top: 44, bottom: 58 },
        xAxis: {
            type: "category",
            data: items.map(item => item.time),
            axisLabel: { rotate: 25 }
        },
        yAxis: [
            {
                type: "value",
                min: 0,
                max: 1,
                name: "活跃度"
            },
            {
                type: "value",
                name: "修正量"
            }
        ],
        series: [
            {
                name: "规则预测",
                type: "line",
                data: items.map(item => item.rule_expected_activity),
                smooth: true,
                lineStyle: { type: "dashed" },
                symbolSize: 5
            },
            {
                name: "ML 修正预测",
                type: "line",
                data: items.map(item => item.ml_adjusted_activity),
                smooth: true,
                symbolSize: 5
            },
            {
                name: "修正量",
                type: "bar",
                yAxisIndex: 1,
                data: items.map(item => item.ml_residual_adjustment),
                barMaxWidth: 18
            }
        ]
    });
}

async function main() {
    try {
        const data = await loadMonitorData();
        renderMetricCards(data);
        renderAlerts(data);
        renderHistoryChart(data);
        renderFutureChart(data);
    } catch (error) {
        showError(`页面数据加载失败：${error.message}。如果你是直接双击打开 HTML，请改用本地静态服务器或 GitHub Pages 链接。`);
        console.error(error);
    }
}

main();

window.addEventListener("resize", () => {
    historyChart.resize();
    futureChart.resize();
});
