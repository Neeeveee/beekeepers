# bee-project

一个面向蜜蜂授粉、花期匹配、蜜源供给与生态错配分析的可视化决策支持项目。项目把数据采集、指标推导、预测建模、地图展示、策略卡片生成和 AI 对话串成了一套完整流程，既能做静态监测，也能做带有场景策略的交互式分析。

详细版文档见 [PROJECT_DOCUMENTATION.md](/d:/homeworks/workshop/s7-8/bee-project/PROJECT_DOCUMENTATION.md)。

## 项目概览

这个项目主要由 4 部分组成：

- 数据层：从天气、数据库和业务脚本中整理原始数据，沉淀到 SQLite、`data/` 和 `data_raw/`。
- 分析层：用一系列 Python 脚本计算花期指数、蜜源供给指数、蜂群活动指数、错配指数和未来预测结果。
- 展示层：用 `dashboard1.html`、`scenario_ai.html`、`scenario_chat.html`、`ml_monitor.html` 等页面做主界面、策略页和监测页。
- AI 层：Node/Express 服务负责策略卡片生成与自由问答，接入 DeepSeek，并带有本地回退与缓存机制。

## 当前功能

- 主仪表盘：以地图和信息条为核心的监测总览页面。
- 策略卡片：根据分析结果生成策略建议卡片，支持详情查看和上下文承接。
- AI 策略对话：围绕某一张策略卡片继续追问，也支持自由模式对话。
- 天气与环境展示：展示当前天气、温度、湿度等辅助信息。
- 图表接口：Flask 服务输出蜂群活动预测、环境影响预测、花期概览、蜜源供给概览、错配概览等数据。
- 数据更新脚本：支持按顺序抓取、入库、推导、训练、预测与导出。
- 部署支持：内置 `render.yaml`，可直接部署 Node 版本的策略服务。

## 技术栈

- 前端：原生 HTML、CSS、JavaScript
- 策略服务：Node.js + Express + node-fetch
- 图表服务：Python + Flask + Flask-CORS
- 数据存储：SQLite
- AI：DeepSeek Chat Completions API
- 数据产出：多份 Python 分析脚本 + JSON/CSV 静态导出

## 核心页面

- [dashboard1.html](/d:/homeworks/workshop/s7-8/bee-project/dashboard1.html)
  主仪表盘，承载地图、热力层、顶部环境信息和入口跳转。
- [scenario_ai.html](/d:/homeworks/workshop/s7-8/bee-project/scenario_ai.html)
  场景策略页，展示策略卡片、详情弹层和策略入口。
- [scenario_chat.html](/d:/homeworks/workshop/s7-8/bee-project/scenario_chat.html)
  策略上下文对话页，支持继续细化某个策略或自由提问。
- [ml_monitor.html](/d:/homeworks/workshop/s7-8/bee-project/ml_monitor.html)
  机器学习/监测相关页面。
- [chart_test.html](/d:/homeworks/workshop/s7-8/bee-project/chart_test.html)
  图表联调和接口验证页面。

## 主要服务

### 1. Node 策略服务

入口文件：
- [server.js](/d:/homeworks/workshop/s7-8/bee-project/server.js)

作用：
- 提供策略卡片 API
- 提供 AI 对话 API
- 提供当前天气 API
- 托管静态页面

主要接口：
- `GET /`
- `GET /api/health`
- `GET /api/current-weather`
- `GET /api/scenarios`
- `POST /api/chat`

说明：
- 当配置 `DEEPSEEK_API_KEY` 时，策略卡片会尝试调用 DeepSeek 动态生成。
- 当 AI 超时或失败时，会回退到缓存结果；如果没有缓存，再回退到本地兜底策略。
- 当前版本已经加入策略卡片缓存和超时保护，避免页面首屏长时间阻塞。

### 2. Flask 图表服务

入口文件：
- [chart_api.py](/d:/homeworks/workshop/s7-8/bee-project/chart_api.py)

作用：
- 输出图表型分析数据
- 对接数据库和原始天气文件
- 为监测页、图表页或其它前端模块提供 JSON 数据

主要接口：
- `GET /`
- `GET /api/bee-activity-forecast`
- `GET /api/env-impact-forecast`
- `GET /api/flowering-overview`
- `GET /api/nectar-supply-overview`
- `GET /api/mismatch-overview`

## 数据与分析链路

项目里的 Python 脚本基本按“同步数据 -> 入库 -> 推导指标 -> 训练预测 -> 导出前端文件”的顺序组织。

推荐从总控脚本开始理解：
- [update_all.py](/d:/homeworks/workshop/s7-8/bee-project/update_all.py)

它当前串联的步骤包括：

- `sync_supabase_to_sqlite.py`
- `fetch_qweather_24h.py`
- `fetch_qweather_7d.py`
- `fetch_qweather_history.py`
- `insert_qweather_data_patched.py`
- `insert_qweather_history.py`
- `build_eco_time_series.py`
- `build_bee_activity_curve.py`
- `build_bee_activity_hourly.py`
- `build_bee_env_aligned_hourly.py`
- `derive_flowering_index.py`
- `derive_nectar_supply.py`
- `derive_expected_activity_hourly.py`
- `derive_mismatch_index.py`
- `build_future_expected_activity_hourly.py`
- `train_residual_model.py`
- `predict_future_activity_residual.py`
- `export_static_json.py`
- `export_ml_monitor_data.py`

这些脚本共同产出：

- `bee_env.db`
- `data/` 下的结构化 JSON 和说明文件
- `data_raw/` 下的原始天气抓取结果
- `latest_activity.json`、`latest_activity.csv`

## 目录说明

### 前端与样式

- [dashboard1.html](/d:/homeworks/workshop/s7-8/bee-project/dashboard1.html)
- [scenario_ai.html](/d:/homeworks/workshop/s7-8/bee-project/scenario_ai.html)
- [scenario_chat.html](/d:/homeworks/workshop/s7-8/bee-project/scenario_chat.html)
- [ml_monitor.html](/d:/homeworks/workshop/s7-8/bee-project/ml_monitor.html)
- [styles/scenario-ai.css](/d:/homeworks/workshop/s7-8/bee-project/styles/scenario-ai.css)
- [scripts/scenario-ai.js](/d:/homeworks/workshop/s7-8/bee-project/scripts/scenario-ai.js)
- [scripts/scenario-chat.js](/d:/homeworks/workshop/s7-8/bee-project/scripts/scenario-chat.js)

### 服务与接口

- [server.js](/d:/homeworks/workshop/s7-8/bee-project/server.js)
- [chart_api.py](/d:/homeworks/workshop/s7-8/bee-project/chart_api.py)

### 数据与模型

- [bee_env.db](/d:/homeworks/workshop/s7-8/bee-project/bee_env.db)
- [data](/d:/homeworks/workshop/s7-8/bee-project/data)
- [data_raw](/d:/homeworks/workshop/s7-8/bee-project/data_raw)
- [models](/d:/homeworks/workshop/s7-8/bee-project/models)

### 批处理与辅助脚本

- [update_all.py](/d:/homeworks/workshop/s7-8/bee-project/update_all.py)
- [start_bee_site.bat](/d:/homeworks/workshop/s7-8/bee-project/start_bee_site.bat)
- [update_bee_system.bat](/d:/homeworks/workshop/s7-8/bee-project/update_bee_system.bat)
- [preview_static_site.bat](/d:/homeworks/workshop/s7-8/bee-project/preview_static_site.bat)
- [preview_ml_monitor.bat](/d:/homeworks/workshop/s7-8/bee-project/preview_ml_monitor.bat)

## 本地运行

### 运行 Node 策略服务

要求：
- Node.js 18+

安装依赖：

```powershell
npm install
```

启动：

```powershell
npm start
```

默认地址：

```text
http://localhost:3000
```

### 运行 Flask 图表服务

要求：
- Python 3.10+

安装依赖：

```powershell
pip install -r requirements.txt
```

启动：

```powershell
python chart_api.py
```

### 运行整套数据更新流程

```powershell
python update_all.py
```

## 环境变量

Node 服务当前明确使用：

- `DEEPSEEK_API_KEY`

本地可以在项目根目录创建 `.env`：

```env
DEEPSEEK_API_KEY=your_deepseek_key
```

说明：
- 如果没有这个 key，策略页仍能工作，但会使用缓存结果或本地兜底策略。
- 某些 Python 数据同步脚本可能还依赖你本地已有的其它外部配置或数据库连接信息，这部分取决于你的实际数据来源。

## 部署

项目内置 Render 配置：
- [render.yaml](/d:/homeworks/workshop/s7-8/bee-project/render.yaml)

当前 Render 配置面向 Node 服务：
- 运行时：`node`
- 构建命令：`npm install`
- 启动命令：`npm start`

如果要一起部署 Flask 图表服务，建议单独拆成另一个服务，或者在后续做统一网关整合。

## 适合继续优化的方向

- 统一 Node 与 Flask 接口层，减少双服务维护成本。
- 给策略卡片加“来源标识”，明确区分 `deepseek`、`cache`、`fallback`。
- 整理中文乱码的历史兜底文案编码问题。
- 为数据链路增加更清晰的失败告警和日志。
- 增加自动测试，尤其是 API 返回结构和前端策略页加载流程。

## 文档说明

原来仓库中的旧说明文件和 `.docx` 文档没有被删除，但这份 README 现在应作为新的主说明入口。

如果你要进一步写论文附录、答辩材料或部署手册，建议从 [PROJECT_DOCUMENTATION.md](/d:/homeworks/workshop/s7-8/bee-project/PROJECT_DOCUMENTATION.md) 继续扩展。
