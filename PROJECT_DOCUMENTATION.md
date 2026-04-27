# Project Documentation

## 1. 项目定位

`bee-project` 是一个围绕“蜜蜂授粉行为 + 花期与蜜源时序 + 环境影响 + 错配诊断 + AI 辅助建议”构建的综合型原型系统。

它不只是一个前端展示页，而是一条从原始数据到策略输出的完整链路：

1. 获取原始环境/天气/业务数据
2. 对齐并落库到 SQLite
3. 推导花期、蜜源、活动、错配等指标
4. 输出可视化所需 JSON 与图表接口
5. 生成场景策略卡片
6. 基于策略上下文进入 AI 对话

这个结构很适合课程项目、研究型原型、展示型作品或后续扩展成正式决策支持平台。

## 2. 系统架构

### 2.1 架构分层

项目可以拆成以下四层：

### 数据采集层

负责把外部数据拉进项目：

- 天气抓取脚本
- 数据库同步脚本
- 历史数据写入脚本

典型文件：

- [fetch_qweather_24h.py](/d:/homeworks/workshop/s7-8/bee-project/fetch_qweather_24h.py)
- [fetch_qweather_7d.py](/d:/homeworks/workshop/s7-8/bee-project/fetch_qweather_7d.py)
- [fetch_qweather_history.py](/d:/homeworks/workshop/s7-8/bee-project/fetch_qweather_history.py)
- [sync_supabase_to_sqlite.py](/d:/homeworks/workshop/s7-8/bee-project/sync_supabase_to_sqlite.py)

### 数据加工与建模层

负责把原始数据变成可解释指标：

- 花期指数
- 蜜源供给指数
- 蜂群活动指数
- 预期活动曲线
- 错配指数
- 未来预测和残差修正

典型文件：

- [build_eco_time_series.py](/d:/homeworks/workshop/s7-8/bee-project/build_eco_time_series.py)
- [build_bee_activity_curve.py](/d:/homeworks/workshop/s7-8/bee-project/build_bee_activity_curve.py)
- [build_bee_activity_hourly.py](/d:/homeworks/workshop/s7-8/bee-project/build_bee_activity_hourly.py)
- [build_bee_env_aligned_hourly.py](/d:/homeworks/workshop/s7-8/bee-project/build_bee_env_aligned_hourly.py)
- [derive_flowering_index.py](/d:/homeworks/workshop/s7-8/bee-project/derive_flowering_index.py)
- [derive_nectar_supply.py](/d:/homeworks/workshop/s7-8/bee-project/derive_nectar_supply.py)
- [derive_expected_activity_hourly.py](/d:/homeworks/workshop/s7-8/bee-project/derive_expected_activity_hourly.py)
- [derive_mismatch_index.py](/d:/homeworks/workshop/s7-8/bee-project/derive_mismatch_index.py)
- [build_future_expected_activity_hourly.py](/d:/homeworks/workshop/s7-8/bee-project/build_future_expected_activity_hourly.py)
- [train_residual_model.py](/d:/homeworks/workshop/s7-8/bee-project/train_residual_model.py)
- [predict_future_activity_residual.py](/d:/homeworks/workshop/s7-8/bee-project/predict_future_activity_residual.py)

### 服务接口层

分成两个服务：

#### Node / Express

负责页面托管、天气接口、策略卡片与 AI 对话：

- [server.js](/d:/homeworks/workshop/s7-8/bee-project/server.js)

#### Flask

负责图表分析接口：

- [chart_api.py](/d:/homeworks/workshop/s7-8/bee-project/chart_api.py)

### 交互展示层

由原生前端页面组成：

- [dashboard1.html](/d:/homeworks/workshop/s7-8/bee-project/dashboard1.html)
- [scenario_ai.html](/d:/homeworks/workshop/s7-8/bee-project/scenario_ai.html)
- [scenario_chat.html](/d:/homeworks/workshop/s7-8/bee-project/scenario_chat.html)
- [ml_monitor.html](/d:/homeworks/workshop/s7-8/bee-project/ml_monitor.html)

## 3. 页面职责说明

### 3.1 `dashboard1.html`

这是系统主入口，偏“监测驾驶舱”定位。

主要职责：

- 展示地图主体
- 切换热力层
- 显示时间、天气、温度、湿度、场地信息
- 承接去往其它模块的入口

你可以把它理解成“总览页”。

### 3.2 `scenario_ai.html`

这是策略建议模块的主页面。

主要职责：

- 展示策略卡片列表
- 展示策略详情弹层
- 支持进入特定策略上下文
- 支持进入自由 AI 对话

当前版本里，策略卡片的加载逻辑已经做了优化：

- 页面先渲染本地默认卡片
- 如果浏览器里有上次缓存，则优先恢复缓存卡片
- 再异步请求 `/api/scenarios`
- 如果 AI 返回成功，则刷新成最新策略

这样就不会因为等待 AI 接口导致页面首屏卡住。

### 3.3 `scenario_chat.html`

这是策略对话页。

它支持两种模式：

- 策略上下文模式：围绕某张策略卡片继续提问
- 自由模式：不绑定具体策略，自由咨询

上下文来源主要依赖：

- URL 参数
- `sessionStorage`
- `/api/scenarios` 返回的数据

### 3.4 `ml_monitor.html`

这是监测/模型相关页面，适合展示更加偏数据分析和预测侧的结果。

## 4. API 说明

## 4.1 Node 接口

### `GET /api/health`

作用：
- 健康检查
- 判断 DeepSeek key 是否存在

返回示例：

```json
{
  "ok": true,
  "hasDeepSeekKey": true
}
```

### `GET /api/current-weather`

作用：
- 返回最新天气快照
- 数据来自 `data_raw` 中最近的 `qweather_24h_*.json`

### `GET /api/scenarios`

作用：
- 返回策略卡片数据

返回来源分三种：

- `deepseek`
- `cache`
- `fallback`

当前逻辑：

1. 如果存在有效缓存且未过期，直接返回缓存
2. 否则尝试请求 DeepSeek
3. DeepSeek 成功则写入缓存并返回
4. DeepSeek 失败则优先返回缓存
5. 没有缓存时再返回本地兜底卡片

这就是为什么现在策略页首屏速度比之前快。

### `POST /api/chat`

作用：
- 处理 AI 对话
- 把历史消息和当前策略上下文打包发送给 DeepSeek

请求体核心字段：

- `message`
- `history`
- `scenarioContext`

## 4.2 Flask 接口

### `GET /api/bee-activity-forecast`

输出蜂群活动相关预测结果。

### `GET /api/env-impact-forecast`

输出环境因子影响分析。

### `GET /api/flowering-overview`

输出花期总览与花期指数相关数据。

### `GET /api/nectar-supply-overview`

输出蜜源供给总览。

### `GET /api/mismatch-overview`

输出生态错配概览。

## 5. 数据文件与目录说明

## 5.1 `data/`

这里存放面向前端和分析消费的中间结果、导出结果和说明文件。

通常包括：

- 花期概览
- 蜜源供给概览
- 错配概览
- 行为预测结果
- 说明性 Markdown

## 5.2 `data_raw/`

这里存放原始抓取文件，例如：

- 24 小时天气
- 7 天天气
- 历史天气

这些文件通常是接口返回的原始 JSON。

## 5.3 `bee_env.db`

这是项目的核心数据库文件。

作用：

- 存环境与行为相关表
- 存预测中间结果
- 为 Flask 图表接口和部分规则模型提供数据来源

## 5.4 `models/`

这里主要用于存放模型或训练产物。

## 6. 更新流程说明

最关键的总控脚本是：

- [update_all.py](/d:/homeworks/workshop/s7-8/bee-project/update_all.py)

它按固定顺序执行整条更新流程。

简化理解如下：

1. 同步业务数据到 SQLite
2. 抓取实时与预报天气
3. 将天气数据写回本地数据库
4. 构建生态时间序列
5. 推导蜂群行为与环境对齐数据
6. 计算花期、蜜源与错配指标
7. 生成未来预期活动
8. 训练残差模型并做未来修正
9. 导出给前端和监测页面使用的 JSON

如果你的目标是“刷新整个系统的数据”，通常直接运行它就够了。

## 7. 当前版本的重要改动

这是当前 README 所基于的版本特征。

### 7.1 策略卡片性能优化

之前的问题：

- 页面要等 `/api/scenarios` 完成后才渲染卡片
- DeepSeek 慢时，整个策略页会显得卡

现在的行为：

- 页面先用默认卡片秒开
- 如果本地有缓存，优先显示缓存
- 后台再请求最新策略

### 7.2 AI 策略缓存

服务端现在会把 DeepSeek 生成结果写入：

- `.cache/scenarios.json`

缓存过期时间：

- 30 分钟

好处：

- 减少重复请求 AI
- 降低策略页首屏等待时间
- AI 接口偶发失败时仍然有“上一次可用结果”

### 7.3 DeepSeek 超时控制

Node 服务对 DeepSeek 请求加入了超时保护，避免长时间挂起导致前端感知非常差。

## 8. 运行与部署建议

## 8.1 开发环境建议

建议本地至少准备：

- Node.js 18 或更高版本
- Python 3.10 或更高版本
- 可用的 `DEEPSEEK_API_KEY`

## 8.2 本地启动建议

建议分两类启动：

### 页面与策略服务

```powershell
npm install
npm start
```

### 图表服务

```powershell
pip install -r requirements.txt
python chart_api.py
```

## 8.3 部署建议

当前 `render.yaml` 只覆盖了 Node 服务。如果你后续要完整上线：

- Node 服务部署策略页和主页面
- Flask 服务单独部署图表接口
- 或者把图表接口逐步迁移到 Node，减少维护复杂度

## 9. 目前已知问题

项目里还有一些历史问题值得后续清理：

- `server.js` 里部分中文兜底文案存在编码乱码
- 前后端仍然是双服务结构，维护成本偏高
- Python 侧部分脚本依赖关系隐含，需要后续整理
- 自动化测试基本缺失
- 环境变量与外部依赖还没有完全收敛成一份统一配置手册

## 10. 后续建议

如果这个项目还要继续做，我会建议优先处理下面四件事：

### 第一优先级

- 修复 Node 侧历史中文乱码
- 给所有 API 加统一日志和错误结构
- 给策略卡片标出来源：AI、缓存、兜底

### 第二优先级

- 整理 Python 脚本的输入输出依赖
- 补一个完整的 `.env.example`
- 把运行方式固化成一键脚本或 Makefile 风格入口

### 第三优先级

- 给图表和策略页补测试
- 做更清晰的部署拆分
- 把项目说明与论文描述进一步统一

## 11. 文档使用建议

如果你只是想快速上手：

- 先看 [README.md](/d:/homeworks/workshop/s7-8/bee-project/README.md)

如果你要详细讲项目、交接项目、写答辩材料或论文附录：

- 重点看这份 [PROJECT_DOCUMENTATION.md](/d:/homeworks/workshop/s7-8/bee-project/PROJECT_DOCUMENTATION.md)

这份文档已经可以作为当前版本的主项目说明使用，原来的旧说明文件可以不再作为主入口。
