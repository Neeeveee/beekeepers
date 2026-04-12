from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "bee-project-项目完整说明-详细算法版.docx"


CONTENT = """
Bee Project 项目完整说明

一、项目定位

这个项目本质上是一个“蜜蜂行为与生态环境联动预测系统”。它把蜂箱行为数据、气象数据、花期与蜜源知识、规则模型和一个轻量 ML 残差修正模型串成一条完整链路，最后输出可视化页面和静态 JSON。

更具体地说，它想回答三类问题：
1. 现在蜜蜂活动强不强。
2. 未来几天蜜蜂活动会怎么变化。
3. 这种变化和花期、蜜源、天气之间是否存在错配风险。

从代码结构上看，项目不是传统前后端分离 Web 应用，而是“Python 数据管线 + SQLite 业务库 + Flask 只读 API + 静态页面”的组合。

二、整体架构

主更新链路定义在 update_all.py 和 update_bee_system.bat。它们会顺序执行：
- 同步蜂箱行为数据 sync_supabase_to_sqlite.py
- 拉取和风天气 24h / 7d / 历史天气
- 写入 SQLite
- 构建生态时序与蜂行为聚合表
- 推导花期指数、蜜源供给指数、预期蜂行为、错配指数
- 训练残差修正模型
- 生成未来 ML 修正预测
- 导出前端使用的 JSON 与 ML 监控 JSON

前端展示分两套：
- 图表主页面由 chart_api.py + chart_page.js 驱动，支持 api 模式和 static 模式。
- MQTT 实时监控页是 dashboard1.html。
- ML 监控页是 ml_monitor.html + ml_monitor.js。

三、核心数据流

1. 蜂箱行为数据来源
- 一个来源是 MQTT 实时订阅并写库，mqtt_to_sqlite.py 负责订阅 beehive/hive01/10min。
- 另一个来源是 Supabase 回灌同步，sync_supabase_to_sqlite.py 会从云端表 beehive_10min_raw 拉数据并写入本地 bee_counter_raw。

2. 天气数据来源
- fetch_qweather_24h.py
- fetch_qweather_7d.py
- fetch_qweather_history.py

这些脚本把原始天气 JSON 存到 data_raw/，然后 insert_qweather_data_patched.py 和 insert_qweather_history.py 入库。

3. 中间清洗与聚合
- build_bee_activity_curve.py：把原始进出计数整理成活动曲线。
- build_bee_activity_hourly.py：聚成小时级蜂行为数据。
- build_bee_env_aligned_hourly.py：把蜂行为和环境按小时对齐。
- build_eco_time_series.py：建立生态环境时序基础表。

4. 生态指数层
- derive_flowering_index.py：根据植物花期窗口、温湿度、降雨，计算每日花期指数。
- derive_nectar_supply.py：结合花期结果和植物泌蜜能力，计算每日蜜源供给指数。
- derive_expected_activity_hourly.py：按小时构建规则版预期蜂行为。
- derive_mismatch_index.py：比较蜜源供给与行为表现，生成错配风险。

5. 预测层
- build_future_expected_activity_hourly.py：把未来天气映射成未来规则预测。
- train_residual_model.py：训练一个纯 Python ridge 回归残差模型。
- predict_future_activity_residual.py：对未来规则预测做小幅 ML 修正。

6. 导出层
- export_static_json.py：把 API 返回直接导成静态 JSON，供 GitHub Pages 或纯静态部署使用。
- export_ml_monitor_data.py：导出 ML 监控数据。

四、数据库角色

数据库文件是 bee_env.db，它是整个项目的中心，不只是缓存。

当前项目里的主要表包括：
- 基础实体：sites、sensors、plants、site_plant_inventory
- 原始观测：measurements、bee_counter_raw
- 聚合行为：bee_activity_curve、bee_activity_hourly、bee_activity_obs
- 对齐结果：bee_env_aligned_hourly、eco_time_series
- 生态模型结果：flowering_model_daily、nectar_supply_model_daily
- 行为预测：expected_activity_hourly、future_expected_activity_hourly
- 风险结果：mismatch_index_daily
- 展示/索引：flower_resource_index、nectar_availability_index、activity_index
- 模型支持：nectar_plants、nectar_plant_site_weights
- 元数据：schema_version、sync_meta

从实际数据规模看，它目前更像“单蜂场样例系统”，而不是多站点大规模生产系统。

五、算法逻辑

这个项目最有意思的地方是“规则模型先行，ML 只做残差修正”。也就是说，系统先用一套可解释的生态与行为规则生成基线预测，再让机器学习去学习“规则和真实值之间还差多少”。这种设计的优点是：第一，可解释性强；第二，小样本情况下更稳；第三，后期容易继续插入新的业务规则。

从层次上看，算法链路可以分成五层：
第一层是天气与行为的基础整理层。
第二层是花期指数层。
第三层是蜜源供给指数层。
第四层是蜂行为规则预测层。
第五层是 ML 残差修正层。

1. 天气与行为基础整理层

这一层的目标不是直接预测，而是把后面需要的输入先整理好。

蜂行为侧：
- 原始输入来自 bee_counter_raw 表，记录每 10 分钟一个时间桶里的 in_count、out_count、daily_in、daily_out。
- 然后通过 build_bee_activity_curve.py 和 build_bee_activity_hourly.py 聚合为小时级数据。
- 小时级表里会得到 sum_in_count、sum_out_count、avg_activity_value、max_activity_value、min_activity_value 等字段。

环境侧：
- 原始天气来自和风天气，包括 24 小时预报、7 天预报和历史天气。
- 入库后再通过 build_eco_time_series.py 做统一整理。
- 再由 build_bee_env_aligned_hourly.py 把蜂行为与天气按小时对齐，形成 bee_env_aligned_hourly。

这一步的结果非常关键，因为后面的 expected_activity_hourly 就是建立在“行为与天气同时间尺度对齐”之上的。

2. 花期指数模型

花期模型的目标是估计：某一种植物在某一天到底处于多强的开花状态。

它主要使用三类输入：
- 植物自身花期窗口：bloom_start_mmdd、bloom_end_mmdd
- 当日环境：avg_temp_c、avg_humidity_pct、precip_mm
- 植物资源属性：nectar_grade、pollen_grade、confidence

花期模型的核心思想分三步。

第一步，计算季节基线分值 base_flowering_score。
- 系统先判断某一天是否落在某个植物的花期窗口内。
- 如果不在窗口内，不直接给 0，而是给一个很低的基础值，表示自然界不是绝对突变，而是有过渡。
- 如果在窗口内，则根据当前日期在花期窗口里的进度，划分成启动期、上升期、盛花期、回落期、尾期，不同阶段给不同的基准值。

第二步，计算环境修正因子。
- 温度因子 temp_factor：温度太低会抑制开花活性，适中温度最优，过高也会回落。
- 湿度因子 humidity_factor：适中湿度最好，过干或过湿都可能不理想。
- 降雨因子 rain_factor：无雨最好，小雨轻微抑制，中大雨明显抑制。

第三步，计算资源属性修正。
- 这里不是判断“有没有花”，而是判断“这种植物即使开花，对蜂场的重要性有多大”。
- nectar_grade 越高，说明泌蜜价值越高。
- pollen_grade 越高，说明花粉价值越高。
- confidence 越低，说明资料可信度不高，会适当下调。

最终，花期指数大致可以理解为：
flowering_index = base_flowering_score × env_modifier × resource_factor

其中 env_modifier 是温度、湿度、降雨三个因子的加权组合。

最后 flowering_index 会被截断到 0 到 1 之间，并映射成阶段标签，例如未开花、花期启动、初花期、盛花期、最佳花期等。

3. 蜜源供给指数模型

花期指数说明“花开得怎么样”，但它还不完全等价于“蜜蜂能采到多少蜜”。所以项目单独做了一层 nectar_supply_index。

这一层主要使用：
- 上一层输出的 flowering_index
- 植物泌蜜能力相关属性：nectar_grade、avg_yield_kg_per_colony、confidence
- 同样的天气修正：温度、湿度、降雨

它的逻辑是：
第一，先根据植物天然泌蜜能力计算 nectar_resource_factor。
- nectar_grade 越高，基础泌蜜能力越强。
- avg_yield_kg_per_colony 越高，说明作为蜜源植物的价值越高。
- confidence 越低，会做保守折减。

第二，再计算环境修正。
- 花开了不代表当天适合泌蜜。
- 温度不合适、湿度太高或太低、降雨过多，都会影响蜜源可利用程度。

第三，把花期强度和泌蜜能力乘起来，得到蜜源供给指数。

可以把它理解成：
nectar_supply_index = flowering_index × nectar_resource_modifier × env_modifier

这一步特别重要，因为它把“植物生态状态”进一步翻译成了“对蜜蜂真正可利用的资源强度”。

4. 蜂行为规则预测模型

这一层是整个系统的核心，因为它直接输出 expected_activity。

它使用的输入包括：
- 小时 hour
- 温度 temperature_c
- 湿度 humidity_pct
- 风速 wind_speed_ms
- 降雨 precip_mm
- 每日花期指数 daily_flowering_index
- 每日蜜源供给指数 daily_nectar_supply_index

它的结构非常清楚，可以拆成三个部分。

第一部分，日内基础行为曲线 base_activity。
- 系统默认蜜蜂在凌晨和夜间活动接近 0。
- 白天逐渐升高，中午前后达到高点，之后回落。
- 这部分不是用硬编码阶梯，而是用一个近似高斯曲线的函数表示，所以变化更平滑。
- 这意味着即便天气完全相同，上午 9 点和下午 3 点的理论行为基线也不一样。

第二部分，天气修正 weather_modifier。
- 温度因子：低于阈值时几乎直接抑制飞行，适温时最优，高温时回落。
- 湿度因子：中等湿度最好。
- 风速因子：风越大，飞行越困难；超过上限后可以直接压到 0。
- 降雨因子：雨对外出采集影响很大，降雨越强抑制越明显。

这一层的一个关键保守规则是：
- 如果基础曲线为 0，或者温度太差、风太大、降雨过强达到硬抑制条件，expected_activity 会直接归零。
- 这体现了项目不是只做连续回归，而是有明确的行为边界条件。

第三部分，资源修正 resource_factor。
- daily_flowering_index 反映“当天花开得如何”。
- daily_nectar_supply_index 反映“当天资源对蜜蜂是否真正可用”。
- 系统先把它们分别映射成 flower_factor 和 nectar_factor，再做组合，得到最终 resource_factor。

最终，规则行为预测大致可以理解为：
expected_activity = base_activity × weather_modifier × resource_factor

同时，系统还会把真实观测的 avg_activity_value 做归一化，得到 actual_activity，用于后续训练残差模型。

5. 错配风险模型

错配模型不是单纯看“活动高不高”，而是看“资源和行为是否同步”。

它比较的核心量是：
- nectar_supply_index：资源侧有多强
- behavior_index_norm 或 expected / actual activity：行为侧有多强

如果资源高、行为低：
- 可能意味着花源已经起来了，但蜂群行为还没有跟上。
- 这类情况会被归入 resource_ahead 一类。

如果行为高、资源低：
- 可能意味着蜂群仍在积极外出，但可用资源其实不够。
- 这类情况会被归入另一类错配。

系统随后根据 gap 大小，再映射成轻度、中度、显著错配等等级，最终用于前端的风险展示。

从业务表达上，这层很有价值，因为它已经从“预测数值”上升到了“管理判断”。

6. ML 残差修正模型

ML 模型不是替代规则模型，而是学习规则模型还没解释到的偏差。

训练目标不是直接拟合 actual_activity，而是拟合：
residual = actual_activity - expected_activity

也就是说，模型问的是：
“在当前小时、天气和资源条件下，规则模型通常会高估还是低估多少？”

使用的特征包括：
- hour
- temperature_c
- humidity_pct
- wind_speed_ms
- precip_mm
- base_activity
- temp_factor
- humidity_factor
- wind_factor
- rain_factor
- weather_modifier
- daily_flowering_index
- daily_nectar_supply_index
- flower_factor
- nectar_factor
- resource_factor
- expected_activity

模型形式是纯 Python 实现的 ridge regression。

选择 ridge regression 的好处是：
- 它是线性的，可解释。
- 对小样本更稳。
- 带正则化，不容易因为样本少就把权重学得过激。
- 不依赖复杂第三方机器学习框架，部署和迁移都简单。

训练完成后会保存：
- 模型系数 coefficients
- 每个特征的均值和标准差 feature_stats
- 样本量 sample_count
- 评估指标 metrics

7. ML 推理保护机制

这是你项目里非常值得讲的一点，因为它体现了工程谨慎性。

项目没有把 ML 输出直接无条件叠加，而是加了 guard 保护：
- 样本数不足时，confidence_scale 会小于 1。
- 同时设置 low_cap、medium_cap、high_cap 三档修正上限。
- 真正能加到规则预测上的修正量，会被同时乘以置信缩放并裁剪到上限范围内。

这意味着即便模型原始判断说“应该修正 0.20”，如果当前样本量不够，最终可能只允许修正 0.04 或 0.08。

所以未来预测的最终结果更准确地说不是：
最终预测 = 规则预测 + 原始 ML 输出

而是：
最终预测 = 规则预测 + 受保护约束后的残差修正

这一步非常重要，它防止了小样本条件下 ML 破坏规则模型的稳定性。

8. 为什么这套算法结构合理

如果从工程角度评价，这套结构的合理性在于：
- 规则模型保证下限，确保无论样本多少，系统都能产出可解释结果。
- 花期、蜜源、行为三层分开建模，符合生态逻辑，也方便后期分别调参。
- ML 只修正残差，降低了对大样本和复杂模型的依赖。
- 保护机制保证 ML 不会在样本少时“乱修”。

换句话说，这个项目不是追求“最炫的黑箱预测”，而是在追求“足够可信、足够稳、可以给人解释”的预测系统。

六、前端展示

主图表接口包括：
- /api/bee-activity-forecast
- /api/env-impact-forecast
- /api/flowering-overview
- /api/nectar-supply-overview
- /api/mismatch-overview

前端支持：
- API 模式
- 静态 JSON 模式
- 小时 / 日粒度切换
- 历史值与未来值桥接显示
- 当前主导植物与未来主导植物展示

这说明前端的核心不是复杂交互，而是把分析结果讲清楚。

七、当前业务设定

从数据库和种子脚本看，当前业务背景是：
- 站点是“杭州蜂场 A”
- 经纬度在杭州附近
- 蜜源植物是杭州春季场景定制过的
- 已经有站点植物显示权重这层定制，说明图表不是简单平均，而是尝试贴近真实蜂场经验

八、项目优点

- 链路完整，从采集到展示是闭环。
- 规则模型可解释，适合答辩和汇报。
- 有 SQLite 落地，便于离线演示。
- 支持 API 和静态 JSON 两种展示模式，部署灵活。
- ML 部分比较克制，不会喧宾夺主。
- 脚本虽多，但基本按处理阶段拆开了。

九、当前局限

- 目前明显是单站点、小样本系统，泛化能力有限。
- ML 训练样本量还不大，修正幅度需要保守。
- requirements.txt 依赖写得偏少，和实际运行脚本不完全一致。
- 一些配置是硬编码的，比如 Python 路径、MQTT 账号、Supabase 地址等。
- README.md 目前几乎为空，项目价值还没有被文档充分表达出来。
- 终端下部分中文注释有乱码，说明编码环境还需要统一。

十、一句话总结

这是一个面向蜂场场景的蜜蜂活动预测与生态错配分析系统。它把蜂箱进出行为、天气和蜜源植物花期数据整合进 SQLite，通过可解释规则模型生成花期指数、蜜源供给指数和蜂行为预测，再用轻量 ML 对预测残差做保守修正，最后通过 Flask API 和静态图表页面展示历史趋势、未来预测和错配风险。
""".strip()


def xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def build_document_xml(content: str) -> str:
    paragraphs = []
    for line in content.splitlines():
        escaped = xml_escape(line)
        if line.strip():
            paragraphs.append(
                "<w:p><w:r><w:rPr><w:rFonts w:ascii=\"Calibri\" w:hAnsi=\"Calibri\" "
                "w:eastAsia=\"Microsoft YaHei\"/></w:rPr><w:t xml:space=\"preserve\">"
                f"{escaped}</w:t></w:r></w:p>"
            )
        else:
            paragraphs.append("<w:p/>")

    body = "".join(paragraphs)
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<w:document xmlns:wpc=\"http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas\" "
        "xmlns:mc=\"http://schemas.openxmlformats.org/markup-compatibility/2006\" "
        "xmlns:o=\"urn:schemas-microsoft-com:office:office\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\" "
        "xmlns:m=\"http://schemas.openxmlformats.org/officeDocument/2006/math\" "
        "xmlns:v=\"urn:schemas-microsoft-com:vml\" "
        "xmlns:wp14=\"http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing\" "
        "xmlns:wp=\"http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing\" "
        "xmlns:w10=\"urn:schemas-microsoft-com:office:word\" "
        "xmlns:w=\"http://schemas.openxmlformats.org/wordprocessingml/2006/main\" "
        "xmlns:w14=\"http://schemas.microsoft.com/office/word/2010/wordml\" "
        "xmlns:wpg=\"http://schemas.microsoft.com/office/word/2010/wordprocessingGroup\" "
        "xmlns:wpi=\"http://schemas.microsoft.com/office/word/2010/wordprocessingInk\" "
        "xmlns:wne=\"http://schemas.microsoft.com/office/word/2006/wordml\" "
        "xmlns:wps=\"http://schemas.microsoft.com/office/word/2010/wordprocessingShape\" "
        "mc:Ignorable=\"w14 wp14\">"
        f"<w:body>{body}<w:sectPr>"
        "<w:pgSz w:w=\"11906\" w:h=\"16838\"/>"
        "<w:pgMar w:top=\"1440\" w:right=\"1440\" w:bottom=\"1440\" w:left=\"1440\" "
        "w:header=\"708\" w:footer=\"708\" w:gutter=\"0\"/>"
        "</w:sectPr></w:body></w:document>"
    )


def build_content_types_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>"""


def build_root_rels_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""


def build_document_rels_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>"""


def build_core_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Bee Project 项目完整说明</dc:title>
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
</cp:coreProperties>"""


def build_app_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Codex</Application>
</Properties>"""


def main() -> None:
    document_xml = build_document_xml(CONTENT)
    with ZipFile(OUTPUT_PATH, "w", ZIP_DEFLATED) as docx:
        docx.writestr("[Content_Types].xml", build_content_types_xml())
        docx.writestr("_rels/.rels", build_root_rels_xml())
        docx.writestr("docProps/core.xml", build_core_xml())
        docx.writestr("docProps/app.xml", build_app_xml())
        docx.writestr("word/document.xml", document_xml)
        docx.writestr("word/_rels/document.xml.rels", build_document_rels_xml())

    print(f"Created: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
