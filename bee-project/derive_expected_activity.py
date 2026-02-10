
import sqlite3  # 导入SQLite模块
from datetime import datetime  # 导入时间模块

DB_PATH = "bee_env.db"  # 数据库文件名（默认与脚本同目录）

def fill_tpl(tpl, temp, rh, rain, wind):  # 填模板变量
    text = tpl or ""  # 模板可能为空
    text = text.replace("{temp}", f"{temp:.1f}" if temp is not None else "NA")  # 替换温度
    text = text.replace("{rh}", f"{rh:.0f}" if rh is not None else "NA")  # 替换湿度
    text = text.replace("{rain}", f"{rain:.1f}" if rain is not None else "NA")  # 替换降水
    text = text.replace("{wind}", f"{wind:.1f}" if wind is not None else "NA")  # 替换风速
    return text  # 返回文本

def calc_match_score(obs, exp_min, exp_max):  # 计算匹配分数(0~1)
    if obs is None or exp_min is None or exp_max is None:  # 缺数据
        return None  # 返回空
    if exp_min <= obs <= exp_max:  # 在范围内
        return 1.0  # 满分
    span = (exp_max - exp_min) if (exp_max - exp_min) != 0 else 1.0  # 防止除0
    if obs < exp_min:  # 低于下界
        return max(0.0, 1.0 - (exp_min - obs) / span)  # 线性扣分
    return max(0.0, 1.0 - (obs - exp_max) / span)  # 高于上界扣分

def deviation_tag(obs, exp_min, exp_max):  # 生成偏离标签
    if obs is None or exp_min is None or exp_max is None:  # 缺数据
        return "no_data"  # 无数据
    if obs < exp_min:  # 低于预期
        return "below_expected"  # 标签
    if obs > exp_max:  # 高于预期
        return "above_expected"  # 标签
    return "within_expected"  # 标签

def get_daily_env(conn, site_id, day_str):  # 读取某天的环境汇总
    row = conn.execute(  # 查询 daily_weather_summary
        """
        SELECT
          day_avg_temp_c,
          day_avg_humidity_pct,
          day_sum_precip_mm,
          day_avg_wind_ms
        FROM daily_weather_summary
        WHERE site_id = ? AND date = ?;
        """,
        (site_id, day_str),
    ).fetchone()  # 获取一行
    return row  # 返回(row或None)

def pick_rule(conn, site_id, species_code, day_str, temp, rh, rain, wind):  # 按环境挑规则
    month = int(day_str.split("-")[1])  # 取月份
    row = conn.execute(  # 查询最匹配规则（先按区间过滤，再按置信度排序）
        """
        SELECT
          id,
          expected_min, expected_max,
          peak_start_hour, peak_end_hour,
          confidence,
          explain_tpl
        FROM behavior_rule_library
        WHERE site_id = ?
          AND species_code = ?
          AND (month_from IS NULL OR month_from <= ?)
          AND (month_to IS NULL OR month_to >= ?)
          AND (temp_min IS NULL OR temp_min <= ?)
          AND (temp_max IS NULL OR temp_max >= ?)
          AND (rh_min IS NULL OR rh_min <= ?)
          AND (rh_max IS NULL OR rh_max >= ?)
          AND (precip_max IS NULL OR precip_max >= ?)
          AND (wind_max IS NULL OR wind_max >= ?)
        ORDER BY confidence DESC
        LIMIT 1;
        """,
        (site_id, species_code, month, month, temp, temp, rh, rh, rain, wind),
    ).fetchone()  # 获取一行
    return row  # 返回(规则或None)

def get_daily_observed_activity(conn, site_id, species_code, day_str):  # 计算某天实测活跃度（日均）
    row = conn.execute(  # 查询当天平均活跃度
        """
        SELECT AVG(activity_index)
        FROM bee_activity_obs
        WHERE site_id = ?
          AND species_code = ?
          AND substr(obs_time, 1, 10) = ?;
        """,
        (site_id, species_code, day_str),
    ).fetchone()  # 获取一行
    return row[0] if row and row[0] is not None else None  # 返回值或None

def run_one_day(site_id, species_code, day_str):  # 跑一天推导
    conn = sqlite3.connect(DB_PATH)  # 连接数据库

    env = get_daily_env(conn, site_id, day_str)  # 取环境
    if not env:  # 如果没有环境汇总
        print("⚠ 没有 daily_weather_summary 数据：先跑 build_daily_weather_summary.py")  # 提示
        conn.close()  # 关闭
        return  # 退出

    temp, rh, rain, wind = env  # 解包环境
    rule = pick_rule(conn, site_id, species_code, day_str, temp, rh, rain, wind)  # 选规则
    if not rule:  # 没命中
        print("⚠ 未命中行为规律：先往 behavior_rule_library 填几条规则")  # 提示
        conn.close()  # 关闭
        return  # 退出

    rule_id, exp_min, exp_max, p_start, p_end, conf, tpl = rule  # 解包规则
    explain = fill_tpl(tpl, temp, rh, rain, wind)  # 生成解释文本

    # 1) 写入/更新每日预期输出（UPSERT）
    conn.execute(  # 写入 expected_activity_daily
        """
        INSERT INTO expected_activity_daily
          (site_id, species_code, date, env_temp, env_rh, env_rain, env_wind,
           rule_id, expected_min, expected_max, peak_start_hour, peak_end_hour, confidence, explain_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, species_code, date) DO UPDATE SET
          env_temp=excluded.env_temp,
          env_rh=excluded.env_rh,
          env_rain=excluded.env_rain,
          env_wind=excluded.env_wind,
          rule_id=excluded.rule_id,
          expected_min=excluded.expected_min,
          expected_max=excluded.expected_max,
          peak_start_hour=excluded.peak_start_hour,
          peak_end_hour=excluded.peak_end_hour,
          confidence=excluded.confidence,
          explain_text=excluded.explain_text,
          created_at=datetime('now', 'localtime');
        """,
        (site_id, species_code, day_str, temp, rh, rain, wind, rule_id, exp_min, exp_max, p_start, p_end, conf, explain),
    )  # 执行写入

    # 2) 如果有实测活跃度，就做验证；没有也照样写一条 no_data（前端能显示“暂无法验证”）
    obs = get_daily_observed_activity(conn, site_id, species_code, day_str)  # 取实测
    score = calc_match_score(obs, exp_min, exp_max)  # 算分
    tag = deviation_tag(obs, exp_min, exp_max)  # 标签

    if tag == "within_expected":  # 一致
        explain2 = "实测活跃度落在预期范围内，表现与该场地规律一致。"  # 解释
        dev_val = 0.0  # 偏离量
    elif tag == "below_expected":  # 偏低
        explain2 = "实测低于预期：可能是花源不足、降雨/风干扰、蜂群健康压力等（建议结合现场检查）。"  # 解释
        dev_val = (exp_min - obs) if obs is not None else None  # 偏离量
    elif tag == "above_expected":  # 偏高
        explain2 = "实测高于预期：可能遇到强蜜源窗口或短期刺激因素，可结合现场观察确认。"  # 解释
        dev_val = (obs - exp_max) if obs is not None else None  # 偏离量
    else:  # no_data
        explain2 = "缺少实测活跃度数据，暂无法验证一致性（但预期范围已生成，可用于曲线模板）。"  # 解释
        dev_val = None  # 偏离量

    conn.execute(  # 写入 validation_activity_daily
        """
        INSERT INTO validation_activity_daily
          (site_id, species_code, date, observed_activity, expected_min, expected_max,
           match_score, deviation_tag, deviation_value, explain_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, species_code, date) DO UPDATE SET
          observed_activity=excluded.observed_activity,
          expected_min=excluded.expected_min,
          expected_max=excluded.expected_max,
          match_score=excluded.match_score,
          deviation_tag=excluded.deviation_tag,
          deviation_value=excluded.deviation_value,
          explain_text=excluded.explain_text,
          created_at=datetime('now', 'localtime');
        """,
        (site_id, species_code, day_str, obs, exp_min, exp_max, score, tag, dev_val, explain2),
    )  # 执行写入

    conn.commit()  # 提交
    conn.close()  # 关闭
    print(f"✅ 推导完成：site_id={site_id}, species={species_code}, date={day_str}")  # 提示

if __name__ == "__main__":  # 入口
    # 你第一次跑的时候，把下面三项改成你的真实值：
    # 1) site_id：看 sites 表里的 id
    # 2) species_code：比如 CHINESE_BEE
    # 3) date：比如 2026-01-22
    run_one_day(site_id=1, species_code="CHINESE_BEE", day_str="2026-01-22")  # 示例
