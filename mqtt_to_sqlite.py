# -*- coding: utf-8 -*-  # 指定源码编码为 UTF-8

import json  # 用于解析 MQTT 收到的 JSON 数据
import ssl  # 用于 MQTT TLS 加密连接
import sqlite3  # 用于操作 SQLite 数据库
from datetime import datetime  # 用于生成状态日志时间
from pathlib import Path  # 用于处理数据库文件路径

import certifi  # 用于提供系统可信 CA 证书
import paho.mqtt.client as mqtt  # MQTT 客户端库


DB_PATH = r"D:\homeworks\workshop\s7-8\bee-project\bee_env.db"  # 你的 SQLite 数据库路径
MQTT_HOST = "h1b01eed.ala.asia-southeast1.emqxsl.com"  # MQTT Broker 地址
MQTT_PORT = 8883  # MQTT TLS 端口
MQTT_USERNAME = "bee01"  # MQTT 用户名
MQTT_PASSWORD = "12345678"  # MQTT 密码
DATA_TOPIC = "beehive/hive01/10min"  # 蜜蜂 10 分钟统计数据 topic
STATUS_TOPIC = "beehive/hive01/status"  # 设备状态 topic（当前脚本只打印，不写库）
DEFAULT_DEVICE_ID = "hive01"  # 默认设备编号
DEFAULT_HIVE_ID = "hive01"  # 默认蜂箱编号
DEFAULT_FARM_NAME = "当前测试蜂场"  # 默认蜂场名称
DEFAULT_BUCKET_SEC = 600  # 默认时间桶长度，10 分钟 = 600 秒


def log(message: str) -> None:  # 定义一个简单日志函数
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")  # 打印带时间戳的日志


def get_db_connection() -> sqlite3.Connection:  # 获取数据库连接
    db_file = Path(DB_PATH)  # 把数据库路径转成 Path 对象
    if not db_file.exists():  # 如果数据库文件不存在
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")  # 直接抛出异常
    conn = sqlite3.connect(DB_PATH)  # 连接 SQLite 数据库
    conn.execute("PRAGMA journal_mode=WAL;")  # 开启 WAL 模式，提高并发稳定性
    return conn  # 返回数据库连接


def parse_device_id_from_topic(topic: str) -> str:  # 从 topic 中解析设备编号
    parts = topic.split("/")  # 按 / 拆分 topic
    if len(parts) >= 2:  # 如果 topic 至少有两段
        return parts[1]  # 第二段就是 hive01 这种设备编号
    return DEFAULT_DEVICE_ID  # 如果解析失败，则回退到默认设备编号


def insert_bee_counter_data(topic: str, payload_text: str) -> None:  # 把一条蜜蜂计数数据写入数据库
    data = json.loads(payload_text)  # 把 JSON 字符串解析成字典

    event_time = data.get("time")  # 读取时间字段
    in_count = data.get("in")  # 读取当前 10 分钟进入数量
    out_count = data.get("out")  # 读取当前 10 分钟离开数量
    daily_in = data.get("daily_in")  # 读取当天累计进入数量
    daily_out = data.get("daily_out")  # 读取当天累计离开数量

    if not event_time:  # 如果没有时间字段
        raise ValueError("消息缺少 time 字段")  # 直接报错
    if in_count is None:  # 如果没有 in 字段
        raise ValueError("消息缺少 in 字段")  # 直接报错
    if out_count is None:  # 如果没有 out 字段
        raise ValueError("消息缺少 out 字段")  # 直接报错

    device_id = parse_device_id_from_topic(topic)  # 从 topic 解析设备编号
    hive_id = device_id  # 当前先让蜂箱编号等于设备编号
    farm_name = DEFAULT_FARM_NAME  # 当前先使用默认蜂场名称

    conn = get_db_connection()  # 获取数据库连接
    try:  # 开始数据库写入
        cursor = conn.cursor()  # 创建游标
        cursor.execute(  # 执行插入语句
            """
            INSERT OR IGNORE INTO bee_counter_raw
            (
                device_id,
                hive_id,
                farm_name,
                topic,
                event_time,
                in_count,
                out_count,
                daily_in,
                daily_out,
                bucket_sec,
                status,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                device_id,  # 写入设备编号
                hive_id,  # 写入蜂箱编号
                farm_name,  # 写入蜂场名称
                topic,  # 写入 topic
                event_time,  # 写入事件时间
                int(in_count),  # 写入 in_count
                int(out_count),  # 写入 out_count
                int(daily_in) if daily_in is not None else None,  # 写入 daily_in
                int(daily_out) if daily_out is not None else None,  # 写入 daily_out
                DEFAULT_BUCKET_SEC,  # 写入时间桶长度
                None,  # 数据 topic 不写状态字段
                payload_text,  # 原始 JSON 原文写入 raw_json
            ),
        )
        conn.commit()  # 提交事务

        if cursor.rowcount == 0:  # 如果 rowcount 为 0，表示被唯一索引拦住，没有真正插入
            log(f"重复数据，已跳过：device_id={device_id}, event_time={event_time}")  # 打印重复跳过日志
        else:  # 否则说明插入成功
            log(f"写入成功：device_id={device_id}, event_time={event_time}, in={in_count}, out={out_count}")  # 打印成功日志
    finally:  # 无论成功还是失败都关闭连接
        conn.close()  # 关闭数据库连接


def on_connect(client, userdata, flags, reason_code, properties=None):  # MQTT 连接成功后的回调
    if reason_code == 0:  # reason_code 为 0 表示连接成功
        log("MQTT 连接成功")  # 打印连接成功日志
        client.subscribe(DATA_TOPIC, qos=0)  # 订阅 10 分钟数据 topic
        client.subscribe(STATUS_TOPIC, qos=0)  # 订阅状态 topic
        log(f"已订阅：{DATA_TOPIC}")  # 打印已订阅数据 topic
        log(f"已订阅：{STATUS_TOPIC}")  # 打印已订阅状态 topic
    else:  # 否则表示连接失败
        log(f"MQTT 连接失败，reason_code={reason_code}")  # 打印失败原因


def on_message(client, userdata, msg):  # 收到 MQTT 消息后的回调
    topic = msg.topic  # 取出当前消息的 topic
    payload_text = msg.payload.decode("utf-8", errors="replace")  # 把二进制消息解码成字符串

    log(f"收到消息：topic={topic} payload={payload_text}")  # 打印收到的原始消息

    try:  # 开始按 topic 分流处理
        if topic == DATA_TOPIC:  # 如果是 10 分钟数据 topic
            insert_bee_counter_data(topic, payload_text)  # 写入数据库
        elif topic == STATUS_TOPIC:  # 如果是状态 topic
            log(f"设备状态消息：{payload_text}")  # 当前先只打印状态，不写库
        else:  # 如果收到其他 topic
            log(f"未处理的 topic：{topic}")  # 打印提示
    except Exception as e:  # 如果处理过程中出错
        log(f"处理消息失败：{e}")  # 打印错误信息


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):  # MQTT 断开连接回调
    log(f"MQTT 已断开，reason_code={reason_code}")  # 打印断开原因


def main() -> None:  # 主函数
    log("程序启动，准备连接 MQTT 并写入 SQLite")  # 打印启动日志

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="bee_db_writer_hive01")  # 创建 MQTT 客户端
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)  # 设置 MQTT 用户名和密码
    client.tls_set(ca_certs=certifi.where(), cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)  # 配置 TLS 证书校验
    client.tls_insecure_set(False)  # 禁止不安全 TLS

    client.on_connect = on_connect  # 绑定连接回调
    client.on_message = on_message  # 绑定消息回调
    client.on_disconnect = on_disconnect  # 绑定断开回调

    log(f"连接到 MQTT Broker：{MQTT_HOST}:{MQTT_PORT}")  # 打印目标 broker
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)  # 发起 MQTT 连接
    client.loop_forever()  # 进入阻塞循环，持续接收消息


if __name__ == "__main__":  # 如果当前文件作为主程序运行
    main()  # 执行主函数