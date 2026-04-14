文件夹结构：

map_embed_activity_heatmap_click_anywhere_zone/
├─ index.html
├─ bee-config.js
├─ realtime-data.js
├─ realtime-data.json
└─ qgis tiles 03/
   └─ z/x/y.png

你要做的事：
1. 把你的瓦片文件夹放进来，文件夹名字必须严格叫：qgis tiles 03
2. 直接打开 index.html

这一版已经包含：
- 和 map embed 一样的固定范围
- 满屏显示
- 不允许缩小到露出范围外内容
- 去白边源头修复
- 顺滑缩放
- 叠加 S0-S8 热力图
- 地图上不显示传感器点
- 点击任意位置会弹出：所在分区地名 + 蜜蜂活跃度

说明：
- 地名来自之前分区图层里的 label_web，例如“北庄枇杷农田区”
- 蜜蜂活跃度为根据周边传感器点插值得到的估计值，以百分比显示
