# GitHub Pages 部署步骤

## 你需要做的事

1. 把当前项目推送到你自己的 GitHub 仓库。
2. 在仓库的 `Settings -> Secrets and variables -> Actions` 中新增：
   - `QWEATHER_API_KEY`
3. 在仓库的 `Settings -> Pages` 中开启 GitHub Pages。
4. 如果你要用静态页面展示，把页面发布目录指向仓库根目录，或者后续按你的正式网站结构调整。

## 项目里已经准备好的内容

- `update_all.py`
  - 跨平台更新入口，GitHub Actions 会直接运行它。
- `.github/workflows/update-static-site.yml`
  - 每天自动更新数据，并把结果提交回仓库。
- `export_static_json.py`
  - 导出页面需要的静态 JSON 数据。
- `chart_page.js`
  - 支持两种模式：
    - `api`
    - `static`

## 本地静态预览

1. 先运行一次：
   - `update_bee_system.bat`
2. 再运行：
   - `preview_static_site.bat`
3. 打开的页面会自动使用：
   - `chart_test.html?mode=static`

## 说明

- 当前默认模式仍然是 `api`，不影响你现在本地继续使用 Flask。
- 以后如果要正式切到静态展示，可以把页面链接改成带 `?mode=static`，或者再把默认模式改成 `static`。
