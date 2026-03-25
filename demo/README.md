# Demo Assets

这个目录只存放演示用途的脚本和轻量模拟样例，不属于正式分析链路。

## 数据说明

- 本目录中的样例数据均为模拟数据
- 仅用于展示产品流程、界面效果和分析能力
- 不包含真实员工薪酬数据

## 当前保留内容

- 合成宽表生成脚本
- HR 模拟数据生成脚本
- 演示辅助脚本
- 小型样例 CSV / Numbers 文件
- 录屏相关脚本与说明

## 不随仓库发布的内容

- 大体量 demo CSV
- 本地数据库
- 上传缓存
- 录屏原视频文件

如需完整演示流程，建议使用当前目录中的小型样例，或通过脚本自行生成模拟数据。

## 使用边界

- 正式启动不会自动读取这里的任何文件
- `/api/upload`、`/api/report`、`/api/report/stream` 不会默认依赖这里的数据
- 这里只有在手动运行 demo 脚本时才会生效

## 手动运行示例

```bash
python3 demo/generate_hr_data.py
python3 demo/generate_salary_wide_dataset.py
python3 demo/optimize_data.py
```

如果你要做真实分析，请回到正式链路，直接上传真实宽表 CSV 或设置 `ACTIVE_DATASET_PATH`。
