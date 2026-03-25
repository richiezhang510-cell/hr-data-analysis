# Demo Recording Pipeline

这一套脚本把 demo 演示拆成 3 个阶段：

1. `prepare_demo.py`
   检查服务是否可访问、默认数据源是否 ready。
2. `record_demo.py`
   用 Playwright 自动操作页面并录制无声浏览器视频，同时输出时间线。
3. `render_demo.py`
   生成字幕、调用 TTS 生成旁白，再用 FFmpeg 合成最终成片。

## 依赖

先安装开发依赖：

```bash
cd "/Users/zhangyuesheng/Desktop/03_项目代码/agent搭建/HR薪酬规划"
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m playwright install chromium
```

系统还需要：

- `ffmpeg`
- `ffprobe`

## 使用方式

先启动项目服务，并确保默认 demo 数据源已经 ready：

```bash
python -m uvicorn app:app --reload
```

然后在另一个终端执行：

```bash
cd "/Users/zhangyuesheng/Desktop/03_项目代码/agent搭建/HR薪酬规划"
source .venv/bin/activate

python demo/recording/prepare_demo.py
python demo/recording/record_demo.py --headed
python demo/recording/render_demo.py
```

## 输出文件

所有产物默认输出到 `demo/recording/artifacts/`：

- `prepare_report.json`
- `timeline.json`
- `raw/demo-recording.webm`
- `text/demo_subtitles.srt`
- `text/demo_narration.md`
- `audio/*.mp3`
- `audio/voice_track.wav`
- `rendered/demo_final.mp4`

## 可调参数

主要文案和场景时长都放在 `demo_scenes.json`：

- `question`
- `follow_up`
- 各 scene 的 `narration`
- 各 scene 的 `subtitle_cues`
- TTS `voice` 与 `voice_rate`

如果页面结构有调整，优先修改前端 `data-testid` 或 `record_demo.py` 中的等待/滚动逻辑，不需要改字幕与配音脚本。
