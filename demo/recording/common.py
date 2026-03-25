from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


RECORDING_DIR = Path(__file__).resolve().parent
CONFIG_PATH = RECORDING_DIR / "demo_scenes.json"
ARTIFACTS_DIR = RECORDING_DIR / "artifacts"
RAW_DIR = ARTIFACTS_DIR / "raw"
AUDIO_DIR = ARTIFACTS_DIR / "audio"
TEXT_DIR = ARTIFACTS_DIR / "text"
RENDER_DIR = ARTIFACTS_DIR / "rendered"
TIMELINE_PATH = ARTIFACTS_DIR / "timeline.json"
PREPARE_REPORT_PATH = ARTIFACTS_DIR / "prepare_report.json"
RAW_VIDEO_PATH = RAW_DIR / "demo-recording.webm"
SUBTITLE_PATH = TEXT_DIR / "demo_subtitles.srt"
NARRATION_PATH = TEXT_DIR / "demo_narration.md"
AUDIO_MANIFEST_PATH = AUDIO_DIR / "audio_manifest.json"
MIXED_AUDIO_PATH = AUDIO_DIR / "voice_track.wav"
FINAL_VIDEO_PATH = RENDER_DIR / "demo_final.mp4"


@dataclass(slots=True)
class SubtitleCue:
    offset_s: float
    duration_s: float
    text: str


@dataclass(slots=True)
class Scene:
    id: str
    title: str
    target_duration_s: float
    narration: str
    subtitle_cues: list[SubtitleCue]


@dataclass(slots=True)
class DemoConfig:
    base_url: str
    viewport_width: int
    viewport_height: int
    question: str
    follow_up_question: str
    narration_voice: str
    narration_rate: str
    output_name: str
    scenes: list[Scene]


def ensure_artifact_dirs() -> None:
    for path in (ARTIFACTS_DIR, RAW_DIR, AUDIO_DIR, TEXT_DIR, RENDER_DIR):
        path.mkdir(parents=True, exist_ok=True)


def load_config(path: Path = CONFIG_PATH) -> DemoConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    video = payload.get("video", {})
    scenes = [
        Scene(
            id=item["id"],
            title=item["title"],
            target_duration_s=float(item["target_duration_s"]),
            narration=item["narration"].strip(),
            subtitle_cues=[
                SubtitleCue(
                    offset_s=float(cue["offset_s"]),
                    duration_s=float(cue["duration_s"]),
                    text=cue["text"].strip(),
                )
                for cue in item.get("subtitle_cues", [])
            ],
        )
        for item in payload["scenes"]
    ]
    return DemoConfig(
        base_url=video.get("base_url", "http://127.0.0.1:8000").rstrip("/"),
        viewport_width=int(video.get("width", 1920)),
        viewport_height=int(video.get("height", 1080)),
        question=payload["question"].strip(),
        follow_up_question=payload["follow_up"].strip(),
        narration_voice=video.get("voice", "zh-CN-XiaoxiaoNeural"),
        narration_rate=video.get("voice_rate", "+0%"),
        output_name=video.get("output_name", "comp-insight-demo"),
        scenes=scenes,
    )


def load_timeline(path: Path = TIMELINE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def timestamp_srt(seconds: float) -> str:
    total_ms = max(int(round(seconds * 1000)), 0)
    hours = total_ms // 3_600_000
    minutes = (total_ms % 3_600_000) // 60_000
    secs = (total_ms % 60_000) // 1000
    millis = total_ms % 1000
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def build_subtitle_entries(config: DemoConfig, timeline: dict[str, Any]) -> list[dict[str, Any]]:
    timeline_map = {scene["id"]: scene for scene in timeline.get("scenes", [])}
    entries: list[dict[str, Any]] = []
    for scene in config.scenes:
        timing = timeline_map.get(scene.id)
        if not timing:
            continue
        scene_start = float(timing["start_s"])
        scene_end = float(timing["end_s"])
        previous_end = scene_start
        for cue in scene.subtitle_cues:
            cue_start = max(scene_start + cue.offset_s, previous_end)
            cue_end = min(cue_start + cue.duration_s, scene_end)
            if cue_end <= cue_start:
                continue
            entries.append(
                {
                    "scene_id": scene.id,
                    "scene_title": scene.title,
                    "start_s": cue_start,
                    "end_s": cue_end,
                    "text": cue.text,
                }
            )
            previous_end = cue_end
    return entries


def write_srt(entries: list[dict[str, Any]], path: Path = SUBTITLE_PATH) -> None:
    lines: list[str] = []
    for index, entry in enumerate(entries, start=1):
        lines.append(str(index))
        lines.append(f"{timestamp_srt(entry['start_s'])} --> {timestamp_srt(entry['end_s'])}")
        lines.append(entry["text"])
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_narration_markdown(config: DemoConfig, timeline: dict[str, Any], path: Path = NARRATION_PATH) -> None:
    timeline_map = {scene["id"]: scene for scene in timeline.get("scenes", [])}
    lines = ["# Demo 旁白脚本", ""]
    for scene in config.scenes:
        timing = timeline_map.get(scene.id, {})
        start_s = float(timing.get("start_s", 0.0))
        end_s = float(timing.get("end_s", 0.0))
        lines.extend(
            [
                f"## {scene.title}",
                f"- Scene ID: `{scene.id}`",
                f"- 时间: `{start_s:.2f}s - {end_s:.2f}s`",
                f"- 旁白: {scene.narration}",
                "",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def require_binary(name: str) -> str:
    resolved = shutil.which(name)
    if not resolved:
        raise RuntimeError(f"未找到可执行文件 `{name}`，请先安装后再运行。")
    return resolved


def run_command(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, check=True, text=True, capture_output=True)


def ffprobe_duration(path: Path) -> float:
    ffprobe = require_binary("ffprobe")
    result = run_command(
        [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
    )
    return float(result.stdout.strip())
