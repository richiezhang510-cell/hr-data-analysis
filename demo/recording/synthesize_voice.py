from __future__ import annotations

import argparse
import asyncio
import shutil
import subprocess
from pathlib import Path

from common import (
    AUDIO_DIR,
    AUDIO_MANIFEST_PATH,
    NARRATION_PATH,
    ensure_artifact_dirs,
    ffprobe_duration,
    load_config,
    load_timeline,
    write_json,
    write_narration_markdown,
)

try:
    import edge_tts  # type: ignore
except ImportError:
    edge_tts = None


async def synthesize_clip(text: str, path: Path, voice: str, rate: str) -> None:
    if edge_tts is not None:
        communicator = edge_tts.Communicate(text=text, voice=voice, rate=rate)
        await communicator.save(str(path))
        return
    raise RuntimeError("edge_tts 不可用")


def synthesize_with_say(text: str, path: Path, voice: str) -> None:
    say_bin = shutil.which("say")
    if not say_bin:
        raise RuntimeError("当前环境既没有 edge_tts，也没有 macOS `say` 命令可回退。")
    aiff_path = path.with_suffix(".aiff")
    subprocess.run(
        [say_bin, "-v", voice, "-o", str(aiff_path), text],
        check=True,
    )
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(aiff_path), str(path)],
        check=True,
        capture_output=True,
    )
    aiff_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="为 demo 旁白生成 TTS 音频。")
    parser.add_argument("--voice", help="覆盖 demo_scenes.json 中的语音")
    parser.add_argument("--rate", help="覆盖 demo_scenes.json 中的语速")
    args = parser.parse_args()

    config = load_config()
    timeline = load_timeline()
    ensure_artifact_dirs()
    write_narration_markdown(config, timeline, NARRATION_PATH)

    timeline_map = {scene["id"]: scene for scene in timeline.get("scenes", [])}
    manifest_scenes: list[dict[str, object]] = []
    voice = args.voice or config.narration_voice
    rate = args.rate or config.narration_rate
    using_edge_tts = edge_tts is not None
    fallback_voice = "Tingting"

    for scene in config.scenes:
        timing = timeline_map.get(scene.id)
        if not timing:
            continue
        audio_path = AUDIO_DIR / f"{scene.id}.mp3"
        if using_edge_tts:
            asyncio.run(synthesize_clip(scene.narration, audio_path, voice=voice, rate=rate))
        else:
            synthesize_with_say(scene.narration, audio_path, voice=fallback_voice)
        duration_s = ffprobe_duration(audio_path)
        scene_duration_s = float(timing["end_s"]) - float(timing["start_s"])
        if duration_s > scene_duration_s:
            raise RuntimeError(
                f"场景 `{scene.id}` 的配音 {duration_s:.2f}s 超过镜头时长 {scene_duration_s:.2f}s，请缩短旁白或延长镜头。"
            )
        manifest_scenes.append(
            {
                "id": scene.id,
                "title": scene.title,
                "audio_path": str(audio_path),
                "start_s": float(timing["start_s"]),
                "end_s": float(timing["end_s"]),
                "audio_duration_s": duration_s,
            }
        )

    write_json(
        AUDIO_MANIFEST_PATH,
        {
            "voice": voice if using_edge_tts else fallback_voice,
            "rate": rate if using_edge_tts else "system-default",
            "engine": "edge_tts" if using_edge_tts else "macos-say",
            "narration_markdown": str(NARRATION_PATH),
            "scenes": manifest_scenes,
        },
    )
    print(f"[synthesize_voice] 旁白脚本: {NARRATION_PATH}")
    print(f"[synthesize_voice] 音频清单: {AUDIO_MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
