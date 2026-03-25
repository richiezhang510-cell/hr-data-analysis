from __future__ import annotations

import argparse
import subprocess
import sys

from common import (
    AUDIO_MANIFEST_PATH,
    FINAL_VIDEO_PATH,
    MIXED_AUDIO_PATH,
    RAW_VIDEO_PATH,
    SUBTITLE_PATH,
    TIMELINE_PATH,
    ensure_artifact_dirs,
    ffprobe_duration,
    load_timeline,
    require_binary,
)
from generate_subtitles import main as generate_subtitles_main
from synthesize_voice import main as synthesize_voice_main


def build_filter_complex(audio_manifest: dict) -> str:
    delayed_labels: list[str] = []
    filters: list[str] = []
    for index, scene in enumerate(audio_manifest["scenes"], start=1):
        delay_ms = max(int(round(float(scene["start_s"]) * 1000)), 0)
        label = f"a{index}"
        filters.append(f"[{index}:a]adelay={delay_ms}|{delay_ms}[{label}]")
        delayed_labels.append(f"[{label}]")
    return "; ".join(filters + [f"[0:a]{''.join(delayed_labels)}amix=inputs={len(delayed_labels) + 1}:duration=first:dropout_transition=0[aout]"])


def run_ffmpeg(command: list[str]) -> None:
    subprocess.run(command, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="生成字幕、配音并合成最终 demo 视频。")
    parser.add_argument("--voice", help="覆盖 TTS 语音")
    parser.add_argument("--rate", help="覆盖 TTS 语速")
    args = parser.parse_args()

    ensure_artifact_dirs()
    if not RAW_VIDEO_PATH.exists():
        print(f"[render_demo] 未找到原始录屏: {RAW_VIDEO_PATH}", file=sys.stderr)
        return 1
    if not TIMELINE_PATH.exists():
        print(f"[render_demo] 未找到录制时间线: {TIMELINE_PATH}", file=sys.stderr)
        return 1

    require_binary("ffmpeg")
    require_binary("ffprobe")

    original_argv = sys.argv
    try:
        sys.argv = ["generate_subtitles.py"]
        generate_subtitles_main()

        sys.argv = ["synthesize_voice.py"]
        if args.voice:
            sys.argv.extend(["--voice", args.voice])
        if args.rate:
            sys.argv.extend(["--rate", args.rate])
        synthesize_voice_main()
    finally:
        sys.argv = original_argv

    audio_manifest = load_timeline(AUDIO_MANIFEST_PATH)
    video_duration = ffprobe_duration(RAW_VIDEO_PATH)

    filter_complex = build_filter_complex(audio_manifest)
    mix_command = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-t",
        f"{video_duration:.3f}",
        "-i",
        "anullsrc=channel_layout=stereo:sample_rate=44100",
    ]
    for scene in audio_manifest["scenes"]:
        mix_command.extend(["-i", str(scene["audio_path"])])
    mix_command.extend(
        [
            "-filter_complex",
            filter_complex,
            "-map",
            "[aout]",
            "-c:a",
            "pcm_s16le",
            str(MIXED_AUDIO_PATH),
        ]
    )
    run_ffmpeg(mix_command)

    final_command = [
        "ffmpeg",
        "-y",
        "-i",
        str(RAW_VIDEO_PATH),
        "-i",
        str(MIXED_AUDIO_PATH),
        "-i",
        str(SUBTITLE_PATH),
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-map",
        "2:0",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-c:s",
        "mov_text",
        "-movflags",
        "+faststart",
        str(FINAL_VIDEO_PATH),
    ]
    run_ffmpeg(final_command)
    print(f"[render_demo] 最终成片: {FINAL_VIDEO_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
