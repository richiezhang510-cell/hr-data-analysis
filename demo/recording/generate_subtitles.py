from __future__ import annotations

import argparse

from common import SUBTITLE_PATH, build_subtitle_entries, load_config, load_timeline, write_srt


def main() -> int:
    parser = argparse.ArgumentParser(description="根据录制时间线生成 SRT 字幕。")
    parser.parse_args()

    config = load_config()
    timeline = load_timeline()
    entries = build_subtitle_entries(config, timeline)
    write_srt(entries, SUBTITLE_PATH)
    print(f"[generate_subtitles] 已生成字幕: {SUBTITLE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
