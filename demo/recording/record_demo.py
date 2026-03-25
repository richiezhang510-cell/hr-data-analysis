from __future__ import annotations

import argparse
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

from common import RAW_VIDEO_PATH, TIMELINE_PATH, ensure_artifact_dirs, load_config, write_json


def gentle_scroll(page: Page, total: int, steps: int = 6, delay_s: float = 0.4) -> None:
    increment = int(total / max(steps, 1))
    for _ in range(steps):
        page.mouse.wheel(0, increment)
        page.wait_for_timeout(int(delay_s * 1000))


def wait_for_saved_report_row(page: Page) -> None:
    page.get_by_test_id("history-panel-content").wait_for(state="visible", timeout=15_000)
    page.get_by_text("手动保存").wait_for(timeout=20_000)


def main() -> int:
    parser = argparse.ArgumentParser(description="自动录制 demo 浏览器演示视频。")
    parser.add_argument("--base-url", help="覆盖 demo_scenes.json 中的 base_url")
    parser.add_argument("--headed", action="store_true", help="以有头模式打开浏览器，方便观察。")
    parser.add_argument("--slow-mo", type=int, default=120, help="浏览器动作 slow_mo，单位毫秒。")
    args = parser.parse_args()

    config = load_config()
    base_url = (args.base_url or config.base_url).rstrip("/")
    ensure_artifact_dirs()

    scene_timings: list[dict[str, float | str]] = []
    current_scene: dict[str, float | str] | None = None

    def start_scene(scene_id: str) -> None:
        nonlocal current_scene
        now = time.perf_counter() - start_time
        if current_scene is not None:
            current_scene["end_s"] = round(now, 3)
            scene_timings.append(current_scene)
        current_scene = {"id": scene_id, "start_s": round(now, 3)}
        print(f"[record_demo] 场景开始: {scene_id}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not args.headed, slow_mo=args.slow_mo)
        context = browser.new_context(
            viewport={"width": config.viewport_width, "height": config.viewport_height},
            locale="zh-CN",
            record_video_dir=str(RAW_VIDEO_PATH.parent),
            record_video_size={"width": config.viewport_width, "height": config.viewport_height},
        )
        page = context.new_page()
        page.set_default_timeout(30_000)

        start_time = time.perf_counter()
        page.goto(base_url, wait_until="networkidle")
        page.get_by_test_id("analysis-question-input").wait_for(state="visible")
        page.get_by_test_id("active-data-source-label").wait_for(state="visible")

        start_scene("opening")
        page.wait_for_timeout(2_400)
        page.mouse.move(420, 220)
        page.wait_for_timeout(1_600)

        start_scene("data_source")
        page.locator("[data-testid='active-data-source-label']").scroll_into_view_if_needed()
        page.wait_for_timeout(2_400)
        page.mouse.move(1520, 220)
        page.wait_for_timeout(2_400)

        start_scene("question_entry")
        question_input = page.get_by_test_id("analysis-question-input")
        question_input.click()
        question_input.fill("")
        question_input.type(config.question, delay=28)
        page.wait_for_timeout(1_000)
        page.get_by_test_id("analyze-button").click()

        start_scene("analysis_depth")
        page.get_by_test_id("streaming-progress").wait_for(timeout=30_000)
        page.get_by_test_id("analysis-thinking-panel").wait_for(timeout=30_000)
        page.wait_for_timeout(7_000)
        page.get_by_test_id("report-workspace-title").wait_for(timeout=240_000)
        page.wait_for_timeout(2_000)
        page.get_by_test_id("report-methodology-card").scroll_into_view_if_needed()
        page.wait_for_timeout(2_000)
        gentle_scroll(page, total=500, steps=5)
        page.get_by_test_id("tab-report").click()
        page.get_by_test_id("executive-summary-card").wait_for(timeout=30_000)
        page.wait_for_timeout(2_000)
        gentle_scroll(page, total=900, steps=7)
        page.wait_for_timeout(1_500)
        page.get_by_test_id("tab-summary").click()
        page.wait_for_timeout(1_200)

        start_scene("follow_up")
        page.get_by_test_id("follow-up-toggle").click()
        page.get_by_test_id("follow-up-input").wait_for(state="visible")
        follow_up_input = page.get_by_test_id("follow-up-input")
        follow_up_input.click()
        follow_up_input.fill(config.follow_up_question)
        page.wait_for_timeout(800)
        page.get_by_test_id("send-follow-up-button").click()
        page.get_by_test_id("follow-up-messages").get_by_text(config.follow_up_question).wait_for(timeout=120_000)
        page.wait_for_timeout(8_000)

        start_scene("reuse")
        page.get_by_test_id("save-report-button").click()
        page.get_by_text("已保存").wait_for(timeout=60_000)
        page.wait_for_timeout(1_400)
        page.get_by_test_id("tab-history").click()
        page.get_by_test_id("history-tab-saved").click()
        wait_for_saved_report_row(page)
        page.wait_for_timeout(6_000)

        if current_scene is not None:
            current_scene["end_s"] = round(time.perf_counter() - start_time, 3)
            scene_timings.append(current_scene)

        video = page.video
        page.wait_for_timeout(300)
        page.close()
        video_path = Path(video.path())
        context.close()
        browser.close()

        if RAW_VIDEO_PATH.exists():
            RAW_VIDEO_PATH.unlink()
        video_path.rename(RAW_VIDEO_PATH)

    total_duration = scene_timings[-1]["end_s"] if scene_timings else 0.0
    write_json(
        TIMELINE_PATH,
        {
            "base_url": base_url,
            "video_path": str(RAW_VIDEO_PATH),
            "total_duration_s": total_duration,
            "scenes": scene_timings,
        },
    )
    print(f"[record_demo] 录制完成: {RAW_VIDEO_PATH}")
    print(f"[record_demo] 时间线已写入: {TIMELINE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
