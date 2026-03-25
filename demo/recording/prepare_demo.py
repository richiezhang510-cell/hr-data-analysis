from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from common import PREPARE_REPORT_PATH, ensure_artifact_dirs, load_config, write_json


def fetch_json(url: str) -> dict:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="检查 demo 录制前的服务与数据源状态。")
    parser.add_argument("--base-url", help="覆盖 demo_scenes.json 中的 base_url")
    args = parser.parse_args()

    config = load_config()
    base_url = (args.base_url or config.base_url).rstrip("/")
    ensure_artifact_dirs()

    try:
        health = fetch_json(f"{base_url}/health")
        data_source = fetch_json(f"{base_url}/api/data-source")
        metadata = fetch_json(f"{base_url}/api/metadata")
    except HTTPError as exc:
        print(f"[prepare_demo] 请求失败: {exc.code} {exc.reason}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"[prepare_demo] 无法连接到 {base_url}: {exc.reason}", file=sys.stderr)
        return 1

    report = {
        "base_url": base_url,
        "health": health,
        "data_source": data_source,
        "metadata": metadata,
    }
    write_json(PREPARE_REPORT_PATH, report)

    if health.get("status") != "ok":
        print("[prepare_demo] /health 未返回 status=ok", file=sys.stderr)
        return 1
    if not data_source.get("ready"):
        print("[prepare_demo] 当前活动数据源未就绪，请先完成默认 demo 数据接入。", file=sys.stderr)
        return 1

    print("[prepare_demo] 环境检查通过")
    print(f"[prepare_demo] base_url: {base_url}")
    print(f"[prepare_demo] 数据源: {data_source.get('filename')}")
    print(f"[prepare_demo] 行数: {data_source.get('row_count')}")
    print(f"[prepare_demo] 时间范围: {metadata.get('period_start')} -> {metadata.get('period_end')}")
    print(f"[prepare_demo] 详情已写入: {PREPARE_REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
