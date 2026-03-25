#!/usr/bin/env python3
"""
HR 薪酬规划 Demo 录屏脚本
使用 playwright-cli 驱动浏览器，结合 page.evaluate 注入字幕，录制约 2 分钟 Demo 视频。

用法：
    python3 demo/record_demo.py

前提：
    1. uvicorn 已在 http://localhost:8000 运行
    2. 已激活 demo 数据
    3. playwright-cli 已安装 (npm install -g @playwright/cli@latest)

输出：
    demo/demo_video.webm
"""

import subprocess
import time
import sys
import os
import shutil
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────────────────────────────
BASE_URL = "http://localhost:8000"
DEMO_DIR = Path(__file__).resolve().parent
OUTPUT_VIDEO = DEMO_DIR / "demo_video.webm"

# playwright-cli 命令
PCLI = "playwright-cli"

# 主分析问题（经济补偿金）
MAIN_QUESTION = "为什么2025年3月经济补偿金明显上涨？请按BU、级别、司龄拆解，重点看总额和人均"

# 追问内容
FOLLOW_UP_1 = "能再按级别细分一下吗？"
FOLLOW_UP_2 = "哪些员工贡献最大？"
FOLLOW_UP_3 = "请把这份报告改成更适合管理层汇报的版本。"

# ── 工具函数 ──────────────────────────────────────────────────────────────────

def run(cmd: str, check=True, timeout=30) -> subprocess.CompletedProcess:
    """执行 playwright-cli 命令"""
    full_cmd = f"{PCLI} {cmd}"
    print(f"  » {full_cmd}")
    result = subprocess.run(
        full_cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(DEMO_DIR.parent),  # 项目根目录
    )
    if result.stdout.strip():
        print(f"    {result.stdout.strip()[:200]}")
    if result.returncode != 0 and check:
        print(f"  ✗ 错误: {result.stderr.strip()[:300]}", file=sys.stderr)
        # 非致命错误不退出，继续录制
    return result


def set_caption(text: str):
    """注入/更新页面底部字幕浮层"""
    # 用 JS 转义处理中文
    escaped = text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", " ")
    js = f"""
(function() {{
  let cap = document.getElementById('demo-caption-overlay');
  if (!cap) {{
    cap = document.createElement('div');
    cap.id = 'demo-caption-overlay';
    Object.assign(cap.style, {{
      position: 'fixed',
      bottom: '36px',
      left: '50%',
      transform: 'translateX(-50%)',
      zIndex: '99999',
      background: 'rgba(0,0,0,0.72)',
      color: '#fff',
      fontSize: '17px',
      fontWeight: '500',
      fontFamily: '-apple-system, "PingFang SC", "Microsoft YaHei", sans-serif',
      padding: '10px 28px',
      borderRadius: '32px',
      whiteSpace: 'nowrap',
      letterSpacing: '0.03em',
      boxShadow: '0 4px 20px rgba(0,0,0,0.4)',
      backdropFilter: 'blur(8px)',
      border: '1px solid rgba(255,255,255,0.12)',
      transition: 'opacity 0.3s ease',
      pointerEvents: 'none',
    }});
    document.body.appendChild(cap);
  }}
  cap.style.opacity = '0';
  setTimeout(() => {{
    cap.textContent = '{escaped}';
    cap.style.opacity = '1';
  }}, 150);
}})();
"""
    run(f'eval "{js.strip()}"', check=False)


def wait(seconds: float):
    """等待指定秒数，同时打印进度"""
    print(f"  ⏱  等待 {seconds}s...")
    time.sleep(seconds)


def step(label: str, caption: str):
    """打印步骤标题并设置字幕"""
    print(f"\n{'─'*60}")
    print(f"  📍 {label}")
    print(f"{'─'*60}")
    set_caption(caption)
    wait(0.8)


# ── 主流程 ────────────────────────────────────────────────────────────────────

def check_pcli():
    """检查 playwright-cli 是否可用"""
    result = subprocess.run(
        f"{PCLI} --version", shell=True, capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        print("✗ playwright-cli 未安装，正在尝试安装...")
        subprocess.run("npm install -g @playwright/cli@latest", shell=True, check=True, timeout=120)
        # 安装浏览器
        subprocess.run(f"{PCLI} install-browser", shell=True, timeout=300)
    else:
        print(f"✓ playwright-cli 已就绪: {result.stdout.strip()}")


def main():
    print("=" * 60)
    print("  HR 薪酬规划 Demo 录制开始")
    print("=" * 60)

    # 0. 检查依赖
    check_pcli()

    # 1. 关闭可能存在的旧会话
    run("close-all", check=False)
    wait(1)

    # 2. 打开浏览器并设置分辨率
    print("\n🚀 启动浏览器...")
    run(f"open {BASE_URL}", timeout=30)
    wait(1.5)
    run("resize 1440 900")
    wait(1)

    # 3. 开始录制
    print("\n🎬 开始录制视频...")
    run("video-start")
    wait(0.5)

    # ── 第一幕：首页展示 (约 10s) ─────────────────────────────────────────────
    step(
        "第一幕：展示系统首页",
        "✦  HR 智能薪酬分析系统  ·  200 万行真实数据已激活"
    )
    wait(3)

    step(
        "展示数据源信息",
        "数据就绪：2024-12 至 2025-12  ·  多 BU · 多科目 · 多维度"
    )
    # 向下滚动展示数据源卡片
    run('eval "window.scrollBy({top: 320, behavior: \'smooth\'})"', check=False)
    wait(3)
    run('eval "window.scrollTo({top: 0, behavior: \'smooth\'})"', check=False)
    wait(2)

    # ── 第二幕：发起分析 (约 10s) ─────────────────────────────────────────────
    step(
        "第二幕：输入分析问题",
        "提出业务问题：分析经济补偿金异常上涨原因  ·  按 BU / 级别 / 司龄拆解"
    )

    # 先截图确认当前页面状态
    run("snapshot --filename=before_input.yaml", check=False)
    wait(1)

    # 点击输入框（通过 CSS 选择器）
    run('run-code "async page => { const el = page.locator(\'input[placeholder*=\\\"请分析最近\\\"\]').first(); await el.click(); }"', check=False, timeout=15)
    wait(0.5)

    # 逐字输入问题（模拟真实用户输入）
    # 先清空，再输入
    run(f'run-code "async page => {{ const el = page.locator(\'input[placeholder*=\\\"请分析最近\\\"]\').first(); await el.fill(\\\"{MAIN_QUESTION}\\\"); }}"', check=False, timeout=15)
    wait(2)

    step(
        "点击发送，等待报告生成",
        "⚙  AI 正在进行多维度拆解与 LLM 分析，请稍候..."
    )

    # 点击"分析"发送按钮
    run('run-code "async page => { const btn = page.locator(\'button\').filter({hasText: \'分析\'}).last(); await btn.click(); }"', check=False, timeout=15)
    wait(2)

    # ── 第三幕：等待报告生成 (约 50-70s) ─────────────────────────────────────
    # 等待 ThinkingPanel 出现
    print("\n  ⏳ 等待报告生成中（LLM 分析，最长等待 120s）...")
    
    # 每隔 5s 更新字幕，保持视频有动感
    captions_during_wait = [
        "⚙  正在读取数据 · 计算 BU 总览...",
        "⚙  正在按级别 · 司龄 · 绩效分位多维拆解...",
        "⚙  正在生成管理层可读报告正文...",
        "⚙  正在整合结论 · 即将完成...",
    ]
    
    wait_total = 0
    report_found = False
    
    for i in range(20):  # 最多等 100s
        wait(5)
        wait_total += 5
        
        # 更新等待字幕
        caption_idx = i % len(captions_during_wait)
        set_caption(captions_during_wait[caption_idx])
        
        # 检查页面是否已跳转到报告页（检查是否出现 Tab 按钮）
        snap = run("snapshot --filename=wait_check.yaml", check=False, timeout=15)
        
        # 读取快照内容检查
        snap_file = Path(DEMO_DIR.parent / ".playwright-cli" / "wait_check.yaml")
        if snap_file.exists():
            content = snap_file.read_text(encoding="utf-8", errors="ignore")
            # 检查报告相关关键词
            if any(kw in content for kw in ["执行摘要", "report", "完整正文", "报告已生成", "dimension"]):
                print(f"  ✓ 检测到报告已生成（等待了 {wait_total}s）")
                report_found = True
                break
        
        if wait_total >= 90:
            print(f"  ⚠ 等待超过 90s，继续下一步")
            break
    
    wait(3)

    # ── 第四幕：报告展示 (约 15s) ─────────────────────────────────────────────
    step(
        "第四幕：报告生成完成",
        "📋  报告已生成  ·  执行摘要  +  BU 总览  +  分维度洞察  +  管理建议"
    )
    wait(3)

    # 滚动展示报告内容
    run('eval "window.scrollBy({top: 400, behavior: \'smooth\'})"', check=False)
    wait(2.5)
    run('eval "window.scrollBy({top: 400, behavior: \'smooth\'})"', check=False)
    wait(2.5)
    run('eval "window.scrollBy({top: 500, behavior: \'smooth\'})"', check=False)
    wait(2)

    step(
        "展示完整报告正文",
        "完整管理报告：执行摘要  ·  现状透视  ·  异常分析  ·  管理建议"
    )
    # 尝试点击"完整正文"Tab（如果存在）
    run('run-code "async page => { try { const tab = page.locator(\'[role=tab]\').filter({hasText: \'完整正文\'}).first(); if (await tab.count() > 0) await tab.click(); } catch(e) {} }"', check=False, timeout=10)
    wait(3)

    # 继续滚动
    run('eval "window.scrollBy({top: 600, behavior: \'smooth\'})"', check=False)
    wait(3)

    # ── 第五幕：追问环节 ──────────────────────────────────────────────────────
    step(
        "第五幕：打开追问侧边栏",
        "💬  报告范围内继续追问  ·  无需重新发起分析"
    )

    # 点击右侧"追问"按钮（竖排文字按钮）
    run('run-code "async page => { try { const btn = page.locator(\'button\').filter({hasText: \'追问\'}).first(); await btn.click(); } catch(e) {} }"', check=False, timeout=15)
    wait(1.5)  # 等待抽屉滑入动画

    # 追问一：按级别细分
    step(
        "追问一：按级别细分",
        "💡  追问一：按职级细分经济补偿金  ·  锁定异常集中人群"
    )

    # 点击追问建议 Chip "能再按级别细分一下吗？"
    run(f'run-code "async page => {{ try {{ const chip = page.locator(\'button\').filter({{hasText: \'能再按级别细分一下吗\'}}).first(); await chip.click(); }} catch(e) {{ const input = page.locator(\'aside input\').first(); await input.fill(\\\"{FOLLOW_UP_1}\\\"); }} }}"', check=False, timeout=15)
    wait(0.5)

    # 点击"发送追问"
    run('run-code "async page => { try { const btn = page.locator(\'aside button\').filter({hasText: \'发送追问\'}).first(); if (await btn.count() > 0) await btn.click(); } catch(e) {} }"', check=False, timeout=15)
    wait(2)

    print("  ⏳ 等待追问一响应（最多 30s）...")
    for _ in range(6):
        wait(5)
        snap = run("snapshot --filename=followup1.yaml", check=False, timeout=15)
        snap_file = Path(DEMO_DIR.parent / ".playwright-cli" / "followup1.yaml")
        if snap_file.exists():
            content = snap_file.read_text(encoding="utf-8", errors="ignore")
            # 检查追问回答是否出现
            if "级别" in content and len(content) > 5000:
                print("  ✓ 追问一响应已出现")
                break

    # 追问二：找贡献最大员工
    step(
        "追问二：锁定核心员工",
        "💡  追问二：哪些员工贡献最大  ·  锁定高成本核心人群"
    )

    # 点击追问建议 Chip "哪些员工贡献最大？"
    run(f'run-code "async page => {{ try {{ const chip = page.locator(\'button\').filter({{hasText: \'哪些员工贡献最大\'}}).first(); await chip.click(); }} catch(e) {{ const input = page.locator(\'aside input\').first(); await input.fill(\\\"{FOLLOW_UP_2}\\\"); }} }}"', check=False, timeout=15)
    wait(0.5)

    # 点击"发送追问"
    run('run-code "async page => { try { const btn = page.locator(\'aside button\').filter({hasText: \'发送追问\'}).first(); if (await btn.count() > 0) await btn.click(); } catch(e) {} }"', check=False, timeout=15)
    wait(2)

    print("  ⏳ 等待追问二响应（最多 30s）...")
    for _ in range(6):
        wait(5)

    # 追问三：生成管理层报告
    step(
        "追问三：生成管理层汇报版",
        "✨  一键生成管理层汇报版报告  ·  无需重跑底层数据"
    )

    # 填写追问三内容
    run(f'run-code "async page => {{ try {{ const input = page.locator(\'aside input\').first(); await input.fill(\\\"{FOLLOW_UP_3}\\\"); }} catch(e) {{}} }}"', check=False, timeout=15)
    wait(0.8)

    # 点击"生成新报告"
    run('run-code "async page => { try { const btn = page.locator(\'aside button\').filter({hasText: \'生成新报告\'}).first(); if (await btn.count() > 0) await btn.click(); } catch(e) {} }"', check=False, timeout=15)
    wait(2)

    print("  ⏳ 等待追问三响应（最多 40s）...")
    for _ in range(8):
        wait(5)

    # ── 收尾 ──────────────────────────────────────────────────────────────────
    step(
        "收尾",
        "✦  从一次性报告，到可持续追问的管理闭环  ·  Comp Insight Studio"
    )
    wait(5)

    # ── 停止录制，保存视频 ────────────────────────────────────────────────────
    print("\n🎬 停止录制，保存视频...")
    
    # 保存到 demo 目录
    video_output = str(OUTPUT_VIDEO)
    run(f'video-stop "{video_output}"', timeout=30)
    wait(2)

    # 关闭浏览器
    run("close", check=False)

    # 检查输出文件
    if OUTPUT_VIDEO.exists():
        size_mb = OUTPUT_VIDEO.stat().st_size / 1024 / 1024
        print(f"\n{'='*60}")
        print(f"  ✅ 录制完成！")
        print(f"  📁 视频文件: {OUTPUT_VIDEO}")
        print(f"  📦 文件大小: {size_mb:.1f} MB")
        print(f"{'='*60}")
    else:
        # 尝试从 .playwright-cli 目录查找生成的视频
        pcli_dir = Path(DEMO_DIR.parent / ".playwright-cli")
        webm_files = list(pcli_dir.glob("*.webm")) if pcli_dir.exists() else []
        if webm_files:
            latest = max(webm_files, key=lambda f: f.stat().st_mtime)
            shutil.move(str(latest), str(OUTPUT_VIDEO))
            size_mb = OUTPUT_VIDEO.stat().st_size / 1024 / 1024
            print(f"\n{'='*60}")
            print(f"  ✅ 录制完成（从缓存目录移动）！")
            print(f"  📁 视频文件: {OUTPUT_VIDEO}")
            print(f"  📦 文件大小: {size_mb:.1f} MB")
            print(f"{'='*60}")
        else:
            print(f"\n{'='*60}")
            print(f"  ⚠  视频文件未找到，请检查 playwright-cli 输出")
            print(f"  期望路径: {OUTPUT_VIDEO}")
            print(f"{'='*60}")


if __name__ == "__main__":
    main()
