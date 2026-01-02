#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple

DEFAULT_EXTS = {"mp4", "mkv", "avi", "mov", "wmv", "ts"}


def which_or_exit(name: str) -> str:
    from shutil import which
    p = which(name)
    if not p:
        print(f"❌ 错误: 未找到 {name}，请先安装/确保在 PATH 中", file=sys.stderr)
        sys.exit(1)
    return p


def run_with_timeout(cmd: list[str], timeout_s: int, extra_env: Optional[dict] = None) -> Tuple[int, str]:
    """
    运行外部命令，超时则杀掉整个进程组，返回 (returncode, stderr_text)。
    """
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    # 让子进程在独立进程组里，便于超时后 killpg（UNIX）
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        preexec_fn=os.setsid,
    )
    try:
        _, stderr = proc.communicate(timeout=timeout_s)
        return proc.returncode, stderr or ""
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            proc.kill()
        _, stderr = proc.communicate()
        return 124, (stderr or "") + "\n[timeout]"


def ffprobe_duration_seconds(ffprobe: str, video: Path, timeout_s: int = 15) -> Optional[float]:
    # format=duration 输出秒数（字符串）
    cmd = [
        ffprobe,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(video),
    ]
    code, err = run_with_timeout(cmd, timeout_s)
    if code != 0:
        return None
    # ffprobe 的 stdout 被我们丢弃了；为了简单起见，改成用 stderr 不太合适
    # 所以这里用 subprocess.run 直接拿 stdout（ffprobe 通常不会挂很久）
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        if r.returncode != 0:
            return None
        s = (r.stdout or "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def choose_timestamp(snapshot_time: float, duration: Optional[float]) -> float:
    """
    针对 CD2/115 网盘环境的优化逻辑：
    尽量不往后跳太多，防止超时/加载失败。
    默认策略：
    - 如果视频很短（<60s）：取中间。
    - 否则：取 30s ~ 60s 之间的一个点（或指定点），确保能截到内容但又不至于读太久。
    """
    # 如果用户没指定 snapshot_time (默认 120)，我们强制改写为更有利于云盘的值
    # 这里我们假设如果 snapshot_time > 60 就视为“用户没特别指定或者原来的默认值”，
    # 我们把它压缩到 45秒 左右，保证读取顺畅。
    target = snapshot_time
    if target > 60:
        target = 45.0

    if duration is None or duration <= 0:
        return max(0.0, target)

    # 特短视频
    if duration < 60:
        if duration < 5:
            return duration * 0.5
        return min(target, duration * 0.5)

    return target


def iter_video_files(root: Path, exts: Set[str], follow_links: bool = True) -> Iterable[Path]:
    """
    类似 find -L：递归目录，支持跟随符号链接，并避免 symlink loop。
    """
    visited: Set[Tuple[int, int]] = set()

    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_links):
        try:
            st = os.stat(dirpath)
            key = (st.st_dev, st.st_ino)
            if key in visited:
                dirnames[:] = []
                continue
            visited.add(key)
        except Exception:
            # 无权限等情况：跳过该目录
            dirnames[:] = []
            continue

        for fn in filenames:
            p = Path(dirpath) / fn
            ext = p.suffix.lower().lstrip(".")
            if ext in exts:
                yield p


def poster_path_for(video: Path) -> Path:
    base = video.with_suffix("")  # 只去掉最后一个后缀
    return Path(str(base) + "-poster.jpg")


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate poster JPGs for videos using ffmpeg.")
    ap.add_argument("--search-dir", default="/mnt/user/embydata/links/Hentai", help="扫描根目录")
    ap.add_argument("--snapshot-time", type=float, default=120, help="默认截图时间点（秒）")
    ap.add_argument("--dry-run", action="store_true", help="试运行：只打印不执行")
    ap.add_argument("--force", action="store_true", help="强制重新生成：覆盖旧封面")
    ap.add_argument("--cooldown", type=float, default=3.0, help="每个视频处理后冷却秒数（正式运行才生效）")
    ap.add_argument("--fast-timeout", type=int, default=30, help="快速模式 ffmpeg 超时秒数")
    ap.add_argument("--compat-timeout", type=int, default=60, help="兼容模式 ffmpeg 超时秒数")
    ap.add_argument("--ext", action="append", default=[], help="额外视频后缀（可重复传入）")

    args = ap.parse_args()

    search_dir = Path(args.search_dir)
    if not search_dir.is_dir():
        print(f"❌ 错误: 目录不存在: {search_dir}", file=sys.stderr)
        return 1

    ffmpeg = which_or_exit("ffmpeg")
    ffprobe = which_or_exit("ffprobe")

    exts = set(DEFAULT_EXTS)
    for e in args.ext:
        exts.add(e.lower().lstrip("."))

    print("========================================")
    print(f"📂 扫描目录: {search_dir}")
    print("🧪 模式: [DRY RUN - 试运行]" if args.dry_run else "🚀 模式: [正式运行]")
    print("⚠️ 策略: [强制重刷]" if args.force else "ℹ️ 策略: [增量模式]")
    print(f"🎞️ 后缀: {sorted(exts)}")
    print("========================================")

    # ffmpeg 输入侧参数（放在 -i 前，减少探测失败）
    # probesize/analyzeduration 的意义与默认值见 ffmpeg 文档 :contentReference[oaicite:5]{index=5}
    common_input = ["-hide_banner", "-loglevel", "error", "-analyzeduration", "20M", "-probesize", "20M"]
    common_output = ["-y", "-frames:v", "1", "-q:v", "2"]

    processed = 0
    created = 0
    skipped = 0
    failed = 0

    for video in iter_video_files(search_dir, exts, follow_links=True):
        processed += 1
        target = poster_path_for(video)

        # 1) 已存在逻辑
        if target.exists() and target.stat().st_size > 0:
            if args.force:
                if args.dry_run:
                    print(f"🧪 [模拟删除] 旧封面: {target.name}")
                else:
                    print(f"💥 强制: 删除旧封面 {target}")
                    try:
                        target.unlink(missing_ok=True)
                    except Exception as e:
                        print(f"❌ 删除失败: {e}")
                        failed += 1
                        continue
            else:
                if args.dry_run:
                    print(f"⏩ [模拟跳过] 已存在: {video.name}")
                skipped += 1
                continue

        print("------------------------------------------------")
        print(f"🎬 目标视频: {video.name}")

        # 2) 计算安全截图时间
        dur = ffprobe_duration_seconds(ffprobe, video)
        t = choose_timestamp(args.snapshot_time, dur)

        if args.dry_run:
            print(f"🧪 [模拟执行] 截图时间点: {t:.2f}s (duration={dur})")
            print(f"   输出: {target.name}")
            continue

        # 3) 生成（先写临时文件，成功后替换）
        #    同时准备 FFmpeg 报告文件
        #    注意：临时文件必须以 .jpg 结尾
        tmp = target.with_name(target.name + ".tmp.jpg")
        report_file = video.with_name(video.name + ".ffreport.log")
        ff_env = {"FFREPORT": f"file={report_file}:level=32"}

        # 【云盘优化版】直接使用“兼容模式”（解码并丢弃数据直到时间点）
        # "-ss" 放在 input 之后，意味着 FFmpeg 会顺序读取并解码，直到 45s (默认)
        # 虽然比 seek 慢，但这是对网络流最友好的方式，几乎不会 404 或超时。
        
        print(f"🐢 [云盘安全模式] 顺序读取至 {t:.2f}s 处截图...")
        
        cmd_safe = [
            ffmpeg, *common_input,
            "-i", str(video),
            "-ss", f"{t}",
            *common_output,
            str(tmp),
        ]
        
        # 因为是顺序读取，时间会比较久（取决于网速），超时给大一点
        code, err = run_with_timeout(cmd_safe, timeout_s=180, extra_env=ff_env)

        ok = (code == 0 and tmp.exists() and tmp.stat().st_size > 0)
        if ok:
            os.replace(tmp, target)
            print("✅ 成功")
            # 成功则删除报告
            try:
                report_file.unlink(missing_ok=True)
            except Exception:
                pass
            created += 1
        else:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            
            print(f"❌ 失败: {video}")
            if video.is_symlink():
                try:
                    print(f"   � 软链接指向: {video.resolve()}")
                except Exception:
                    pass
            print(f"   📝 错误报告: {report_file}")
            failed += 1

        if args.cooldown > 0:
            time.sleep(args.cooldown)

    print("========================================")
    if args.dry_run:
        print("🧪 试运行结束。去掉 --dry-run 以正式执行。")
    else:
        print(f"🎉 完成！处理 {processed} 个视频 | 新建 {created} | 跳过 {skipped} | 失败 {failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
