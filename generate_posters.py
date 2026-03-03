#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import signal
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple, Dict, Any

DEFAULT_EXTS = {"mp4", "mkv", "avi", "mov", "wmv", "ts"}


shutdown_requested = False
shutdown_lock = threading.Lock()


def request_shutdown(signum=None, frame=None):
    """请求优雅关闭（线程安全）"""
    global shutdown_requested
    with shutdown_lock:
        shutdown_requested = True


def is_shutdown_requested() -> bool:
    """检查是否应该关闭（线程安全）"""
    global shutdown_requested
    with shutdown_lock:
        return shutdown_requested


signal.signal(signal.SIGTERM, request_shutdown)
signal.signal(signal.SIGINT, request_shutdown)


def which_or_exit(name: str) -> str:
    from shutil import which

    p = which(name)
    if not p:
        print(f"❌ 错误: 未找到 {name}，请先安装/确保在 PATH 中", file=sys.stderr)
        sys.exit(1)
    return p


def run_with_timeout(
    cmd: list[str], timeout_s: int, extra_env: Optional[dict] = None
) -> Tuple[int, str]:
    """
    运行外部命令，超时则杀掉整个进程组，返回 (returncode, stderr_text)。
    """
    if is_shutdown_requested():
        return 130, "shutdown requested"

    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    # 子进程使用独立进程组，便于超时后 killpg
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
    except Exception as e:
        proc.kill()
        return 130, str(e)


def ffprobe_duration_seconds(
    ffprobe: str, video: Path, timeout_s: int = 15
) -> Optional[float]:
    if is_shutdown_requested():
        return None

    cmd = [
        ffprobe,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video),
    ]
    code, err = run_with_timeout(cmd, timeout_s)
    if code != 0:
        return None
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
    target = snapshot_time
    if target > 60:
        target = 45.0

    if duration is None or duration <= 0:
        return max(0.0, target)

    if duration < 60:
        if duration < 5:
            return duration * 0.5
        return min(target, duration * 0.5)

    return target


def iter_video_files(
    root: Path, exts: Set[str], follow_links: bool = True
) -> Iterable[Path]:
    """
    类似 find -L：递归目录，支持跟随符号链接，并避免 symlink loop。
    """
    visited: Set[Tuple[int, int]] = set()

    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_links):
        if is_shutdown_requested():
            break

        try:
            st = os.stat(dirpath)
            key = (st.st_dev, st.st_ino)
            if key in visited:
                dirnames[:] = []
                continue
            visited.add(key)
        except Exception:
            dirnames[:] = []
            continue

        for fn in filenames:
            p = Path(dirpath) / fn
            ext = p.suffix.lower().lstrip(".")
            if ext in exts:
                yield p


def poster_path_for(video: Path) -> Path:
    """
    Generate poster path with filename truncation for Linux 255-byte limit.
    Truncates the original filename if adding '-poster.jpg' would exceed the limit.
    """
    base = video.with_suffix("")
    stem = base.name
    poster_suffix = "-poster.jpg"
    parent = base.parent
    poster_name = stem + poster_suffix
    max_filename_bytes = 255

    if len(poster_name.encode("utf-8")) <= max_filename_bytes:
        return parent / poster_name

    # Truncate by bytes (not chars) to preserve UTF-8 validity
    suffix_bytes = len(poster_suffix.encode("utf-8"))
    max_stem_bytes = max_filename_bytes - suffix_bytes
    stem_bytes = stem.encode("utf-8")
    while len(stem_bytes) > max_stem_bytes:
        stem_bytes = stem_bytes[:-1]

    truncated_stem = stem_bytes.decode("utf-8", errors="ignore")
    return parent / (truncated_stem + poster_suffix)


def process_video(
    video: Path,
    args: argparse.Namespace,
    ffmpeg: str,
    ffprobe: str,
    common_input: list,
    common_output: list,
    stats_lock: threading.Lock,
    stats: Dict[str, int],
    print_lock: threading.Lock,
) -> str:
    """
    处理单个视频（线程安全）
    返回状态: "created", "skipped", "failed"
    """
    if is_shutdown_requested():
        return "skipped"

    target = poster_path_for(video)

    # 1) 已存在逻辑
    if target.exists() and target.stat().st_size > 0:
        if args.force:
            with print_lock:
                if args.dry_run:
                    print(f"🧪 [模拟删除] 旧封面: {target.name}")
                else:
                    print(f"💥 强制: 删除旧封面 {target}")
            if not args.dry_run:
                try:
                    target.unlink(missing_ok=True)
                except Exception as e:
                    with print_lock:
                        print(f"❌ 删除失败: {e}")
                    with stats_lock:
                        stats["failed"] += 1
                    return "failed"
        else:
            with print_lock:
                if args.dry_run:
                    print(f"⏩ [模拟跳过] 已存在: {video.name}")
            with stats_lock:
                stats["skipped"] += 1
            return "skipped"

    with print_lock:
        print("------------------------------------------------")
        print(f"🎬 目标视频: {video.name}")

    # 2) 计算安全截图时间
    dur = ffprobe_duration_seconds(ffprobe, video)
    t = choose_timestamp(args.snapshot_time, dur)

    if args.dry_run:
        with print_lock:
            print(f"🧪 [模拟执行] 截图时间点: {t:.2f}s (duration={dur})")
            print(f"   输出: {target.name}")
        return "skipped"

    # 3) 生成（先写临时文件，成功后替换）
    tmp = target.with_name(target.name + ".tmp.jpg")
    report_file = video.with_name(video.name + ".ffreport.log")
    ff_env = {"FFREPORT": f"file={report_file}:level=32"}

    with print_lock:
        print(f"🐢 [云盘安全模式] 顺序读取至 {t:.2f}s 处截图...")

    cmd_safe = [
        ffmpeg,
        *common_input,
        "-i",
        str(video),
        "-ss",
        f"{t}",
        *common_output,
        str(tmp),
    ]

    code, err = run_with_timeout(cmd_safe, timeout_s=180, extra_env=ff_env)

    ok = code == 0 and tmp.exists() and tmp.stat().st_size > 0
    if ok:
        os.replace(tmp, target)
        with print_lock:
            print("✅ 成功")
        # 成功则删除报告
        try:
            report_file.unlink(missing_ok=True)
        except Exception:
            pass
        with stats_lock:
            stats["created"] += 1
        return "created"
    else:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

        with print_lock:
            print(f"❌ 失败: {video}")
            if video.is_symlink():
                try:
                    print(f"   🔗 软链接指向: {video.resolve()}")
                except Exception:
                    pass
            print(f"   📝 错误报告: {report_file}")

        with stats_lock:
            stats["failed"] += 1
        return "failed"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Generate poster JPGs for videos using ffmpeg (multi-threaded)."
    )
    ap.add_argument(
        "--search-dir", default="/mnt/user/embydata/links/Hentai", help="扫描根目录"
    )
    ap.add_argument(
        "--snapshot-time", type=float, default=120, help="默认截图时间点（秒）"
    )
    ap.add_argument("--dry-run", action="store_true", help="试运行：只打印不执行")
    ap.add_argument("--force", action="store_true", help="强制重新生成：覆盖旧封面")
    ap.add_argument(
        "--cooldown",
        type=float,
        default=0.0,
        help="每个视频处理后冷却秒数（多线程模式下建议为0）",
    )
    ap.add_argument(
        "--fast-timeout", type=int, default=30, help="快速模式 ffmpeg 超时秒数"
    )
    ap.add_argument(
        "--compat-timeout", type=int, default=60, help="兼容模式 ffmpeg 超时秒数"
    )
    ap.add_argument(
        "--ext", action="append", default=[], help="额外视频后缀（可重复传入）"
    )
    ap.add_argument(
        "--workers", type=int, default=4, help="并发线程数（0=自动检测CPU核心数）"
    )

    args = ap.parse_args()

    # 检查 cooldown 参数
    if args.cooldown > 0:
        print(
            "⚠️ 警告: 多线程模式下使用 --cooldown 会严重影响性能，建议设置为 0",
            file=sys.stderr,
        )

    search_dir = Path(args.search_dir)
    if not search_dir.is_dir():
        print(f"❌ 错误: 目录不存在: {search_dir}", file=sys.stderr)
        return 1

    ffmpeg = which_or_exit("ffmpeg")
    ffprobe = which_or_exit("ffprobe")

    # 自动检测线程数
    if args.workers == 0:
        import os

        args.workers = max(1, os.cpu_count() or 4)

    exts = set(DEFAULT_EXTS)
    for e in args.ext:
        exts.add(e.lower().lstrip("."))

    print("========================================")
    print(f"📂 扫描目录: {search_dir}")
    print(f"🧪 模式: [DRY RUN - 试运行]" if args.dry_run else f"🚀 模式: [正式运行]")
    print(f"⚠️ 策略: [强制重刷]" if args.force else f"ℹ️ 策略: [增量模式]")
    print(f"🧵 并发数: {args.workers}")
    print(f"🎞️ 后缀: {sorted(exts)}")
    print("========================================")

    common_input = [
        "-hide_banner",
        "-loglevel",
        "error",
        "-analyzeduration",
        "20M",
        "-probesize",
        "20M",
    ]
    common_output = ["-y", "-frames:v", "1", "-q:v", "2"]

    # 线程安全的统计和打印锁
    stats_lock = threading.Lock()
    print_lock = threading.Lock()
    stats = {"processed": 0, "created": 0, "skipped": 0, "failed": 0}

    # 收集所有视频文件
    video_files = list(iter_video_files(search_dir, exts, follow_links=True))

    with print_lock:
        print(f"📊 共发现 {len(video_files)} 个视频文件")
        print("========================================")

    # 多线程处理
    with ThreadPoolExecutor(
        max_workers=args.workers, thread_name_prefix="worker"
    ) as executor:
        # 提交所有任务
        future_to_video = {
            executor.submit(
                process_video,
                video,
                args,
                ffmpeg,
                ffprobe,
                common_input,
                common_output,
                stats_lock,
                stats,
                print_lock,
            ): video
            for video in video_files
        }

        # 处理完成的任务
        for future in as_completed(future_to_video):
            video = future_to_video[future]
            try:
                result = future.result()
                with stats_lock:
                    stats["processed"] += 1
            except Exception as e:
                with print_lock:
                    print(f"❌ 异常: {video} - {e}")
                with stats_lock:
                    stats["failed"] += 1
                    stats["processed"] += 1

    print("========================================")
    if args.dry_run:
        print("🧪 试运行结束。去掉 --dry-run 以正式执行。")
    else:
        print(
            f"🎉 完成！处理 {stats['processed']} 个视频 | 新建 {stats['created']} | 跳过 {stats['skipped']} | 失败 {stats['failed']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
