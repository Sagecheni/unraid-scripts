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


def run_with_timeout(cmd: list[str], timeout_s: int) -> Tuple[int, str]:
    """
    运行外部命令，超时则杀掉整个进程组，返回 (returncode, stderr_text)。
    """
    # 让子进程在独立进程组里，便于超时后 killpg（UNIX）
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
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
    如果拿得到 duration，则避免 snapshot_time 超出时长导致失败。
    """
    if duration is None or duration <= 0:
        return max(0.0, snapshot_time)

    # 特短视频：取中间
    if duration < 6:
        return max(0.0, duration * 0.5)

    t = snapshot_time if snapshot_time > 0 else duration * 0.2
    # 如果接近结尾，移到中间偏后
    if t >= duration - 0.5:
        t = duration * 0.6
    return max(0.0, min(t, duration - 0.2))


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
        tmp = target.with_name(target.name + ".tmp")

        # 尝试 1：快速模式（-ss 在 -i 前，按 ffmpeg 文档属于 input seek） :contentReference[oaicite:6]{index=6}
        cmd_fast = [
            ffmpeg, *common_input,
            "-ss", f"{t}",
            "-i", str(video),
            *common_output,
            str(tmp),
        ]
        code, err = run_with_timeout(cmd_fast, args.fast_timeout)

        ok = (code == 0 and tmp.exists() and tmp.stat().st_size > 0)
        if ok:
            os.replace(tmp, target)
            print("✅ 成功 (快速模式)")
            created += 1
        else:
            # 清理失败产物
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

            # 尝试 2：兼容/更准确模式（-ss 在 -i 后，会先解码再丢弃到时间点，通常更稳但更慢） :contentReference[oaicite:7]{index=7}
            # 你原脚本固定用 5 秒，这里也保留“尽量靠前”的策略
            compat_t = 5.0
            if dur is not None and dur < 6:
                compat_t = max(0.0, dur * 0.5)

            print(f"⚠️ 快速模式失败，切换到兼容模式 ({compat_t:.2f}s)...")
            cmd_compat = [
                ffmpeg, *common_input,
                "-i", str(video),
                "-ss", f"{compat_t}",
                *common_output,
                str(tmp),
            ]
            code2, err2 = run_with_timeout(cmd_compat, args.compat_timeout)

            ok2 = (code2 == 0 and tmp.exists() and tmp.stat().st_size > 0)
            if ok2:
                os.replace(tmp, target)
                print("✅ 成功 (兼容模式)")
                created += 1
            else:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                print("❌ 彻底失败 (可能文件损坏 / 探测失败 / 超时)")
                # 需要的话把 err/err2 打出来方便排查
                # print(err.strip()[:500])
                # print(err2.strip()[:500])
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
