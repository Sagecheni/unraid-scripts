#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
split_nfo.py — 将混合的 NFO 目录按视频归属分离为 Jav / FC2 两套独立目录树。

分类逻辑：
  对 nfo_root 下每个包含文件的叶子目录，计算其相对路径 rel_path，
  然后检查 jav_video_root/rel_path 和 fc2_video_root/rel_path 是否存在，
  据此决定将该目录复制到 output_root/Jav/rel_path 或 output_root/FC2/rel_path。
"""

import argparse
import csv
import logging
import shutil
import sys
import time
from pathlib import Path

# ================= 默认配置 =================
NFO_ROOT = "/mnt/user/embydata/mediainfo/mdcnfo"
JAV_VIDEO_ROOT = "/mnt/user/CloudNAS/CloudDrive/115open/NAS/Jav"
FC2_VIDEO_ROOT = "/mnt/user/CloudNAS/CloudDrive/115open/NAS/FC2"
OUTPUT_ROOT = "/mnt/user/embydata/mediainfo/split_nfo"
DRY_RUN = True
OVERWRITE = False
REPORT_PATH = "split_nfo_report.csv"
# =============================================

logger = logging.getLogger("split_nfo")


def setup_logging() -> None:
    """配置同时输出到控制台和日志文件的 logger。"""
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )

    # 控制台 handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # 文件 handler
    fh = logging.FileHandler("split_nfo.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


def find_leaf_dirs(root: Path) -> list[Path]:
    """
    找出 root 下所有"包含文件的叶子目录"（即作品目录）。
    如果一个目录内直接包含至少一个文件，就视为叶子目录。
    """
    leaves: list[Path] = []
    for dirpath, _dirnames, filenames in root.walk():
        if filenames:  # 目录内有文件
            leaves.append(dirpath)
    return leaves


def classify_dir(
    rel_path: Path,
    jav_root: Path,
    fc2_root: Path,
) -> str:
    """
    根据相对路径判断作品归属。

    返回值: "Jav" | "FC2" | "conflict" | "unmatched"
    """
    in_jav = (jav_root / rel_path).is_dir()
    in_fc2 = (fc2_root / rel_path).is_dir()

    if in_jav and in_fc2:
        return "conflict"
    if in_jav:
        return "Jav"
    if in_fc2:
        return "FC2"
    return "unmatched"


def copy_dir(src: Path, dst: Path, *, overwrite: bool, dry_run: bool) -> None:
    """
    将 src 目录中的所有文件复制到 dst，保留文件名。
    只复制文件，不递归子目录（叶子目录内不应有子目录需要处理）。
    """
    if dry_run:
        for f in src.iterdir():
            if f.is_file():
                logger.info("  [DRY RUN] 复制 %s -> %s", f.name, dst / f.name)
        return

    dst.mkdir(parents=True, exist_ok=True)

    for f in src.iterdir():
        if f.is_file():
            target = dst / f.name
            if target.exists() and not overwrite:
                logger.debug("  跳过已存在: %s", target)
                continue
            shutil.copy2(f, target)
            logger.debug("  复制: %s", f.name)


def write_report(rows: list[dict], report_path: Path, *, dry_run: bool) -> None:
    """将分类结果写入 CSV 报告。"""
    if dry_run:
        logger.info("[DRY RUN] 跳过写入报告: %s", report_path)
        return

    fieldnames = ["status", "rel_path", "source_dir", "target_dir", "matched_root"]
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("报告已写入: %s (%d 条记录)", report_path, len(rows))


def run(
    nfo_root: Path,
    jav_video_root: Path,
    fc2_video_root: Path,
    output_root: Path,
    *,
    dry_run: bool,
    overwrite: bool,
    report_path: Path,
) -> None:
    """主处理流程。"""
    logger.info("=" * 60)
    logger.info("NFO 目录分离工具")
    logger.info("=" * 60)
    logger.info("NFO 根目录:       %s", nfo_root)
    logger.info("JAV 视频根目录:   %s", jav_video_root)
    logger.info("FC2 视频根目录:   %s", fc2_video_root)
    logger.info("输出根目录:       %s", output_root)
    logger.info("模式:             %s", "DRY RUN（演练）" if dry_run else "正式执行")
    logger.info("覆盖策略:         %s", "覆盖" if overwrite else "跳过已存在")
    logger.info("=" * 60)

    if not nfo_root.is_dir():
        logger.error("NFO 根目录不存在: %s", nfo_root)
        sys.exit(1)

    # 1. 扫描所有叶子目录
    t0 = time.monotonic()
    leaves = find_leaf_dirs(nfo_root)
    logger.info(
        "扫描完成，发现 %d 个作品目录 (耗时 %.1fs)", len(leaves), time.monotonic() - t0
    )

    # 2. 逐个分类并复制
    stats = {"Jav": 0, "FC2": 0, "unmatched": 0, "conflict": 0}
    report_rows: list[dict] = []

    for leaf in leaves:
        rel_path = leaf.relative_to(nfo_root)
        category = classify_dir(rel_path, jav_video_root, fc2_video_root)
        stats[category] += 1

        if category in ("Jav", "FC2"):
            target_dir = output_root / category / rel_path
            status = "copied"
            logger.info("[%s] %s", category, rel_path)
            copy_dir(leaf, target_dir, overwrite=overwrite, dry_run=dry_run)
        elif category == "conflict":
            target_dir = ""
            status = "conflict"
            logger.warning("[CONFLICT] 两边都存在: %s", rel_path)
        else:
            target_dir = ""
            status = "unmatched"
            logger.warning("[UNMATCHED] 未匹配: %s", rel_path)

        report_rows.append(
            {
                "status": status,
                "rel_path": str(rel_path),
                "source_dir": str(leaf),
                "target_dir": str(target_dir),
                "matched_root": category,
            }
        )

    # 3. 输出统计
    logger.info("=" * 60)
    logger.info("处理完成")
    logger.info("  总计:       %d", sum(stats.values()))
    logger.info("  Jav:        %d", stats["Jav"])
    logger.info("  FC2:        %d", stats["FC2"])
    logger.info("  Unmatched:  %d", stats["unmatched"])
    logger.info("  Conflict:   %d", stats["conflict"])
    logger.info("=" * 60)

    # 4. 写入 CSV 报告
    write_report(report_rows, report_path, dry_run=dry_run)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="将混合的 NFO 目录按 JAV/FC2 视频归属分离到独立目录树。"
    )
    ap.add_argument("--nfo-root", default=NFO_ROOT, help="NFO 根目录")
    ap.add_argument("--jav-video-root", default=JAV_VIDEO_ROOT, help="JAV 视频根目录")
    ap.add_argument("--fc2-video-root", default=FC2_VIDEO_ROOT, help="FC2 视频根目录")
    ap.add_argument("--output-root", default=OUTPUT_ROOT, help="输出根目录")
    ap.add_argument(
        "--dry-run", action="store_true", default=DRY_RUN, help="演练模式，不实际复制"
    )
    ap.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="关闭演练模式，正式执行",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        default=OVERWRITE,
        help="覆盖已存在的目标文件",
    )
    ap.add_argument("--report", default=REPORT_PATH, help="CSV 报告输出路径")
    return ap.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    run(
        nfo_root=Path(args.nfo_root),
        jav_video_root=Path(args.jav_video_root),
        fc2_video_root=Path(args.fc2_video_root),
        output_root=Path(args.output_root),
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        report_path=Path(args.report),
    )


if __name__ == "__main__":
    main()
