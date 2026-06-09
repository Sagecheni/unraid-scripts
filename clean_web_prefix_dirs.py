#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import sys
import time
from pathlib import Path

# ================= 配置区 =================
DRY_RUN = False
SLEEP_SECONDS = 0.2
# ==========================================

# 匹配类似 www.98T.la@、abc.com@、sub.domain.co.jp@ 的网址型前缀
WEB_PREFIX_RE = re.compile(
    r"(?i)(?:^|(?<=\s))(?:https?://)?(?:www\.)?(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s@]*)?@+"
)
WHITESPACE_RE = re.compile(r"\s+")
SEPARATOR_SPACE_RE = re.compile(r"\s*([\-–—])\s*")
UNSAFE_CHAR_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f\x7f]')
INVISIBLE_CHAR_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]")
EAST_ASIAN_CHAR_CLASS = (
    r"\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff"
    r"\u3040-\u30ff\u31f0-\u31ff"
    r"\uac00-\ud7af"
)


def remove_web_prefixes(name: str) -> str:
    """移除名称中的网址型前缀。"""
    cleaned = WEB_PREFIX_RE.sub("", name)
    cleaned = cleaned.strip()
    return cleaned


def sanitize_name(name: str) -> str:
    """
    清理名称：
    - 先去掉连接符两侧的空白
    - 将连续空白折叠为单个空格
    - 中文/日文/韩文附近的空格直接删除
    - 删除常见非法字符和控制字符
    - 删除零宽等不可见字符
    """
    name = INVISIBLE_CHAR_RE.sub("", name)
    name = UNSAFE_CHAR_RE.sub("", name)
    name = SEPARATOR_SPACE_RE.sub(r"\1", name)
    name = WHITESPACE_RE.sub(" ", name)
    name = re.sub(rf"(?<=[{EAST_ASIAN_CHAR_CLASS}])\s+", "", name)
    name = re.sub(rf"\s+(?=[{EAST_ASIAN_CHAR_CLASS}])", "", name)
    name = name.strip()
    return name


def build_fallback_name(original_name: str, is_dir: bool) -> str:
    """清理后为空时，生成可追溯的兜底名称。"""
    compact = re.sub(r"[^A-Za-z0-9]+", "_", original_name).strip("_").lower()
    compact = compact[:40] if compact else "empty"
    if is_dir:
        return f"_cleaned_{compact}"

    stem, suffix = os.path.splitext(original_name)
    suffix = sanitize_name(suffix)
    if suffix:
        return f"_cleaned_{compact}{suffix}"
    return f"_cleaned_{compact}"


def get_unique_name(parent: Path, name: str, old_path: Path, is_dir: bool) -> str:
    """如目标名已存在，则追加序号避免冲突。"""
    candidate = name
    counter = 1

    if is_dir:
        stem = name
        suffix = ""
    else:
        stem, suffix = os.path.splitext(name)

    while True:
        candidate_path = parent / candidate
        if not candidate_path.exists() or candidate_path == old_path:
            return candidate
        candidate = f"{stem}_{counter}{suffix}"
        counter += 1


def iter_target_paths(root: Path):
    """
    自底向上遍历文件和目录，避免父目录先重命名后影响子路径。
    不处理根目录本身。
    """
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        current = Path(dirpath)
        for filename in filenames:
            yield current / filename
        for dirname in dirnames:
            yield current / dirname


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="清理文件名和目录名中的网址型前缀（如 www.98T.la@）。"
    )
    ap.add_argument(
        "target_dir",
        help="要扫描的根目录",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        default=DRY_RUN,
        help="演练模式，不实际重命名",
    )
    ap.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="关闭演练模式，正式执行",
    )
    ap.add_argument(
        "--empty-strategy",
        choices=["skip", "fallback"],
        default="skip",
        help="清理后为空时的处理策略",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.target_dir)

    if not root.is_dir():
        print(f"错误: 目录不存在: {root}", file=sys.stderr)
        return 1

    print(f"开始扫描目录: {root}")
    print(f"模式: {'演练模式' if args.dry_run else '实际执行'}")
    print(f"空白策略: {args.empty_strategy}\n")

    renamed = 0
    skipped_empty = 0
    unchanged = 0
    failed = 0

    for old_path in iter_target_paths(root):
        old_name = old_path.name
        is_dir = old_path.is_dir()
        cleaned_name = sanitize_name(remove_web_prefixes(old_name))

        if cleaned_name == old_name:
            unchanged += 1
            continue

        if not cleaned_name:
            if args.empty_strategy == "skip":
                print(f"[跳过] 清理后为空: {old_path}")
                skipped_empty += 1
                continue
            cleaned_name = build_fallback_name(old_name, is_dir=is_dir)

        final_name = get_unique_name(
            old_path.parent, cleaned_name, old_path, is_dir=is_dir
        )
        new_path = old_path.parent / final_name

        print(f"[发现] {old_path}")
        print(f"  清理后: {cleaned_name}")
        if final_name != cleaned_name:
            print(f"  冲突处理: {final_name}")

        if args.dry_run:
            print(f"  [DRY_RUN] -> {new_path}\n")
            renamed += 1
            continue

        try:
            old_path.rename(new_path)
            print(f"  [成功] -> {new_path}\n")
            renamed += 1
            time.sleep(SLEEP_SECONDS)
        except Exception as exc:
            print(f"  [失败] {exc}\n")
            failed += 1

    print("处理完成")
    print(f"  计划/已重命名: {renamed}")
    print(f"  空白跳过: {skipped_empty}")
    print(f"  无需处理: {unchanged}")
    print(f"  失败: {failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
