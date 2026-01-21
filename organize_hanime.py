#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import re
import pathlib
import logging
import argparse  # 新增：用于解析参数


def get_clean_info(filename):
    """解析系列名、集数和版本信息"""
    stem = pathlib.Path(filename).stem

    # 1. 提取版本信息
    version = ""
    for v in ["4K", "2K", "60FPS", "1080P", "CHS", "简体"]:
        if v in stem.upper():
            version += f"[{v}]"

    # 2. 提取集数
    episode = ""
    ep_match = re.search(
        r"(?:第|ep\.?|＃|#|Vol\.?)\s*([0-9一二三四五六七八九十]+)", stem, re.I
    )
    if ep_match:
        num = ep_match.group(1)
        mapping = {
            "一": "01",
            "二": "02",
            "三": "03",
            "四": "04",
            "五": "05",
            "六": "06",
            "七": "07",
            "八": "08",
            "九": "09",
            "十": "10",
            "上": "01",
            "中": "02",
            "下": "03",
            "前": "01",
            "后": "02",
            "後": "02",
        }
        episode = mapping.get(num, num.zfill(2))

    # 3. 提取系列名
    name = re.sub(r"^(\[[^\]]+\]|[\s_])+", "", stem)
    split_patterns = [
        r"\s+第",
        r"\s*[\(\（]",
        r"\s*ep\.?",
        r"\s*＃",
        r"\s*#",
        r"\s*Vol\.?",
        r"\s*其の",
    ]
    for p in split_patterns:
        match = re.search(p, name, re.I)
        if match:
            name = name[: match.start()]
            break

    return name.strip(" _-"), episode, version


def main():
    # --- 参数解析器 ---
    parser = argparse.ArgumentParser(description="HAnime Series Organizer")
    parser.add_argument("--source", required=True, help="原始链接目录 (raw_links)")
    parser.add_argument("--target", required=True, help="整理后的目录 (Organized)")
    parser.add_argument("--log", default=None, help="日志文件路径")
    args = parser.parse_args()

    source_dir = args.source
    target_dir = args.target

    # 输入验证
    if not os.path.exists(source_dir):
        print(f"错误: 源目录不存在: {source_dir}")
        return 1

    if not os.path.isdir(source_dir):
        print(f"错误: 源路径不是目录: {source_dir}")
        return 1

    if os.path.abspath(source_dir) == os.path.abspath(target_dir):
        print(f"错误: 源目录和目标目录不能相同")
        return 1

    # 日志配置
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if args.log:
        handlers.append(logging.FileHandler(args.log, encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    logging.info(f"开始整理: {source_dir} -> {target_dir}")

    current_links = set()
    extensions = {".mp4", ".mkv", ".avi", ".wmv", ".m4v"}

    for root, dirs, files in os.walk(source_dir):
        for file in files:
            ext = pathlib.Path(file).suffix.lower()
            if ext not in extensions:
                continue

            src_path = os.path.join(root, file)
            series, ep, ver = get_clean_info(file)

            if len(series) < 2:
                series = os.path.basename(root) if "202" not in root else "Unknown"

            new_filename = f"{series} - S01E{ep if ep else '01'} {ver}{ext}"
            dest_folder = os.path.join(target_dir, series)
            if not os.path.exists(dest_folder):
                os.makedirs(dest_folder)

            dest_path = os.path.join(dest_folder, new_filename)
            current_links.add(dest_path)

            if not os.path.exists(dest_path):
                try:
                    os.symlink(src_path, dest_path)
                    logging.info(f"[新增] {new_filename}")
                except OSError as e:
                    logging.error(f"[创建链接失败] {file}: {e}")
                except Exception as e:
                    logging.error(f"[未知错误] {file}: {e}")

    # 清理失效链接（只删除目标不存在的链接）
    for root, dirs, files in os.walk(target_dir):
        for file in files:
            f_path = os.path.join(root, file)
            if os.path.islink(f_path):
                try:
                    link_target = os.readlink(f_path)
                    # 只删除指向不存在文件的失效链接
                    if not os.path.exists(link_target):
                        os.remove(f_path)
                        logging.warning(f"[清理失效链接] {file}")
                except OSError as e:
                    logging.error(f"[链接错误] {file}: {e}")
                    try:
                        os.remove(f_path)
                        logging.warning(f"[清理损坏链接] {file}")
                    except Exception as e2:
                        logging.error(f"[删除失败] {file}: {e2}")

    # 清理空目录
    for root, dirs, files in os.walk(target_dir, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    logging.info(f"[清理空目录] {dir_name}")
            except OSError:
                pass  # 目录可能不为空或无权限删除


if __name__ == "__main__":
    main()
