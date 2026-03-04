import os
from collections import defaultdict

# ================= 配置区 =================
# 目标目录
TARGET_DIR = "/mnt/user/CloudNAS/CloudDrive/115open/NAS/"

# 是否为演练模式
DRY_RUN = True

# 视频文件扩展名
VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".wmv",
    ".flv",
    ".webm",
    ".m4v",
    ".mpg",
    ".mpeg",
    ".strm",
}
# ==========================================


def find_duplicate_videos(target_dir):
    """查找所有重名的视频文件"""
    filename_map = defaultdict(list)

    # 遍历所有文件
    for root, dirs, files in os.walk(target_dir):
        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext.lower() in VIDEO_EXTENSIONS:
                filepath = os.path.join(root, filename)
                filename_map[filename].append(filepath)

    # 只返回重名的文件
    duplicates = {name: paths for name, paths in filename_map.items() if len(paths) > 1}
    return duplicates


def rename_duplicates(duplicates):
    """为重名文件添加序号"""
    for filename, paths in duplicates.items():
        print(f"\n[发现重名] {filename} (共 {len(paths)} 个)")

        name, ext = os.path.splitext(filename)

        for idx, old_path in enumerate(paths, start=1):
            if idx == 1:
                # 第一个文件保持原名
                print(f"  保持: {old_path}")
                continue

            # 其他文件添加序号
            new_filename = f"{name}_{idx}{ext}"
            directory = os.path.dirname(old_path)
            new_path = os.path.join(directory, new_filename)

            print(f"  重命名: {old_path}")
            print(f"       -> {new_path}")

            if not DRY_RUN:
                try:
                    os.rename(old_path, new_path)
                    print("  [成功]")
                except Exception as e:
                    print(f"  [失败] {e}")
            else:
                print("  [DRY_RUN] 演练模式")


def main():
    print(f"开始扫描目录: {TARGET_DIR}")
    print(f"模式: {'演练模式' if DRY_RUN else '实际执行'}\n")

    duplicates = find_duplicate_videos(TARGET_DIR)

    if not duplicates:
        print("未发现重名视频文件。")
        return

    print(f"共发现 {len(duplicates)} 组重名文件\n")
    rename_duplicates(duplicates)

    print("\n完成！")


if __name__ == "__main__":
    main()
