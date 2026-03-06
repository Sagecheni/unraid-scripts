import os
import time
import re
from openai import OpenAI
from dotenv import load_dotenv

# --- Configuration & Setup ---

# Load environment variables
load_dotenv()
# ================= 配置区 =================
# 填写你的 API Key

API_KEY = os.getenv("GLOBALAI_API_KEY")
if not API_KEY:
    raise ValueError("环境变量 GLOBALAI_API_KEY 未设置，请在 .env 文件中配置")

# 填写你的 CD2 挂载路径（请修改为你实际的路径，建议先用一个子文件夹测试！）
TARGET_DIR = "/mnt/user/CloudNAS/CloudDrive/115open/NAS/Hentai"

# 触发重命名的文件名长度阈值（超过此长度的才处理）
MAX_FILENAME_LENGTH = 60

# 是否为”演练模式”？设为 True 时只会打印出结果，不会真实重命名文件。强烈建议先保持 True！
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

# 初始化 Grok 客户端 (利用 OpenAI SDK)
client = OpenAI(api_key=API_KEY, base_url="https://globalai.vip/v1")


def get_shortened_name(old_name):
    prompt = f"""
    你是一个专业的成人视频文件重命名工具。
    请将下面这个过长的视频文件名缩短。

    规则：
    1. 保留人物名称、演员名的原文（英文、日文等），不要翻译
    2. 将动作、场景、剧情描述翻译成简洁的中文
    3. 删除多余的修饰词、重复描述和无意义标点
    4. 长度必须控制在 60 个字符以内
    5. 必须保留原有的文件扩展名（如 .mp4, .mkv, .strm）
    6. 绝对只能输出最终的文件名，不要包含任何解释、确认语或多余内容

    原文件名：{old_name}

    缩短后的文件名：
    """

    try:
        response = client.chat.completions.create(
            model="grok-4-1-fast-non-reasoning",
            messages=[
                {
                    "role": "system",
                    "content": "你是一个严格执行指令的机器程序，只输出处理后的文件名。",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,  # 极低的温度保证输出稳定
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"  [!] API 请求失败: {e}")
        return None


def remove_marker(name):
    """不区分大小写地移除 www.98T.la@ 标记"""
    import re

    return re.sub(r"www\.98t\.la@", "", name, flags=re.IGNORECASE)


def sanitize_filename(filename):
    """清理文件名中的非法字符"""
    # 移除或替换非法字符
    illegal_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(illegal_chars, "", filename)
    # 限制文件名长度（保留扩展名）
    name, ext = os.path.splitext(sanitized)
    if len(sanitized.encode("utf-8")) > 255:
        max_name_len = 255 - len(ext.encode("utf-8")) - 10  # 留一些余量
        name = name[:max_name_len]
        sanitized = name + ext
    return sanitized.strip()


def get_unique_filename(directory, filename):
    """如果文件名已存在，添加序号避免覆盖"""
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath):
        return filename

    name, ext = os.path.splitext(filename)
    counter = 1
    while True:
        new_filename = f"{name}_{counter}{ext}"
        new_filepath = os.path.join(directory, new_filename)
        if not os.path.exists(new_filepath):
            return new_filename
        counter += 1


def main():
    print(f"开始扫描目录: {TARGET_DIR}")

    for root, dirs, files in os.walk(TARGET_DIR):
        # 先处理文件夹名
        for dirname in dirs:
            if re.search(r"www\.98t\.la@", dirname, re.IGNORECASE):
                old_dirpath = os.path.join(root, dirname)
                new_dirname = remove_marker(dirname).lstrip()

                if not new_dirname:
                    print(f"\n[跳过] 文件夹 {dirname} 清理后为空，跳过处理。")
                    continue

                new_dirname = get_unique_filename(root, new_dirname)
                new_dirpath = os.path.join(root, new_dirname)
                print(f"\n[发现带标记的文件夹] {dirname}")
                print(f"  [清理后] -> {new_dirname}")

                if not DRY_RUN:
                    try:
                        os.rename(old_dirpath, new_dirpath)
                        print("  [成功] 文件夹已重命名。")
                        time.sleep(1.5)
                    except Exception as e:
                        print(f"  [失败] 重命名出错: {e}")
                else:
                    print("  [DRY_RUN] 演练模式，未进行实际重命名。")

        for filename in files:
            # 只处理视频文件
            _, ext = os.path.splitext(filename)
            if ext.lower() not in VIDEO_EXTENSIONS:
                continue

            # 先清理文件名中的标记
            if re.search(r"www\.98t\.la@", filename, re.IGNORECASE):
                old_filepath = os.path.join(root, filename)
                cleaned_filename = remove_marker(filename).lstrip()

                if not cleaned_filename:
                    print(f"\n[跳过] 文件 {filename} 清理后为空，跳过处理。")
                    continue

                cleaned_filename = get_unique_filename(root, cleaned_filename)
                new_filepath = os.path.join(root, cleaned_filename)
                print(f"\n[发现带标记的文件] {filename}")
                print(f"  [清理后] -> {cleaned_filename}")

                if not DRY_RUN:
                    try:
                        os.rename(old_filepath, new_filepath)
                        print("  [成功] 文件已重命名。")
                        time.sleep(1.5)
                    except Exception as e:
                        print(f"  [失败] 重命名出错: {e}")
                else:
                    print("  [DRY_RUN] 演练模式，未进行实际重命名。")

                # 更新 filename 为清理后的名字，继续后续长度检查
                filename = cleaned_filename
            # 只处理过长的文件
            if len(filename) > MAX_FILENAME_LENGTH:
                old_filepath = os.path.join(root, filename)
                print(f"\n[发现长文件] {filename}")

                # 请求 AI 获取短文件名
                new_filename = get_shortened_name(filename)

                if not new_filename:
                    print("  [!] AI 未返回建议，跳过此文件。")
                    continue

                if new_filename == filename:
                    print("  [!] AI 返回的文件名与原文件名相同，跳过。")
                    continue

                # 验证扩展名是否保留
                _, old_ext = os.path.splitext(filename)
                _, new_ext = os.path.splitext(new_filename)
                if old_ext.lower() != new_ext.lower():
                    print(
                        f"  [!] AI 更改了文件扩展名（{old_ext} -> {new_ext}），跳过此文件。"
                    )
                    continue

                # 基础的安全检查：确保 AI 没有把后缀弄丢，且没有乱加路径符号
                if "/" in new_filename or "\\" in new_filename:
                    print("  [!] AI 试图更改路径，跳过此文件。")
                    continue

                # 清理非法字符
                new_filename = sanitize_filename(new_filename)
                new_filename = get_unique_filename(root, new_filename)
                new_filepath = os.path.join(root, new_filename)
                print(f"  [AI 建议] -> {new_filename}")

                if not DRY_RUN:
                    try:
                        os.rename(old_filepath, new_filepath)
                        print("  [成功] 文件已重命名。")
                        # 挂载盘重命名建议稍微停顿，避免 115 封禁风控
                        time.sleep(1.5)
                    except Exception as e:
                        print(f"  [失败] 重命名出错: {e}")
                else:
                    print("  [DRY_RUN] 演练模式，未进行实际重命名。")
                    time.sleep(0.5)  # 演练模式也稍微停顿，防止触发 DeepSeek 频率限制


if __name__ == "__main__":
    main()
