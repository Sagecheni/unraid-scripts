import os
import time
from openai import OpenAI
from dotenv import load_dotenv

# --- Configuration & Setup ---

# Load environment variables
load_dotenv()
# ================= 配置区 =================
# 填写你的 DeepSeek API Key

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

# 填写你的 CD2 挂载路径（请修改为你实际的路径，建议先用一个子文件夹测试！）
TARGET_DIR = "/mnt/user/CloudNAS/CloudDrive/115open/NAS/98堂"

# 触发重命名的文件名长度阈值（超过此长度的才处理）
MAX_FILENAME_LENGTH = 60

# 是否为”演练模式”？设为 True 时只会打印出结果，不会真实重命名文件。强烈建议先保持 True！
DRY_RUN = True

# 视频文件扩展名
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpg', '.mpeg', '.strm'}
# ==========================================

# 初始化 DeepSeek 客户端 (利用 OpenAI SDK)
client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")


def get_shortened_name(old_name):
    prompt = f"""
    你是一个专业的成人视频文件重命名工具。
    请将下面这个过长的视频文件名缩短。
    规则：
    1. 提取核心的主标题、演员特征（如年龄、身材、外貌）、关键场景描述、番号或系列名。
    2. 删除多余的修饰词、重复描述和无意义标点，但保留能体现视频内容的本质描述。
    3. 长度必须控制在 60 个字符以内（优先使用简洁的关键词组合）。
    4. 必须保留原有的文件扩展名（如 .mp4, .mkv, .strm）。
    5. 绝对只能输出最终的文件名，不要包含任何解释、确认语或多余内容。
    原文件名：{old_name}
    """

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
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
        print(f"  [!] DeepSeek API 请求失败: {e}")
        return None


def main():
    print(f"开始扫描目录: {TARGET_DIR}")

    for root, dirs, files in os.walk(TARGET_DIR):
        # 先处理文件夹名
        for dirname in dirs:
            if "www.98T.la@" in dirname:
                old_dirpath = os.path.join(root, dirname)
                new_dirname = dirname.replace("www.98T.la@", "").lstrip()

                if not new_dirname:
                    print(f"\n[跳过] 文件夹 {dirname} 清理后为空，跳过处理。")
                    continue

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
            if "www.98T.la@" in filename:
                old_filepath = os.path.join(root, filename)
                cleaned_filename = filename.replace("www.98T.la@", "").lstrip()

                if not cleaned_filename:
                    print(f"\n[跳过] 文件 {filename} 清理后为空，跳过处理。")
                    continue

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

                print(f"  [调试] AI 返回: {new_filename}")

                if new_filename == filename:
                    print("  [!] AI 返回的文件名与原文件名相同，跳过。")
                    continue

                if new_filename and new_filename != filename:
                    # 基础的安全检查：确保 AI 没有把后缀弄丢，且没有乱加路径符号
                    if "/" in new_filename or "\\" in new_filename:
                        print("  [!] AI 试图更改路径，跳过此文件。")
                        continue

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
                        time.sleep(
                            0.5
                        )  # 演练模式也稍微停顿，防止触发 DeepSeek 频率限制


if __name__ == "__main__":
    main()
