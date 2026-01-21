# HAnime 媒体库自动整理工具使用指南

本工具用于自动整理 HAnime 媒体库，利用 DeepSeek LLM 将混乱的文件名解析为标准化的 `系列名 - S01EXX` 格式，并创建软链接（Symlinks），同时保留原始的一级目录结构。

## ✨ 功能特点

*   **智能解析**: 使用 DeepSeek V3 API 批量精准识别动画系列名、集数及版本信息。
*   **非破坏性**: 仅在目标目录生成软链接，绝不修改、移动或删除源文件。
*   **结构保持**: 严格遵守源目录的一级分类（如 `2024`, `2025`），将其作为目标目录的顶层结构。
*   **本地缓存**: 内置 JSON 缓存系统，避免重复消耗 API Token，二次运行速度极快。
*   **安全模式**: 默认开启 Dry Run（空跑）模式，便于在实际执行前预览变更。

## 🛠️ 安装与配置

### 1. 环境准备
确保已安装 Python 3.8 或更高版本。

```bash
# 安装依赖库
pip install -r requirements.txt
```

### 2. 配置文件
复制 `.env.example` 为 `.env` 并进行编辑：

```bash
cp .env.example .env
```

编辑 `.env` 文件，填入你的 DeepSeek API Key：

```ini
DEEPSEEK_API_KEY=sk-your_api_key_here

# (可选) 自定义路径，如果不设置则使用脚本内的默认路径
# SOURCE_ROOT=/mnt/user/embydata/raw_links/Hanime/
# TARGET_ROOT=/mnt/user/embydata/links/HAnime/
```

## 🚀 使用方法

### 1. 首次运行 (测试模式)
脚本默认处于 **Dry Run** 模式。运行脚本将扫描文件、调用 API 解析文件名（会产生少量 API 消耗），并打印出“将要创建链接”的日志，但不会在磁盘上进行任何写入操作。

```bash
python organize_hanime.py
```

查看控制台输出或 `organize_hanime.log` 日志文件，检查解析结果是否符合预期。
例如：
```text
[DRY RUN] Link: /mnt/.../2024/系列名/XX - S01E01.mkv -> /mnt/.../raw/2024/source.mkv
```

### 2. 正式执行
确认日志无误后，打开 `organize_hanime.py` 文件，修改第 34 行左右的配置：

```python
# 修改前
DRY_RUN = True

# 修改后
DRY_RUN = False
```

再次运行脚本即可生成软链接：

```bash
python organize_hanime.py
```

## 📂 目录结构示例

**源目录 (Source Layer 1)**:
```text
/mnt/user/embydata/raw_links/Hanime/
├── 2024/
│   ├── [240101] [Group] 某动画 Ep1.mkv
│   └── random_folder/
│       └── [240102] 某动画 Ep2.mkv
└── Cartoon/
    └── [Pixar] 某3D动画.mp4
```

**整理后 (Target Layer 2)**:
```text
/mnt/user/embydata/links/HAnime/
├── 2024/
│   └── 某动画/
│       ├── 某动画 - S01E01.mkv (-> link to Ep1)
│       └── 某动画 - S01E02.mkv (-> link to Ep2)
└── Cartoon/
    └── 某3D动画/
        └── 某3D动画 - S01E01.mp4 (-> link to file)
```

## ❓ 常见问题

**Q: 如何强制重新解析文件？**
A: 删除目录下的 `hanime_cache.json` 文件即可。

**Q: 遇到同名文件如何处理？**
A: 脚本会自动检测目标路径是否已存在。如果是指向相同源文件的链接，跳过；如果是不同源文件的冲突（如不同版本），脚本会自动在文件名后追加 `_v1`, `_v2` 等后缀以实现共存。
