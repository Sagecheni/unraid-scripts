# Role
你是一位精通Python自动化、文件系统操作及媒体服务器管理（Emby/Plex）的系统架构师。你擅长使用LLM API（特别是DeepSeek）解决非结构化数据的清洗与整理问题，并能够编写健壮、高效的工程化代码。

# Context
我正在管理一个基于Unraid的媒体库。为了配合`emby2openlist`实现115网盘的302直链播放，我的文件系统采用了特殊的“双层软链接”架构：
1.  **原始层 (Layer 1)**: `/mnt/user/embydata/raw_links/Hanime/`
    -   这是由`auto_symlink`工具从CD2挂载点生成的。
    -   目录结构混乱，包含日期命名的文件夹（如`2024/`, `2025/`）和分类文件夹（如`Cartoon/`）。
2.  **整理层 (Layer 2)**: `/mnt/user/embydata/links/HAnime/` (本脚本的目标)
    -   **你的任务**是生成指向Layer 1的软链接，但以完美的番剧结构组织。

# Task
请编写一个Python脚本 (`organize_hanime.py`)，完成以下工作：

1.  **环境配置**:
    -   使用 `python-dotenv` 加载 `.env` 文件中的 `DEEPSEEK_API_KEY`。
    -   定义 `SOURCE_ROOT` 为 `/mnt/user/embydata/raw_links/Hanime/`。
    -   定义 `TARGET_ROOT` 为 `/mnt/user/embydata/links/HAnime/`。
    -   定义缓存文件路径 `CACHE_FILE` 为 `hanime_cache.json`。

2.  **缓存机制 (Cache System)**:
    -   实现一个本地JSON缓存数据库，用于存储 `{ "file_hash_or_path": "parsed_result" }`。
    -   在处理每个文件前，先检查缓存。如果该文件（基于相对路径或文件名哈希）通过DeepSeek解析过，直接使用缓存结果，跳过API调用。
    -   脚本执行结束时（或通过 `atexit` 钩子）自动保存缓存到硬盘，避免因中断而丢失进度。

3.  **遍历与结构保持 (关键)**:
    -   遍历 `SOURCE_ROOT` 下的一级子目录（如 `2024`, `2025`, `Cartoon`, `[71]xxx`）。
    -   **约束**：必须严格保留这些一级目录结构。即 `raw_links/2024/video.mkv` 的整理版软链必须位于 `links/HAnime/2024/系列名/video.mkv`，严禁跨一级目录移动（例如不能把2024里的文件移到2025里）。
    -   递归遍历一级目录下的所有视频文件。

4.  **智能解析 (DeepSeek API Batching)**:
    -   收集未在缓存中的文件名。
    -   **Batch处理**：每20-50个文件名打包成以此Prompt发送给DeepSeek API (OpenAI SDK兼容模式, base_url="https://api.deepseek.com")。
    -   **提取目标**:
        -   `series_name`: 纯净的系列名（剔除 `[250131]`, `[字幕组]` 等前缀，通过 `第x话`, `ep` 等关键词截取）。
        -   `season`: 季数 (默认为1)。
        -   `episode`: 标准化集数 (S01E01格式)。
        -   `extra`: 保留版本信息 (如 4K, 60FPS, CHS, Uncensored)。

5.  **执行操作**:
    -   在 `TARGET_ROOT` 下对应的 `一级目录/系列名/` 结构中创建文件夹。
    -   创建软链接：`Target_Path -> Source_Path`。
    -   **版本共存**：如果遇到同系列同集数的多版本文件（如既有1080p又有4K），应保留两者（通过在文件名追加后缀区分），不要覆盖。

6.  **日志与容错**:
    -   脚本需有“Dry Run”（空跑）模式开关，默认开启（`DRY_RUN = True`），只打印操作不执行系统变更。
    -   使用 `logging` 模块记录详细日志：哪些文件解析失败，哪些链接创建成功，API消耗Token数等。

# Data Structure for LLM
告诉LLM，返回的数据必须是严格的JSON List格式，且能够被 `json.loads` 解析。例如：
```json
[
  {
    "original_filename": "[魔人] 魔法闘姫リルスティア 第二話.mkv",
    "series_name": "魔法闘姫リルスティア",
    "standard_filename": "魔法闘姫リルスティア - S01E02.mkv"
  }
]
```

# Constraints
-   **禁止**：不要修改、移动或删除 `SOURCE_ROOT` 下的任何源文件。只创建Symlink。
-   **唯一来源**：即使文件在 `SOURCE_ROOT` 的深层子目录中（如 `2025/2025年01月合集/CHS/`），在 Layer 2 中也应被“打平”到 `2025/系列名/` 下，去掉中间的冗余目录。
-   **依赖**：使用 `pathlib` 处理路径，`openai` 处理API请求。
-   **兼容性**：确保生成的软链接路径是绝对路径。

# Format
-   请输出完整的Python代码 (`organize_hanime.py`)。
-   提供 `requirements.txt`。
-   提供 `.env.example` 模板。
