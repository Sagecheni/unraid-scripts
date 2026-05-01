#!/usr/bin/env python3
"""监控 Unraid 中单个文件是否被删除，并记录触发删除的进程信息。"""

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


DEFAULT_KEY_PREFIX = "watch_delete"
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_SETTLE_SECONDS = 1.5
DEFAULT_LOOKBACK_SECONDS = 30
DEFAULT_LOG_PATH = Path("./logs/file_delete_audit.log")

KV_PATTERN = re.compile(r'(\w+)=(".*?"|\S+)')
TIME_PATTERN = re.compile(r"^time->(?P<value>.+)$")


@dataclass(frozen=True)
class DeleteEvent:
    timestamp: str
    path: str
    exe: str
    comm: str
    pid: str
    ppid: str
    uid: str
    auid: str
    cwd: str
    key: str
    raw: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "监控单个文件是否被删除。依赖 Linux auditd，删除发生后会从审计日志中提取"
            "执行删除动作的进程、PID、可执行文件路径、工作目录等信息。"
        )
    )
    parser.add_argument("--file", required=True, help="要监控的绝对路径文件")
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_PATH),
        help=f"事件输出日志路径，默认 {DEFAULT_LOG_PATH}",
    )
    parser.add_argument(
        "--key",
        help="audit 规则 key；默认会基于文件名自动生成",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help=f"轮询文件存在性的间隔秒数，默认 {DEFAULT_POLL_INTERVAL}",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=DEFAULT_SETTLE_SECONDS,
        help=f"发现文件消失后等待 audit 日志落盘秒数，默认 {DEFAULT_SETTLE_SECONDS}",
    )
    parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=DEFAULT_LOOKBACK_SECONDS,
        help=f"从最近多少秒审计日志中查删除事件，默认 {DEFAULT_LOOKBACK_SECONDS}",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="命中一次删除事件后立即退出；默认开启",
    )
    parser.add_argument(
        "--skip-install-rule",
        action="store_true",
        help="不自动安装 audit watch 规则，适合规则已预先存在时使用",
    )
    return parser.parse_args()


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("unraid_file_delete_audit")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def ensure_commands_exist(commands: list[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        raise SystemExit(
            "缺少必要命令: "
            + ", ".join(missing)
            + "。Unraid 上先安装 NerdTools/包管理器中的 auditd。"
        )


def build_watch_key(target_path: Path, user_key: str | None) -> str:
    if user_key:
        return user_key
    suffix = re.sub(r"[^A-Za-z0-9_]+", "_", target_path.name).strip("_") or "file"
    return f"{DEFAULT_KEY_PREFIX}_{suffix}"


def run_command(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )


def install_watch_rule(target_path: Path, key: str) -> None:
    result = run_command(["auditctl", "-w", str(target_path), "-p", "wa", "-k", key])
    if result.returncode != 0:
        stderr = (result.stderr or "").strip() or "未知错误"
        raise SystemExit(f"安装 audit 规则失败: {stderr}")


def delete_watch_rule(target_path: Path) -> None:
    run_command(["auditctl", "-W", str(target_path)])


def parse_key_value_line(line: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for key, raw_value in KV_PATTERN.findall(line):
        values[key] = (
            raw_value[1:-1]
            if raw_value.startswith('"') and raw_value.endswith('"')
            else raw_value
        )
    return values


def parse_ausearch_output(output: str, target_path: Path) -> DeleteEvent | None:
    blocks = [block.strip() for block in output.split("----") if block.strip()]
    target = str(target_path)

    for block in reversed(blocks):
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        timestamp = ""
        path_values: list[dict[str, str]] = []
        syscall_values: dict[str, str] = {}
        cwd = ""

        for line in lines:
            time_match = TIME_PATTERN.match(line)
            if time_match:
                timestamp = time_match.group("value")
                continue

            parsed = parse_key_value_line(line)
            if line.startswith("type=PATH "):
                path_values.append(parsed)
                continue
            if line.startswith("type=SYSCALL "):
                syscall_values = parsed
                continue
            if line.startswith("type=CWD "):
                cwd = parsed.get("cwd", "")

        matched_path = next(
            (item for item in path_values if item.get("name") == target), None
        )
        if matched_path is None:
            continue
        if matched_path.get("nametype") not in {"DELETE", "NORMAL", "PARENT"}:
            continue

        return DeleteEvent(
            timestamp=timestamp,
            path=matched_path.get("name", target),
            exe=syscall_values.get("exe", ""),
            comm=syscall_values.get("comm", ""),
            pid=syscall_values.get("pid", ""),
            ppid=syscall_values.get("ppid", ""),
            uid=syscall_values.get("uid", ""),
            auid=syscall_values.get("auid", ""),
            cwd=cwd,
            key=syscall_values.get("key", ""),
            raw=block,
        )
    return None


def find_delete_event(
    target_path: Path, key: str, lookback_seconds: int
) -> DeleteEvent | None:
    command = [
        "ausearch",
        "-k",
        key,
        "-ts",
        "recent",
        "-i",
    ]
    result = run_command(command)
    if result.returncode != 0:
        return None

    event = parse_ausearch_output(result.stdout, target_path)
    if event is not None:
        return event

    fallback = run_command(
        [
            "ausearch",
            "-f",
            str(target_path),
            "-ts",
            "recent",
            "-i",
        ]
    )
    if fallback.returncode != 0:
        return None
    return parse_ausearch_output(fallback.stdout, target_path)


def append_event_log(log_path: Path, event: DeleteEvent) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")


def watch_once(
    target_path: Path,
    key: str,
    poll_interval: float,
    settle_seconds: float,
    lookback_seconds: int,
    logger: logging.Logger,
    log_path: Path,
) -> bool:
    while True:
        if target_path.exists():
            time.sleep(poll_interval)
            continue

        logger.warning("检测到文件已消失: %s", target_path)
        time.sleep(settle_seconds)
        event = find_delete_event(target_path, key, lookback_seconds)
        if event is None:
            logger.error("未能从 audit 日志定位删除进程: %s", target_path)
            return False

        append_event_log(log_path, event)
        logger.info(
            "已记录删除事件: comm=%s exe=%s pid=%s path=%s",
            event.comm,
            event.exe,
            event.pid,
            event.path,
        )
        return True


def main() -> int:
    args = parse_args()
    target_path = Path(args.file).expanduser().resolve()
    log_path = Path(args.log_file).expanduser().resolve()
    logger = setup_logging(log_path)

    ensure_commands_exist(["auditctl", "ausearch"])

    key = build_watch_key(target_path, args.key)
    logger.info("开始监控文件删除事件: %s", target_path)
    logger.info("audit key: %s", key)

    installed_rule = False
    try:
        if not args.skip_install_rule:
            install_watch_rule(target_path, key)
            installed_rule = True
            logger.info("已安装 audit watch 规则")

        success = watch_once(
            target_path=target_path,
            key=key,
            poll_interval=args.poll_interval,
            settle_seconds=args.settle_seconds,
            lookback_seconds=args.lookback_seconds,
            logger=logger,
            log_path=log_path,
        )
        return 0 if success else 1
    finally:
        if installed_rule:
            delete_watch_rule(target_path)
            logger.info("已移除 audit watch 规则")


if __name__ == "__main__":
    raise SystemExit(main())
