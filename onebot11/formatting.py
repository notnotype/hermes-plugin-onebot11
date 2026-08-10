"""OneBot 11 的纯文本显示和 Markdown 图片 marker 解析。"""

from __future__ import annotations

import re
from dataclasses import dataclass

MARKDOWN_IMAGE_OPEN = "[[onebot11:markdown-image]]"
MARKDOWN_IMAGE_CLOSE = "[[/onebot11:markdown-image]]"


@dataclass(frozen=True)
class FormattedText:
    """纯文本转换结果及是否发现图片渲染请求。"""

    text: str
    markdown_image_requested: bool = False


def _unwrap_markdown_image_markers(text: str) -> tuple[str, bool]:
    """移除 OneBot 图片 marker，但保留其中的 Markdown 内容。"""
    requested = MARKDOWN_IMAGE_OPEN in text or MARKDOWN_IMAGE_CLOSE in text
    pattern = re.compile(
        re.escape(MARKDOWN_IMAGE_OPEN) + r"(.*?)" + re.escape(MARKDOWN_IMAGE_CLOSE),
        re.DOTALL,
    )
    unwrapped = pattern.sub(lambda match: match.group(1), text)
    return (
        unwrapped.replace(MARKDOWN_IMAGE_OPEN, "").replace(MARKDOWN_IMAGE_CLOSE, ""),
        requested,
    )


def unwrap_markdown_image_markers(content: str) -> tuple[str, bool]:
    """公开 marker 清理入口，供关闭纯文本转换时仍避免 marker 泄漏。"""
    return _unwrap_markdown_image_markers(str(content or ""))


def _protect_code(text: str) -> tuple[str, list[str]]:
    """去除代码围栏和反引号，同时保护代码内容不被强调规则改写。"""
    protected: list[str] = []
    output: list[str] = []
    in_fence = False
    fence_lines: list[str] = []

    def token(value: str) -> str:
        """把代码内容替换成本轮唯一的不可见占位符。"""
        index = len(protected)
        protected.append(value)
        return f"\x00ONEBOT_CODE_{index}\x00"

    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith("```"):
            if in_fence:
                output.append(token("".join(fence_lines)))
                fence_lines = []
                in_fence = False
            else:
                in_fence = True
            continue
        if in_fence:
            fence_lines.append(line)
        else:
            output.append(line)
    if in_fence:
        output.append(token("".join(fence_lines)))

    unprotected = "".join(output)
    unprotected = re.sub(
        r"`([^`\n]+)`",
        lambda match: token(match.group(1)),
        unprotected,
    )
    return unprotected, protected


def _restore_code(text: str, protected: list[str]) -> str:
    """恢复被强调清理规则保护的代码内容。"""
    for index, value in enumerate(protected):
        text = text.replace(f"\x00ONEBOT_CODE_{index}\x00", value)
    return text


def format_onebot_text(content: str) -> FormattedText:
    """把常见 Markdown 转为 OneBot 可读的纯文本。"""
    unwrapped, requested = _unwrap_markdown_image_markers(str(content or ""))
    text, protected = _protect_code(unwrapped)

    # 链接和图片链接都保留标题与 URL，不访问 URL，也不把 Markdown 语法泄漏出去。
    text = re.sub(
        r"!\[([^\]\n]*)\]\(([^)\n]+)\)",
        lambda match: f"{match.group(1)} ({match.group(2)})",
        text,
    )
    text = re.sub(
        r"\[([^\]\n]+)\]\(([^)\n]+)\)",
        lambda match: f"{match.group(1)} ({match.group(2)})",
        text,
    )
    text = re.sub(r"(?m)^[ \t]{0,3}#{1,6}[ \t]+", "", text)
    text = re.sub(r"~~([^~\n]+)~~", r"\1", text)
    text = re.sub(
        r"(?<!\w)(\*{2}|__)(?=\S)(.+?)(?<=\S)\1",
        r"\2",
        text,
    )
    text = re.sub(
        r"(?<!\w)(\*|_)(?=\S)(.+?)(?<=\S)\1",
        r"\2",
        text,
    )
    text = re.sub(r"\\([\\`*_{}\[\]()#+.!|>~-])", r"\1", text)
    return FormattedText(_restore_code(text, protected).strip(), requested)
