"""OneBot 11 消息段（array 格式）解析。

v1 只支持 array 格式（messageFormat: array），CQ 字符串格式暂不支持。
本模块零 Hermes 依赖，可独立测试。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ParsedMessage:
    """从消息段数组解析出的结构化结果。

    - text: 所有 text 段拼接的纯文本（不含 at 段）
    - images: 图片段列表（file 或 url，按出现顺序）
    - mentioned_qq: 被 @ 的他人 QQ 号列表
    - mentioned_self: 是否 @ 了机器人自己
    """

    text: str = ""
    images: list[str] = field(default_factory=list)
    mentioned_qq: list[str] = field(default_factory=list)
    mentioned_self: bool = False


def parse_message_segments(segments: list[dict] | str, self_id: str | None = None) -> ParsedMessage:
    """把 OneBot 11 array 格式消息段解析为 ParsedMessage。

    未知段类型（face/reply 等）静默忽略；字符串格式（CQ 码）抛
    NotImplementedError（v1 只支持 array）。
    """
    if isinstance(segments, str):
        raise NotImplementedError("v1 只支持 array 消息格式,请把 messageFormat 设为 array")

    result = ParsedMessage()
    for seg in segments or []:
        seg_type = seg.get("type")
        data = seg.get("data") or {}
        if seg_type == "text":
            result.text += data.get("text", "")
        elif seg_type == "at":
            qq = str(data.get("qq", ""))
            if qq == "all":
                continue
            if self_id is not None and qq == str(self_id):
                result.mentioned_self = True
            else:
                result.mentioned_qq.append(qq)
        elif seg_type == "image":
            file_id = data.get("file") or data.get("url")
            if file_id:
                result.images.append(str(file_id))
        # 其余段类型（face/reply/record/video 等）v1 忽略
    return result
