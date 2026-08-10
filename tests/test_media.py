"""OneBot 11 同轮媒体去重合同测试。"""

import os
from pathlib import Path

from onebot11.media import MediaDeliveryScope, normalize_media_source


def test同一文件的分隔符和大小写变化可去重(tmp_path: Path):
    """本地路径比较使用 resolve、normpath 和 normcase。"""
    path = tmp_path / "Images With Space" / "Picture.PNG"
    path.parent.mkdir()
    path.write_bytes(b"image")
    variant = str(path).replace(os.sep, "/")
    if os.name == "nt":
        variant = variant.upper()

    scope = MediaDeliveryScope("turn-1")
    assert normalize_media_source(str(path)) != ""
    assert scope.claim(str(path), b"image")[0] is True
    assert scope.would_duplicate(variant, b"image") is True


def testURL和内容hash在同一turn内去重():
    """不同来源指向相同图片内容时只允许第一次 claim。"""
    scope = MediaDeliveryScope("turn-2")
    assert scope.claim("HTTPS://CDN.Example.COM:443/image.png#fragment", b"same")[0]
    assert scope.would_duplicate("https://cdn.example.com/image.png", b"same")
    assert scope.claim("https://cdn.example.com/other.png", b"other")[0]


def test不同scope不互相拦截():
    """去重不跨 turn，下一轮仍可发送同一文件。"""
    first = MediaDeliveryScope("turn-1")
    second = MediaDeliveryScope("turn-2")
    assert first.claim("/tmp/image.png", b"same")[0]
    assert second.claim("/tmp/image.png", b"same")[0]
