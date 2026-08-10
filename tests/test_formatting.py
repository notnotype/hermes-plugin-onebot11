"""OneBot 11 纯文本和 Markdown marker 合同测试。"""

from onebot11.formatting import format_onebot_text


def test_markdown转纯文本并保留链接代码列表和中文():
    """常见 Markdown 不能以语法标记形式出现在 QQ 文本里。"""
    result = format_onebot_text(
        "# 标题\n\n**重点** 和 *说明*\n\n"
        "- 第一项\n- 第二项\n\n"
        "`inline()`\n\n"
        "```python\nprint('中文')\n```\n\n"
        "[Hermes](https://example.com)"
    )

    assert result.text == (
        "标题\n\n重点 和 说明\n\n"
        "- 第一项\n- 第二项\n\n"
        "inline()\n\n"
        "print('中文')\n\n"
        "Hermes (https://example.com)"
    )
    assert "`" not in result.text
    assert "**" not in result.text


def test_markdown图片marker不会泄漏也不会访问外部地址():
    """图片 marker 在 renderer 缺失时只转纯文本并报告请求。"""
    result = format_onebot_text(
        "前文\n[[onebot11:markdown-image]]\n"
        "![图](https://external.invalid/image.png)\n"
        "[[/onebot11:markdown-image]]\n后文"
    )

    assert result.markdown_image_requested is True
    assert "onebot11:markdown-image" not in result.text
    assert "![图]" not in result.text
    assert "图 (https://external.invalid/image.png)" in result.text


def test未闭合marker也不会泄漏():
    """异常或截断的 marker 不能原样发给用户。"""
    result = format_onebot_text("[[onebot11:markdown-image]]内容")
    assert result.markdown_image_requested is True
    assert result.text == "内容"
