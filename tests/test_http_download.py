"""OneBotHttpApi.download_to_temp 测试。"""

import os
from urllib.parse import urlsplit

import pytest
from aiohttp import web

from onebot11.http_api import OneBotHttpApi


@pytest.fixture
async def file_server():
    """提供 GET /file.png 的静态文件服务。"""
    app = web.Application()

    async def _png(_request: web.Request) -> web.Response:
        png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00IEND\xaeB\x60\x82"
        return web.Response(body=png, content_type="image/png")

    app.router.add_get("/file.png", _png)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    yield base
    await runner.cleanup()


async def test_下载成功(tmp_path, file_server):
    api = OneBotHttpApi(
        base_url="http://127.0.0.1:1",
        allowed_media_ports={int(urlsplit(file_server).port or 0)},
    )
    path = await api.download_to_temp(f"{file_server}/file.png", str(tmp_path))
    assert path is not None
    assert path.endswith(".png")
    assert os.path.basename(path).endswith(".png")
    with open(path, "rb") as f:
        assert f.read().startswith(b"\x89PNG\r\n\x1a\n")
    await api.close()


async def test_下载404返回None(tmp_path, file_server):
    api = OneBotHttpApi(base_url="http://127.0.0.1:1")
    path = await api.download_to_temp(f"{file_server}/missing.png", str(tmp_path))
    assert path is None
    await api.close()


async def test_下载网络错误返回None(tmp_path):
    api = OneBotHttpApi(base_url="http://127.0.0.1:1")
    path = await api.download_to_temp("http://127.0.0.1:1/never.png", str(tmp_path))
    assert path is None
    await api.close()


async def test_没有媒体host_allowlist时拒绝外部地址(tmp_path):
    """媒体下载不能在空 base_url 配置下默认访问任意外部域名。"""
    api = OneBotHttpApi(base_url="")
    try:
        with pytest.raises(ValueError):
            api._validate_media_url("https://example.com/image.png")
    finally:
        await api.close()


async def test_图片重定向不携带OneBot令牌(tmp_path):
    """媒体请求及其每一跳重定向都不能泄露 OneBot Bearer token。"""
    seen_auth: list[str] = []
    png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00IEND\xaeB`\x82"
    app = web.Application()

    async def redirect(request: web.Request) -> web.Response:
        seen_auth.append(request.headers.get("Authorization", ""))
        return web.Response(status=302, headers={"Location": "/file.png"})

    async def file(request: web.Request) -> web.Response:
        seen_auth.append(request.headers.get("Authorization", ""))
        return web.Response(body=png, content_type="image/png")

    app.router.add_get("/redirect", redirect)
    app.router.add_get("/file.png", file)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(base_url=base, token="do-not-leak")
    try:
        path = await api.download_to_temp(f"{base}/redirect", str(tmp_path))
        assert path is not None
        assert seen_auth == ["", ""]
    finally:
        await api.close()
        await runner.cleanup()


async def test_get_image使用OneBot查询动作(monkeypatch):
    """file 标识通过标准 get_image 解析，不把它当作外部 URL。"""
    api = OneBotHttpApi(base_url="http://127.0.0.1:3000")

    async def fake_call_action(action: str, params: dict, **_kwargs):
        assert action == "get_image"
        assert params == {"file": "file-id-1"}
        return {"file": "C:/safe/image.png"}

    monkeypatch.setattr(api, "call_action", fake_call_action)
    try:
        assert await api.get_image("file-id-1") == "C:/safe/image.png"
    finally:
        await api.close()
