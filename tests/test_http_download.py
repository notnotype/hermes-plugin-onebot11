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
        allowed_media_hosts={"127.0.0.1"},
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
    api = OneBotHttpApi(
        base_url=base,
        token="do-not-leak",
        allowed_media_hosts={"127.0.0.1"},
        allowed_media_ports={int(urlsplit(base).port or 0)},
    )
    try:
        path = await api.download_to_temp(f"{base}/redirect", str(tmp_path))
        assert path is not None
        assert seen_auth == ["", ""]
    finally:
        await api.close()
        await runner.cleanup()


async def test_重定向到未授权host或port会被阻断(tmp_path):
    """每一跳重定向都重新检查 host/port，不能借第一跳 allowlist 绕过。"""
    # example.com 不在显式媒体 host allowlist；请求不应真的访问外网。
    app = web.Application()

    async def blocked_host(_request: web.Request) -> web.Response:
        return web.Response(
            status=302,
            headers={"Location": "https://example.com/image.png"},
        )

    async def blocked_port(_request: web.Request) -> web.Response:
        return web.Response(
            status=302,
            headers={"Location": "http://127.0.0.1:1/image.png"},
        )

    app.router.add_get("/blocked-host", blocked_host)
    app.router.add_get("/blocked-port", blocked_port)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    redirect_base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(
        base_url=redirect_base,
        allowed_media_hosts={"127.0.0.1"},
        allowed_media_ports={int(urlsplit(redirect_base).port or 0)},
    )
    try:
        assert await api.download_to_temp(
            f"{redirect_base}/blocked-host",
            str(tmp_path),
        ) is None
        assert await api.download_to_temp(
            f"{redirect_base}/blocked-port",
            str(tmp_path),
        ) is None
    finally:
        await api.close()
        await runner.cleanup()


async def test_超过最大重定向次数返回None(tmp_path):
    """重定向循环必须有硬上限。"""
    redirects = 0
    app = web.Application()

    async def loop(request: web.Request) -> web.Response:
        nonlocal redirects
        redirects += 1
        return web.Response(status=302, headers={"Location": str(request.url)})

    app.router.add_get("/loop", loop)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(
        base_url=base,
        allowed_media_hosts={"127.0.0.1"},
        allowed_media_ports={int(urlsplit(base).port or 0)},
        max_redirects=2,
    )
    try:
        assert await api.download_to_temp(f"{base}/loop", str(tmp_path)) is None
        assert redirects == 3
    finally:
        await api.close()
        await runner.cleanup()


async def test_畸形ContentLength返回None(tmp_path):
    """畸形 Content-Length 不能绕过响应体大小检查。"""
    png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00IEND\xaeB`\x82"
    app = web.Application()

    async def malformed(_request: web.Request) -> web.Response:
        return web.Response(
            body=png,
            content_type="image/png",
            headers={"Content-Length": "not-a-number"},
        )

    app.router.add_get("/malformed", malformed)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(
        base_url=base,
        allowed_media_hosts={"127.0.0.1"},
        allowed_media_ports={int(urlsplit(base).port or 0)},
    )
    try:
        assert await api.download_to_temp(f"{base}/malformed", str(tmp_path)) is None
    finally:
        await api.close()
        await runner.cleanup()


async def test_分块响应超过大小上限返回None(tmp_path):
    """没有 Content-Length 的 chunked 响应也必须受实际读取上限保护。"""
    app = web.Application()

    async def oversized(_request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "image/png"})
        await response.prepare(_request)
        await response.write(b"\x89PNG\r\n\x1a\n" + b"x" * 2048)
        await response.write_eof()
        return response

    app.router.add_get("/oversized", oversized)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(
        base_url=base,
        allowed_media_hosts={"127.0.0.1"},
        allowed_media_ports={int(urlsplit(base).port or 0)},
        max_media_bytes=1024,
    )
    try:
        assert await api.download_to_temp(f"{base}/oversized", str(tmp_path)) is None
    finally:
        await api.close()
        await runner.cleanup()


async def test_图片ContentType与魔数不一致返回None(tmp_path):
    """不能只相信 Content-Type，伪造类型的响应必须拒绝。"""
    jpeg = b"\xff\xd8\xff" + b"\x00" * 32
    app = web.Application()

    async def mismatched(_request: web.Request) -> web.Response:
        return web.Response(body=jpeg, content_type="image/png")

    app.router.add_get("/mismatched", mismatched)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(
        base_url=base,
        allowed_media_hosts={"127.0.0.1"},
        allowed_media_ports={int(urlsplit(base).port or 0)},
    )
    try:
        assert await api.download_to_temp(f"{base}/mismatched", str(tmp_path)) is None
    finally:
        await api.close()
        await runner.cleanup()
