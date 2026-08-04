"""OneBotHttpApi.download_to_temp 测试。"""

import os

import pytest
from aiohttp import web

from onebot11.http_api import OneBotHttpApi


@pytest.fixture
async def file_server():
    """提供 GET /file.png 的静态文件服务。"""
    app = web.Application()

    async def _png(_request: web.Request) -> web.Response:
        return web.Response(body=b"PNGDATA", content_type="image/png")

    app.router.add_get("/file.png", _png)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    yield base
    await runner.cleanup()


async def test_下载成功(tmp_path, file_server):
    api = OneBotHttpApi(base_url="http://127.0.0.1:1")  # 下载不走 base_url
    path = await api.download_to_temp(f"{file_server}/file.png", str(tmp_path))
    assert path is not None
    assert os.path.basename(path).endswith(".png")
    with open(path, "rb") as f:
        assert f.read() == b"PNGDATA"
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
