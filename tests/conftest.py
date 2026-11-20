"""pytest 共享夹具：应用、模拟 K8s、可控时间。"""
from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("CATCHUP_DB", ":memory:")

from app.jobindex import JobIndexManager  # noqa: E402
from app.kube import KubeClient, KubeConfig  # noqa: E402
from app.main import create_app  # noqa: E402
from app.service import CatchupService  # noqa: E402
from app.store import CatchupStore  # noqa: E402
from mock_k8s.server import create_mock_app, seed_state  # noqa: E402


class StreamingASGITransport(httpx.AsyncBaseTransport):
    """真正流式的 ASGI transport。

    httpx 自带的 ASGITransport 会等整个响应体结束后才返回，无法用于 WATCH；
    生产环境走真实 HTTP 连接本就不缓冲，这里仅为在测试里还原流式语义。
    """

    def __init__(self, app: Any):
        self.app = app

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for k, v in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "server": (request.url.host, request.url.port or 80),
            "client": ("test", 0),
            "root_path": "",
        }
        request_done = False
        response_started = asyncio.Event()
        response_done = asyncio.Event()
        status_box: dict[str, Any] = {}
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def receive() -> dict[str, Any]:
            nonlocal request_done
            if request_done:
                await response_done.wait()
                return {"type": "http.disconnect"}
            request_done = True
            body = await request.aread()
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_box["status"] = message["status"]
                status_box["headers"] = message.get("headers", [])
                response_started.set()
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                if body:
                    queue.put_nowait(body)
                if not message.get("more_body", False):
                    queue.put_nowait(None)
                    response_done.set()

        async def run_app() -> None:
            try:
                await self.app(scope, receive, send)
            except Exception as exc:  # noqa: BLE001 - 还原为传输层错误
                queue.put_nowait(exc)  # type: ignore[arg-type]
                if not response_started.is_set():
                    status_box["status"] = 500
                    status_box.setdefault("headers", [])
                    response_started.set()
            finally:
                # 确保等待方不会永久挂起
                if response_started.is_set():
                    queue.put_nowait(None)
                response_done.set()

        app_task = asyncio.create_task(run_app())
        started = asyncio.create_task(response_started.wait())
        done, _ = await asyncio.wait(
            {started, app_task}, return_when=asyncio.FIRST_COMPLETED
        )
        await started
        if not response_started.is_set():
            status_box["status"] = 500
            status_box["headers"] = []

        async def body_stream():
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        break
                    if isinstance(item, BaseException):
                        raise item
                    yield item
                    if response_done.is_set() and queue.empty():
                        break
            finally:
                if not app_task.done():
                    app_task.cancel()

        return httpx.Response(
            status_code=status_box["status"],
            headers=status_box.get("headers", []),
            stream=_QueueStream(body_stream, app_task),
        )


class _QueueStream(httpx.AsyncByteStream):
    def __init__(self, gen_factory, app_task: asyncio.Task):
        self._gen = gen_factory()
        self._app_task = app_task

    async def __aiter__(self):
        async for chunk in self._gen:
            yield chunk

    async def aclose(self) -> None:
        if not self._app_task.done():
            self._app_task.cancel()
        await self._gen.aclose()


@pytest.fixture
def mock_state():
    return seed_state()


@pytest_asyncio.fixture
async def harness(mock_state, request):
    """每个用例独立的 mock + 应用栈；可通过 marker 注入故障头。"""
    fault_marker = request.node.get_closest_marker("mock_fault")
    fault = fault_marker.args[0] if fault_marker else None
    slow = request.node.get_closest_marker("mock_slow")
    slow_delay = slow.args[0] if slow else 1.5

    mock_app = create_mock_app(mock_state)
    mock_app.state.slow_delay = slow_delay

    mock_transport = StreamingASGITransport(mock_app)
    mock_client = AsyncClient(
        base_url="http://kube.local", transport=mock_transport
    )

    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    store = CatchupStore(tmp.name)

    kube_config = KubeConfig(
        base_url="http://kube.local",
        timeout=10,
        create_timeout=0.5,
        verify_timeout=10,
        extra_headers={"X-Mock-Fault": fault} if fault else None,
    )
    kube = KubeClient(kube_config)
    # 用 ASGI transport 替换内部 httpx client
    await kube.aclose()
    kube._client = AsyncClient(
        base_url="http://kube.local",
        transport=mock_transport,
        headers={"X-Mock-Fault": fault} if fault else None,
        timeout=10,
    )

    indexes = JobIndexManager(kube, initial_wait=3.0)
    service = CatchupService(kube, store, indexes)
    app = create_app()
    app.state.kube = kube
    app.state.store = store
    app.state.indexes = indexes
    app.state.service = service
    store.recover_creating()

    api_transport = ASGITransport(app=app)
    api_client = AsyncClient(
        base_url="http://api.local", transport=api_transport
    )

    yield {
        "api": api_client,
        "mock_app": mock_app,
        "store": store,
        "kube": kube,
        "service": service,
        "indexes": indexes,
        "state": mock_state,
    }

    await indexes.close()
    await api_client.aclose()
    await mock_client.aclose()
    await kube.aclose()
    store.close()
    os.unlink(tmp.name)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def utc():
    return timezone.utc
