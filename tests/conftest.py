"""pytest 共享夹具：应用、模拟 K8s、可控时间。"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("CATCHUP_DB", ":memory:")

from app.kube import KubeClient, KubeConfig  # noqa: E402
from app.main import create_app  # noqa: E402
from app.service import CatchupService  # noqa: E402
from app.store import CatchupStore  # noqa: E402
from mock_k8s.server import create_mock_app, seed_state  # noqa: E402


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

    mock_transport = ASGITransport(app=mock_app)
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

    service = CatchupService(kube, store)
    app = create_app()
    app.state.kube = kube
    app.state.store = store
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
        "state": mock_state,
    }

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
