"""Guard the code-location images against a broken gRPC / protobuf pairing.

``grpcio``, ``grpcio-health-checking`` and ``protobuf`` arrive through Dagster and are
resolved by the lock, not pinned. Generated protobuf code (``*_pb2.py``) refuses to load
under a protobuf runtime older than the one it was generated for, and Dagster's code server
imports that generated code at startup. A release that ships newer generated code without
raising its declared protobuf requirement therefore resolves cleanly and then stops every
code location with ``google.protobuf.runtime_version.VersionError`` — which took down all
six on 2026-07-07 (``grpcio-health-checking`` 1.82.0 against protobuf 6.x).

Nothing else in the suite imports these modules, so these tests are what turns that
failure into a red build instead of a failed deployment.
"""

import importlib

import pytest

# The modules `dagster code-server start` and `dagster api grpc-health-check` (the image
# CMD and HEALTHCHECK) load, plus the generated code they depend on.
STARTUP_MODULES = [
    "grpc_health.v1.health_pb2",
    "grpc_health.v1.health_pb2_grpc",
    "dagster._grpc.__generated__.dagster_api_pb2",
    "dagster._grpc.__generated__.dagster_api_pb2_grpc",
    "dagster._grpc.server",
    "dagster._grpc.client",
    "dagster._grpc.proxy_server",
]


@pytest.mark.unit
@pytest.mark.parametrize("module", STARTUP_MODULES)
def test_code_server_startup_module_imports(module):
    importlib.import_module(module)


@pytest.mark.unit
def test_health_servicer_builds_a_serving_response():
    """Exercise the generated health messages, not just their import."""
    from grpc_health.v1 import health, health_pb2

    servicer = health.HealthServicer()
    servicer.set("DagsterApi", health_pb2.HealthCheckResponse.SERVING)

    response = servicer.Check(health_pb2.HealthCheckRequest(service="DagsterApi"), None)

    assert response.status == health_pb2.HealthCheckResponse.SERVING
