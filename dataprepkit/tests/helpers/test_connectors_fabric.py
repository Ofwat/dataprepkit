import pytest

from dataprepkit.helpers.connectors import fabric


@pytest.mark.parametrize(
    "endpoint, expected_host, expected_port",
    [
        ("myfabric.database.fabric.microsoft.com", "myfabric.database.fabric.microsoft.com", 1433),
        ("myfabric.database.fabric.microsoft.com,1188", "myfabric.database.fabric.microsoft.com", 1188),
    ],
)
def test_create_engine_builds_connection_string(endpoint, expected_host, expected_port, monkeypatch):
    build_calls = []

    def fake_build(driver, host, database, port, encrypt=True, trust_certificate=False):
        build_calls.append((driver, host, database, port, encrypt, trust_certificate))
        return "DRIVER_CALL"

    monkeypatch.setattr(fabric, "_build_connection_string", fake_build)
    monkeypatch.setattr(fabric, "_get_driver", lambda *_: "driver")
    monkeypatch.setattr(fabric.credentials, "getToken", lambda _: b"token")

    def fake_create_engine(url, connect_args, pool_pre_ping=True, pool_recycle=3600):
        return {"url": url, "connect_args": connect_args}

    monkeypatch.setattr(fabric.sa, "create_engine", fake_create_engine)

    result = fabric.create_engine_for_fabric(endpoint, "target_db")
    assert isinstance(result, dict)
    assert build_calls, "Connection builder should be invoked"
    _, host, database, port, encrypt, trust = build_calls[-1]
    assert host == expected_host
    assert port == expected_port
    assert database == "target_db"
    assert encrypt is True and trust is False


def test_create_engine_honors_preferred_driver(monkeypatch):
    preferred = "Alternative ODBC"
    monkeypatch.setattr(fabric, "_build_connection_string", lambda *_: "STR")
    get_driver_calls = []
    monkeypatch.setattr(fabric, "_get_driver", lambda pref=None: (get_driver_calls.append(pref), preferred)[1])
    monkeypatch.setattr(fabric.credentials, "getToken", lambda _: b"tok")
    monkeypatch.setattr(fabric.sa, "create_engine", lambda url, **kw: url)

    fabric.create_engine_for_fabric("host", "db", preferred_driver=preferred)
    assert get_driver_calls[-1] == preferred
