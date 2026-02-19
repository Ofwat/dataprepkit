import pytest
from sqlalchemy.exc import SQLAlchemyError

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
    validate_calls = []

    def fake_build(driver, host, database, port, encrypt=True, trust_certificate=False):
        build_calls.append((driver, host, database, port, encrypt, trust_certificate))
        return "DRIVER_CALL"

    monkeypatch.setattr(fabric, "_build_connection_string", fake_build)
    monkeypatch.setattr(fabric, "_get_driver", lambda *_: "driver")
    monkeypatch.setattr(fabric.credentials, "getToken", lambda _: b"token")
    monkeypatch.setattr(
        fabric,
        "_validate_engine_with_retry",
        lambda engine, **kwargs: validate_calls.append(kwargs),
    )

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
    assert validate_calls[-1] == {
        "max_retries": 3,
        "initial_backoff_seconds": 1.0,
        "backoff_multiplier": 2.0,
    }


def test_create_engine_honors_preferred_driver(monkeypatch):
    preferred = "Alternative ODBC"
    monkeypatch.setattr(fabric, "_build_connection_string", lambda *_: "STR")
    get_driver_calls = []
    monkeypatch.setattr(fabric, "_get_driver", lambda pref=None: (get_driver_calls.append(pref), preferred)[1])
    monkeypatch.setattr(fabric.credentials, "getToken", lambda _: b"tok")
    monkeypatch.setattr(fabric, "_validate_engine_with_retry", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(fabric.sa, "create_engine", lambda url, **kw: url)

    fabric.create_engine_for_fabric("host", "db", preferred_driver=preferred)
    assert get_driver_calls[-1] == preferred


def test_validate_engine_with_retry_retries_with_backoff(monkeypatch):
    class DummyConn:
        def execute(self, _statement):
            return None

    class DummyCtx:
        def __enter__(self):
            return DummyConn()

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyEngine:
        def __init__(self):
            self.attempts = 0

        def connect(self):
            self.attempts += 1
            if self.attempts < 3:
                raise SQLAlchemyError("temporary")
            return DummyCtx()

    sleeps = []
    monkeypatch.setattr(fabric.time, "sleep", lambda seconds: sleeps.append(seconds))

    engine = DummyEngine()
    fabric._validate_engine_with_retry(
        engine,
        max_retries=3,
        initial_backoff_seconds=0.5,
        backoff_multiplier=2.0,
    )

    assert sleeps == [0.5, 1.0]


def test_validate_engine_with_retry_raises_after_max_retries(monkeypatch):
    class DummyEngine:
        def connect(self):
            raise SQLAlchemyError("always fails")

    monkeypatch.setattr(fabric.time, "sleep", lambda _seconds: None)
    with pytest.raises(SQLAlchemyError):
        fabric._validate_engine_with_retry(
            DummyEngine(),
            max_retries=2,
            initial_backoff_seconds=0.1,
            backoff_multiplier=2.0,
        )
