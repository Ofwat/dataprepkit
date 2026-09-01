import pytest

from dataprepkit.helpers.schema import create_validation_run_summary_view


class _FakeConnection:
    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        self.statements.append(str(statement))


class _FakeBegin:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    class _Dialect:
        name = "mssql"

    dialect = _Dialect()

    def __init__(self):
        self.connection = _FakeConnection()

    def begin(self):
        return _FakeBegin(self.connection)


def test_create_validation_run_summary_view_uses_defaults(monkeypatch):
    engine = _FakeEngine()
    schemas = []
    monkeypatch.setattr(
        "dataprepkit.helpers.schema.ensure_schema_exists",
        lambda _engine, schema: schemas.append(schema),
    )

    create_validation_run_summary_view(engine)

    sql = engine.connection.statements[0]
    assert "CREATE OR ALTER VIEW [hello].[validation_run_summary]" in sql
    assert "FROM [hello].[validation_event]" in sql
    assert "GROUP BY run_id, organisation_cd" in sql
    assert "DATEADD(\n                month,\n                -1," in sql
    assert schemas == ["hello"]


def test_create_validation_run_summary_view_quotes_custom_names(monkeypatch):
    engine = _FakeEngine()
    monkeypatch.setattr(
        "dataprepkit.helpers.schema.ensure_schema_exists",
        lambda *_: None,
    )

    create_validation_run_summary_view(
        engine,
        source_schema="source]schema",
        source_table="events",
        view_schema="reporting",
        view_name="run_summary",
        lookback_months=3,
    )

    sql = engine.connection.statements[0]
    assert "[source]]schema].[events]" in sql
    assert "[reporting].[run_summary]" in sql
    assert "-3," in sql


def test_create_validation_run_summary_view_rejects_invalid_arguments():
    engine = _FakeEngine()

    with pytest.raises(ValueError, match="lookback_months"):
        create_validation_run_summary_view(engine, lookback_months=0)
