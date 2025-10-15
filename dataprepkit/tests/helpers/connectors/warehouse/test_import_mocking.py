import sys
import importlib

def test_get_fabric_warehouse_engine_uses_mock_credentials_when_notebookutils_missing(monkeypatch):
    """
    Test that when the 'notebookutils' module is missing, the fallback _MockCredentials
    is used and returns the expected fake token string.
    """
    sys.modules.pop("notebookutils", None)

    # Simulate ImportError by forcing 'notebookutils' to None
    monkeypatch.setitem(sys.modules, "notebookutils", None)

    # Reload the warehouse module to re-execute the try/except logic
    import dataprepkit.helpers.connectors.warehouse as warehouse_module
    importlib.reload(warehouse_module)

    # Validate that fallback credentials are being used
    creds = warehouse_module._default_credentials # pylint: disable=protected-access
    assert hasattr(creds, "getToken")
    assert callable(creds.getToken)

    # Ensure the mock token is returned
    token = creds.getToken("https://database.windows.net/")
    assert token == "FAKE_TOKEN_FOR_LOCAL_TESTING"
