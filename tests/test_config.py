import pytest
from pydantic import ValidationError

from payments_service.core.config import Settings


def test_configuration_does_not_expose_secrets(settings: Settings) -> None:
    rendered = repr(settings)
    assert "test-api-key" not in rendered
    assert "test:test" not in rendered


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("api_key", "   "),
        ("database_url", "postgresql://user:password@localhost/db"),
        ("rabbitmq_url", "https://localhost"),
    ],
)
def test_configuration_rejects_invalid_values(settings: Settings, field: str, value: str) -> None:
    values = settings.model_dump()
    values[field] = value
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)
