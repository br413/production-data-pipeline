from pathlib import Path

import pytest

from src.pipeline.quality_contracts import (
    load_quality_contracts,
    parse_quality_contract_pin,
)


def test_parse_quality_contract_pin() -> None:
    pin = parse_quality_contract_pin("orders@1.0")
    assert pin.name == "orders"
    assert pin.version == "1.0"
    assert str(pin) == "orders@1.0"


def test_parse_quality_contract_pin_rejects_invalid() -> None:
    with pytest.raises(ValueError, match="name@version"):
        parse_quality_contract_pin("orders")


def test_load_quality_contracts_from_repo_config() -> None:
    pins, dqo_root = load_quality_contracts(Path("config/quality_contracts.yml"))
    assert set(pins) == {"orders", "customers"}
    assert pins["orders"].version == "1.0"
    assert dqo_root.name == "data-quality-observability"
