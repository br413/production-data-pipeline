from pathlib import Path

import pytest

from src.pipeline.quality_contracts import (
    dqo_check_bash,
    dqo_run_command,
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


def test_dqo_run_command_includes_pin_and_data() -> None:
    pins, dqo_root = load_quality_contracts(Path("config/quality_contracts.yml"))
    command = dqo_run_command(
        pins["orders"],
        data_path=Path("data/samples/orders.csv"),
        dqo_project_root=dqo_root,
        references_dir=Path("data/samples"),
    )
    assert command[:5] == ["python", "-m", "src.dqo.cli", "run", "--contract"]
    assert "orders" in command
    assert "data/samples/orders.csv" in command


def test_dqo_check_bash_runs_all_pins() -> None:
    bash = dqo_check_bash(config_path=Path("config/quality_contracts.yml"))
    assert "src.dqo.cli" in bash
    assert "--contract orders" in bash
    assert "--contract customers" in bash
    assert " && " in bash
