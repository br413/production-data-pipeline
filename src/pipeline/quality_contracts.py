"""Load quality contract pins that reference data-quality-observability."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class QualityContractPin:
    name: str
    version: str

    @property
    def registry_name(self) -> str:
        return self.name

    def cli_contract_arg(self) -> str:
        """Argument for `dqo.cli run --contract` (registry name)."""
        return self.name

    def __str__(self) -> str:
        return f"{self.name}@{self.version}"


def parse_quality_contract_pin(value: str) -> QualityContractPin:
    if "@" not in value:
        raise ValueError(f"quality contract pin must be name@version, got {value!r}")

    name, version = value.rsplit("@", 1)
    name = name.strip()
    version = version.strip()
    if not name or not version:
        raise ValueError(f"quality contract pin must be name@version, got {value!r}")

    return QualityContractPin(name=name, version=version)


def load_quality_contracts(
    path: Path = Path("config/quality_contracts.yml"),
) -> tuple[dict[str, QualityContractPin], Path]:
    if not path.is_file():
        raise FileNotFoundError(f"quality contract config not found: {path}")

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("quality contract config must be a mapping")

    pins_section = payload.get("pins")
    if not isinstance(pins_section, dict):
        raise ValueError("quality contract config missing 'pins' mapping")

    pins: dict[str, QualityContractPin] = {}
    for key, raw_pin in pins_section.items():
        if not isinstance(raw_pin, str):
            raise ValueError(f"pin {key!r} must be a string")
        pin = parse_quality_contract_pin(raw_pin)
        if pin.name != key:
            raise ValueError(f"pin key {key!r} does not match contract name {pin.name!r}")
        pins[key] = pin

    dqo_section = payload.get("dqo")
    if not isinstance(dqo_section, dict):
        raise ValueError("quality contract config missing 'dqo' mapping")

    project_root = dqo_section.get("project_root")
    if not isinstance(project_root, str) or not project_root.strip():
        raise ValueError("dqo.project_root must be a non-empty string")

    dqo_root = (path.parent.parent / project_root).resolve()
    return pins, dqo_root


def dqo_run_command(
    pin: QualityContractPin,
    *,
    data_path: Path,
    dqo_project_root: Path,
    references_dir: Path | None = None,
) -> list[str]:
    command = [
        "python",
        "-m",
        "src.dqo.cli",
        "run",
        "--contract",
        pin.cli_contract_arg(),
        "--data",
        str(data_path),
    ]
    if references_dir is not None:
        command.extend(["--references", str(references_dir)])

    return command
