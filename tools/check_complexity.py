"""Fail the quality gate when source exceeds B complexity or A maintainability."""

import logging
from pathlib import Path

from radon.complexity import cc_visit
from radon.metrics import mi_visit

logging.basicConfig(level=logging.INFO)
violations = []
for path in Path("src").rglob("*.py"):
    code = path.read_text()
    violations.extend(
        f"{path}:{block.lineno} {block.name}: complexity {block.complexity} > 10"
        for block in cc_visit(code)
        if block.complexity > 10
    )
    if mi_visit(code, multi=True) < 20:
        violations.append(f"{path}: maintainability below A")
for violation in violations:
    logging.error(violation)
if violations:
    raise SystemExit(1)
logging.info("Radon gate passed: complexity A/B, maintainability A")
