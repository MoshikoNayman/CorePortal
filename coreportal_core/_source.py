"""Internal: load the monolithic ``coreportal.py`` as a module object.

The whole application currently lives in the top-level ``coreportal.py`` file,
which doubles as the deployable entrypoint. The ``coreportal_core`` package
exposes that code as a properly layered set of modules (config / db / quotes /
services / views / routes) so callers get a clean, documented API and import
graph without us having to physically cut the large HTML-template f-strings
apart (which would be risky).

Every submodule pulls the symbols it owns from the single module object loaded
here, so there is exactly one runtime instance of the app and its state.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MONOLITH = _REPO_ROOT / "coreportal.py"


def _load():
    # Reuse the already-imported module if the entrypoint imported it first.
    existing = sys.modules.get("coreportal")
    if existing is not None and getattr(existing, "__file__", None) == str(_MONOLITH):
        return existing

    spec = importlib.util.spec_from_file_location("coreportal", _MONOLITH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"Cannot load CorePortal source from {_MONOLITH}")
    module = importlib.util.module_from_spec(spec)
    # Register before exec so the module's own ``coreportal:app`` references and
    # any self-imports resolve to this same instance.
    sys.modules.setdefault("coreportal", module)
    spec.loader.exec_module(module)
    return module


source = _load()
