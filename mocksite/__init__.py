"""Deterministic local mock site used by pagewatch demos and tests."""

from .app import create_app
from .scenarios import ScenarioState

__all__ = ["create_app", "ScenarioState"]
