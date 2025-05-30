"""
services/acs_caller.py
----------------------
Thin wrapper that creates (or returns) the AcsCaller helper you already
have in `src.acs.acs_helper`.  Splitting it out lets `main.py`
initialise it once during startup and any router import it later.
"""

from __future__ import annotations
from typing import Optional
from utils.ml_logging import get_logger
from src.acs.acs_helper import AcsCaller as ACSCallService

logger = get_logger("services.acs_caller")

__all__ = [
    "ACSCallService",
]
