from __future__ import annotations

"""
rt_agent.py – YAML-driven agents with per-agent memory, model params, tools, and
now a configurable *prompt template path*.

New in this version
-------------------
• Reads `prompts.path` from the YAML (e.g. `voice_agent_authentication.jinja`).
• Uses that template when building / refreshing the system message.
• Summary log line now includes the prompt path.
• Additional validation + debug log for prompt loading.
"""

import logging
from pathlib import Path
from textwrap import shorten
from typing import Any, Dict, Final, Optional

import yaml
from fastapi import WebSocket

from rtagents.RTMedAgent.backend.orchestration.gpt_flow import process_gpt_response
from rtagents.RTMedAgent.backend.orchestration.conversation_state import (
    ConversationManager,
)
from rtagents.RTMedAgent.backend.agents.prompt_store.prompt_manager import PromptManager
from rtagents.RTMedAgent.backend.agents.tool_store import tools as tool_store
from utils.ml_logging import get_logger

logger = get_logger("rt_agent")


class RTAgent:
    """Base class for YAML-configured GPT agents with isolated memory & tools."""

    CONFIG_PATH: str | Path = "agent.yaml"

    def __init__(
        self,
        *,
        config_path: Optional[str | Path] = None,
        session_id: Optional[str] = None,
        template_dir: str = "templates",
    ) -> None:
        # ---------- Load & validate YAML ---------------------------------
        cfg_path = Path(config_path or self.CONFIG_PATH).expanduser().resolve()
        try:
            self._cfg = self._load_yaml(cfg_path)
        except FileNotFoundError:
            logger.exception("Config file not found: %s", cfg_path)
            raise
        except yaml.YAMLError:
            logger.exception("Invalid YAML in %s", cfg_path)
            raise
        self._validate_cfg()

        # ---------- Metadata --------------------------------------------
        self.name: str = self._cfg["agent"].get("name", "UnnamedAgent")
        self.creator: str = self._cfg["agent"].get("creator", "Unknown")
        self.organization: str = self._cfg["agent"].get("organization", "")
        self.description: str = self._cfg["agent"].get(
            "description", "No description provided."
        )

        # ---------- Model params ----------------------------------------
        m = self._cfg["model"]
        self.model_id: str = m["deployment_id"]
        self.temperature: float = float(m.get("temperature", 0.7))
        self.top_p: float = float(m.get("top_p", 1.0))
        self.max_tokens: int = int(m.get("max_tokens", 4096))

        # ---------- Prompt path -----------------------------------------
        self.prompt_path: str = self._cfg.get("prompts", {}).get(
            "path", "voice_agent_authentication.jinja"
        )
        logger.debug("Agent '%s' prompt template: %s", self.name, self.prompt_path)

        # ---------- Tools ------------------------------------------------
        self.tools: list[dict[str, Any]] = []
        for entry in self._cfg.get("tools", []):
            if isinstance(entry, str):
                if entry not in tool_store.TOOL_REGISTRY:
                    raise ValueError(
                        f"Unknown tool name '{entry}' in YAML for {self.name}"
                    )
                self.tools.append(tool_store.TOOL_REGISTRY[entry])
            elif isinstance(entry, dict):
                self.tools.append(entry)
            else:
                raise TypeError("Each tools entry must be a str or dict")

        # ---------- Managers --------------------------------------------
        self.pm: PromptManager = PromptManager(template_dir=template_dir)
        self.cm: ConversationManager = ConversationManager(session_id=session_id)
        self._log_loaded_summary()

    async def respond(
        self,
        cm: ConversationManager,
        user_prompt: str,
        ws: WebSocket,
        *,
        is_acs: bool = False,
    ) -> Any:
        """Stream a GPT response using this agent's settings & memory."""
        return await process_gpt_response(
            cm,
            user_prompt,
            ws,
            is_acs=is_acs,
            model_id=self.model_id,
            temperature=self.temperature,
            top_p=self.top_p,
            max_tokens=self.max_tokens,
            available_tools=self.tools,
        )

    @staticmethod
    def _load_yaml(path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _validate_cfg(self) -> None:
        required = [
            ("agent", ["name"]),
            ("model", ["deployment_id"]),
        ]
        for section, keys in required:
            if section not in self._cfg:
                raise ValueError(f"Missing '{section}' section in YAML config.")
            for key in keys:
                if key not in self._cfg[section]:
                    raise ValueError(f"Missing '{section}.{key}' in YAML config.")

        if "prompts" in self._cfg and "path" not in self._cfg["prompts"]:
            raise ValueError("If 'prompts' is declared, it must include 'path'")

    # ---------------- Logging helper ----------------------------------- #
    def _log_loaded_summary(self) -> None:
        desc_preview = shorten(self.description, width=60, placeholder="…")
        tool_names = [t["function"]["name"] for t in self.tools]
        logger.info(
            "Loaded agent '%s' | org='%s' | desc='%s' | model=%s | prompt=%s | "
            "tools=%s | session=%s",
            self.name,
            self.organization or "-",
            desc_preview,
            self.model_id,
            self.prompt_path,
            tool_names or "∅",
            self.cm.session_id,
        )
