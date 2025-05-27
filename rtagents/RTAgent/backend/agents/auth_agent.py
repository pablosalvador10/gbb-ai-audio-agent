"""agents.py
This module defines the **BaseAgent** class and its concrete subclasses
**AuthAgent** and **TaskAgent**. These classes are responsible for handling
identity verification and business logic respectively in a voice agent application.
Each agent owns a **private Mem0** instance for durable semantic memory and
shares a **ConversationMemory** timeline (Redis→Cosmos) with its peers."""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Sequence
from rtagents.RTMedAgent.backend.agents.base import BaseAgent

from mem0 import Memory
from openai import AsyncAzureOpenAI

from memory_store.conversation import ConversationMemory


class AuthAgent(BaseAgent):
    def __init__(
        self,
        *,
        llm: AsyncAzureOpenAI,
        conv_mem: ConversationMemory,
        mem0: Memory,
        prompt: str,
    ) -> None:
        super().__init__(
            name="auth",
            system_prompt=prompt,
            llm=llm,
            conv_mem=conv_mem,
            mem0=mem0,
        )
        self._authenticated = False

    # ---------- overrides ------------------------------------------------- #

    def gate(self) -> bool:
        return not self._authenticated

    async def after_run(
        self, user_msg: str, assistant_reply: str
    ) -> None:  # noqa: D401
        # Simple heuristic; replace with tool output if you prefer.
        if "authenticated" in assistant_reply.lower() or "✅" in assistant_reply:
            self._authenticated = True
            # Store evidence in private memory for future sessions.
            await asyncio.to_thread(
                self._mem0.add,
                f"User identity verified as: {user_msg}",
                user_id=self._conv_mem.cid,
            )
