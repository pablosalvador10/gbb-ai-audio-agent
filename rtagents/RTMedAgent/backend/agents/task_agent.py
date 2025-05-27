import asyncio
from typing import Any, Dict, Sequence

from mem0 import Memory
from openai import AsyncAzureOpenAI

from memory_store.conversation import ConversationMemory
from agents.base import BaseAgent


class TaskAgent(BaseAgent):
    """Handles every user turn *after* authentication is complete."""

    def __init__(
        self,
        *,
        llm: AsyncAzureOpenAI,
        conv_mem: ConversationMemory,
        mem0: Memory,
        prompt: str,
        tools: Sequence[Dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            name="task",
            system_prompt=prompt,
            llm=llm,
            conv_mem=conv_mem,
            mem0=mem0,
            tools=tools,
        )

    # Always eligible once the AuthAgent has retired.
    def gate(self) -> bool:  # noqa: D401
        return True

    async def after_run(
        self, user_msg: str, assistant_reply: str
    ) -> None:  # noqa: D401
        """
        Persist *important* facts from the user's message into long-term memory.

        Very simple heuristic: if the user utterance contains more than three
        tokens (i.e., not just “yes”, “okay”, etc.), store it in Mem0 so the
        agent can recall it in future sessions.
        """
        if len(user_msg.split()) > 3:
            await asyncio.to_thread(
                self._mem0.add,
                user_msg,
                user_id=self._conv_mem.cid,
                metadata={"origin": "usr_turn"},
            )
