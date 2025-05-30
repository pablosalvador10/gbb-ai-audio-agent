import uuid
import json
from typing import Any, Dict, List, Optional

from rtagents.RTAgent.backend.agents.prompt_store.prompt_manager import PromptManager
from src.redis.async_manager import AsyncAzureRedisManager as AzureRedisManager
from statistics import mean
from utils.ml_logging import get_logger

logger = get_logger()


class ConversationManager:
    def __init__(
            self, 
            auth: bool = False, 
            session_id: Optional[str] = None,
            redis_mgr: Optional[AzureRedisManager] = None
        ) -> None:
        self.pm: PromptManager = PromptManager()
        self.session_id: str = session_id or str(uuid.uuid4())[:8]
        self.hist: List[Dict[str, Any]] = []
        self.context: Dict[str, Any] = {"authenticated": auth}
        self.redis_mgr = redis_mgr

    @staticmethod
    def build_redis_key(session_id: str) -> str:
        return f"session:{session_id}"
    
    def to_redis_dict(self) -> Dict[str, str]:
        return {
            "history": json.dumps(self.hist, ensure_ascii=False),
            "context": json.dumps(self.context, ensure_ascii=False),
        }

    @classmethod
    async def set_redis_context(
        cls, 
        session_id: str, 
        new_context: Dict[str, Any],
        redis_mgr: Optional[AzureRedisManager] = None
    ) -> None:
        key = cls.build_redis_key(session_id)
        data = await (redis_mgr or AzureRedisManager()).get_session_data(key)
        if data:
            context = json.loads(data.get("context", "{}"))
            context.update(new_context)
            data["context"] = json.dumps(context, ensure_ascii=False)
            await (redis_mgr or AzureRedisManager()).store_session_data(key, data)
            logger.info(
                f"Updated context for session {session_id}: "
                f"ctx keys={list(context.keys())}"
            )
        else:
            logger.warning(f"Session {session_id} not found. Cannot update context.")

    @classmethod
    async def from_redis(
        cls, session_id: str, redis_mgr: AzureRedisManager
    ) -> "ConversationManager":
        key = cls.build_redis_key(session_id)
        data = await redis_mgr.get_session_data(key)
        cm = cls(session_id=session_id)
        if "history" in data:
            cm.hist = json.loads(data["history"])
        if "context" in data:
            cm.context = json.loads(data["context"])
        logger.info(
            f"Restored session {session_id}: "
            f"{len(cm.hist)} msgs, ctx keys={list(cm.context.keys())}"
        )
        return cm

    # Remove @classmethod decorator
    async def load_from_redis(self, session_id: str) -> None:
        """Load session data from Redis into this instance."""
        key = self.build_redis_key(session_id)
        data = await self.redis_mgr.get_session_data(key)  # ✅ Now self.redis_mgr works!
        
        if data:
            if "history" in data:
                self.hist = json.loads(data["history"])
            if "context" in data:
                self.context = json.loads(data["context"])
        else:
            logger.warning(f"Session {session_id} not found in Redis. Initializing empty session.")
            self.hist = []
            self.context = {"authenticated": False}
        return self
    

    async def persist_to_redis(
        self, ttl_seconds: Optional[int] = None
    ) -> None:
        key = self.build_redis_key(self.session_id)
        await self.redis_mgr.store_session_data(key, self.to_redis_dict())
        if ttl_seconds:
            await self.redis_mgr.redis_client.expire(key, ttl_seconds)
        logger.info(
            f"Persisted session {self.session_id} – "
            f"history={len(self.hist)}, ctx_keys={list(self.context.keys())}"
        )
        return self.session_id

    async def update_context(self, key: str, value: Any) -> None:
        self.context[key] = value

    async def set_context(self, new_context: Dict[str, Any]) -> None:
        """Set multiple context keys at once."""
        self.context.update(new_context)

    async def get_context(self, key: str, default: Any = None) -> Any:
        return self.context.get(key, default)

    async def append_to_history(self, role: str, content: str) -> None:
        self.hist.append({"role": role, "content": content})

    async def ensure_system_prompt(self) -> None:
        if not any(m["role"] == "system" for m in self.hist):
            self.hist.insert(
                0, {"role": "system", "content": await self._generate_system_prompt()}
            )

    async def upsert_system_prompt(self) -> None:
        new_prompt = await self._generate_system_prompt()
        for msg in self.hist:
            if msg["role"] == "system":
                msg["content"] = new_prompt
                return
        self.hist.insert(0, {"role": "system", "content": new_prompt})

    async def _generate_system_prompt(self) -> str:
        try:
            if await self.get_context("authenticated", False):
                return await self.pm.create_prompt_system_main(
                    patient_phone_number=await self.get_context("phone_number", "5552971078"),
                    patient_name=await self._build_full_name(),
                    patient_dob=await self.get_context("patient_dob", "1987-04-12"),
                    patient_id=await self.get_context("patient_id", "P54321"),
                )
            return await self.pm.get_prompt("voice_agent_authentication.jinja")
        except Exception as exc:  # noqa: BLE001
            logger.error("Unable to generate system prompt", exc_info=True)
            raise exc

    async def _build_full_name(self) -> str:
        return f"{await self.get_context('first_name', 'Alice')} {await self.get_context('last_name', 'Brown')}"
