import uuid
import json
from typing import Any, Dict, List, Optional

from rtagents.RTMedAgent.backend.agents.prompt_store.prompt_manager import PromptManager
from src.redis.manager import AzureRedisManager
from statistics import mean
from utils.ml_logging import get_logger

logger = get_logger()


class ConversationManager:
    def __init__(self, auth: bool = False, session_id: Optional[str] = None) -> None:
        self.pm: PromptManager = PromptManager()
        self.session_id: str = session_id or str(uuid.uuid4())[:8]
        self.hist: List[Dict[str, Any]] = []
        self.context: Dict[str, Any] = {"authenticated": auth}

    @staticmethod
    def build_redis_key(session_id: str) -> str:
        return f"session:{session_id}"

    def to_redis_dict(self) -> Dict[str, str]:
        return {
            "history": json.dumps(self.hist, ensure_ascii=False),
            "context": json.dumps(self.context, ensure_ascii=False),
        }

    @classmethod
    def from_redis(
        cls, session_id: str, redis_mgr: AzureRedisManager
    ) -> "ConversationManager":
        key = cls.build_redis_key(session_id)
        data = redis_mgr.get_session_data(key)
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

    def persist_to_redis(
        self, redis_mgr: AzureRedisManager, ttl_seconds: Optional[int] = None
    ) -> None:
        key = self.build_redis_key(self.session_id)
        redis_mgr.store_session_data(key, self.to_redis_dict())
        if ttl_seconds:
            redis_mgr.redis_client.expire(key, ttl_seconds)
        logger.info(
            f"Persisted session {self.session_id} – "
            f"history={len(self.hist)}, ctx_keys={list(self.context.keys())}"
        )

    def update_context(self, key: str, value: Any) -> None:
        self.context[key] = value

    def get_context(self, key: str, default: Any = None) -> Any:
        return self.context.get(key, default)

    _LAT_KEY = "latency_roundtrip"

    def _latency_bucket(self) -> Dict[str, List[Dict[str, float]]]:
        """
        Return (and lazily create) the latency dictionary that lives
        inside `context`.
        """
        return self.context.setdefault(self._LAT_KEY, {})

    def note_latency(self, stage: str, start_t: float, end_t: float) -> None:
        self._latency_bucket().setdefault(stage, []).append(
            {"start": start_t, "end": end_t, "dur": end_t - start_t}
        )

    def latency_summary(self) -> Dict[str, Dict[str, float]]:
        """
        Compute avg/min/max/count for each stage.  This is *on demand*
        (nothing persisted).
        """
        out: Dict[str, Dict[str, float]] = {}
        for stage, samples in self._latency_bucket().items():
            durations = [s["dur"] for s in samples]
            out[stage] = {
                "count": len(durations),
                "avg": mean(durations) if durations else 0.0,
                "min": min(durations) if durations else 0.0,
                "max": max(durations) if durations else 0.0,
            }
        return out

    def append_to_history(self, role: str, content: str) -> None:
        self.hist.append({"role": role, "content": content})

    def _build_full_name(self) -> str:
        return f"{self.get_context('first_name', 'Alice')} {self.get_context('last_name', 'Brown')}"

    def _generate_system_prompt(self) -> str:
        try:
            if self.get_context("authenticated", False):
                return self.pm.create_prompt_system_main(
                    patient_phone_number=self.get_context("phone_number", "5552971078"),
                    patient_name=self._build_full_name(),
                    patient_dob=self.get_context("patient_dob", "1987-04-12"),
                    patient_id=self.get_context("patient_id", "P54321"),
                )
            return self.pm.get_prompt("voice_agent_authentication.jinja")
        except Exception as exc:  # noqa: BLE001
            logger.error("Unable to generate system prompt", exc_info=True)
            raise exc

    def ensure_system_prompt(self) -> None:
        if not any(m["role"] == "system" for m in self.hist):
            self.hist.insert(
                0, {"role": "system", "content": self._generate_system_prompt()}
            )

    def upsert_system_prompt(self) -> None:
        new_prompt = self._generate_system_prompt()
        for msg in self.hist:
            if msg["role"] == "system":
                msg["content"] = new_prompt
                return
        self.hist.insert(0, {"role": "system", "content": new_prompt})
