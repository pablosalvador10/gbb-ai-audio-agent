import uuid
import json
from typing import Any, Dict, List, Optional
import os
import datetime

from rtagents.RTAgent.backend.agents.prompt_store.prompt_manager import PromptManager
from src.redis.async_manager import AsyncAzureRedisManager as AzureRedisManager
from src.redis.key_manager import RedisKeyManager, DataType, Component
from statistics import mean
from utils.ml_logging import get_logger

logger = get_logger()

"""
Hierarchical Key Management for Conversation State
- rtvoice:<environment>:<DataType>:<session-id>:<Component>

Examples:
- rtvoice:prod:call:call-connection-id-1234:session (ACS call using call_connection_id)
- rtvoice:prod:conversation:session-id-5678:context (conversation using session_id)
- rtvoice:dev:worker:worker-abc123:affinity (worker using worker_id)
"""

class ConversationManager:
    def __init__(
            self, 
            auth: bool = False, 
            session_id: Optional[str] = None,
            redis_mgr: Optional[AzureRedisManager] = None,
            environment: Optional[str] = None
        ) -> None:
        self.pm: PromptManager = PromptManager()
        self.session_id: str = session_id or str(uuid.uuid4())[:8]
        self.hist: List[Dict[str, Any]] = []
        self.context: Dict[str, Any] = {"authenticated": auth}
        
        # Initialize Redis manager with environment
        self.environment = environment or os.getenv("ENVIRONMENT", "dev")
        if isinstance(redis_mgr, str):
            # If redis_mgr is a string (should not be), treat it as environment name
            self.redis_mgr = AzureRedisManager(environment=redis_mgr)
        else:
            self.redis_mgr = redis_mgr or AzureRedisManager(environment=self.environment)
        self.key_manager = self.redis_mgr.key_manager

    def _build_redis_key(self, component: Component = Component.SESSION) -> str:
        """Build hierarchical Redis key for conversation data."""
        return self.key_manager.build_key(
            data_type=DataType.CONVERSATION,
            identifier=self.session_id,
            component=component
        )

    @staticmethod
    def build_legacy_redis_key(session_id: str) -> str:
        """Legacy key format for migration purposes."""
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
        redis_mgr: Optional[AzureRedisManager] = None,
        environment: Optional[str] = None
    ) -> None:
        """Update context using direct Redis operations with hierarchical keys."""
        env = environment or os.getenv("ENVIRONMENT", "dev")
        mgr = redis_mgr or AzureRedisManager(environment=env)
        
        # Build key using the key manager
        context_key = mgr.key_manager.build_key(
            data_type=DataType.CONVERSATION,
            identifier=session_id,
            component=Component.CONTEXT
        )
        ttl = mgr.key_manager.get_ttl(DataType.CONVERSATION)
        
        # Try to get existing context
        existing_data = await mgr.redis_client.get(context_key)
        
        if existing_data:
            # Update existing context
            context = json.loads(existing_data)
            context.update(new_context)
            await mgr.redis_client.set(context_key, json.dumps(context, ensure_ascii=False), ex=ttl)
            logger.info(f"Updated context for session {session_id}: ctx keys={list(context.keys())}")
        else:
            # Try legacy key format for migration
            legacy_key = f"session:{session_id}"
            legacy_data = await mgr.get_session_data(legacy_key)
            
            if legacy_data:
                # Migrate from legacy format
                context = json.loads(legacy_data.get("context", "{}"))
                context.update(new_context)
                await mgr.redis_client.set(context_key, json.dumps(context, ensure_ascii=False), ex=ttl)
                logger.info(f"Migrated and updated context for session {session_id}: ctx keys={list(context.keys())}")
            else:
                # Create new context
                await mgr.redis_client.set(context_key, json.dumps(new_context, ensure_ascii=False), ex=ttl)
                logger.info(f"Created new context for session {session_id}: ctx keys={list(new_context.keys())}")

    @classmethod
    async def from_redis(
        cls, 
        session_id: str, 
        redis_mgr: AzureRedisManager,
        session_type: DataType = DataType.CONVERSATION,
        auto_create: bool = True,
        initial_context: Optional[Dict[str, Any]] = None
    ) -> "ConversationManager":
        """Load ConversationManager from Redis using hierarchical keys."""
        cm = cls(session_id=session_id, redis_mgr=redis_mgr)
        
        # Build keys using the instance method
        context_key = cm._build_redis_key(Component.CONTEXT)
        history_key = cm._build_redis_key(Component.HISTORY)
        
        # Try to load from new hierarchical format first
        if isinstance(redis_mgr, str):
            redis_mgr = AzureRedisManager(environment=redis_mgr)
        
        context_data = await redis_mgr.redis_client.get(context_key)
        history_data = await redis_mgr.redis_client.get(history_key)
        
        session_found = False
            
        if context_data or history_data:
            # New format found - parse JSON data
            if context_data:
                cm.context = json.loads(context_data)
            if history_data:
                cm.hist = json.loads(history_data)
            session_found = True
            logger.info(f"Restored session {session_id} (hierarchical): {len(cm.hist)} msgs, ctx keys={list(cm.context.keys())}")
            
        else:
            # Try legacy format for backward compatibility
            legacy_key = cls.build_legacy_redis_key(session_id)
            data = await redis_mgr.get_session_data(legacy_key)
            
            if data:
                if "history" in data:
                    cm.hist = json.loads(data["history"])
                if "context" in data:
                    cm.context = json.loads(data["context"])
                session_found = True
                logger.info(f"Restored session {session_id} (legacy): {len(cm.hist)} msgs, ctx keys={list(cm.context.keys())}")
                
                # Migrate to new hierarchical format
                await cm._migrate_to_new_format()
        
        # Handle new session creation
        if not session_found:
            if auto_create:
                # Initialize with session-type specific defaults
                if session_type == DataType.CALL:
                    default_context = {
                        "authenticated": False,
                        "session_type": session_type.value,
                        "call_state": "initializing",
                        "created_at": datetime.datetime.now().isoformat()
                    }
                elif session_type == DataType.CONVERSATION:
                    default_context = {
                        "authenticated": False,
                        "session_type": session_type.value,
                        "created_at": datetime.datetime.now().isoformat()
                    }
                else:
                    # Fallback for other data types
                    default_context = {
                        "authenticated": False,
                        "session_type": session_type.value,
                        "created_at": datetime.datetime.now().isoformat()
                    }
                
                
                # Merge with any provided initial context
                if initial_context:
                    default_context.update(initial_context)
                
                cm.context = default_context
                cm.hist = []
                
                # Persist the new session immediately
                await cm.persist_to_redis(session_type)
                
                logger.info(f"Created new {session_type} session {session_id}: ctx keys={list(cm.context.keys())}")
            else:
                logger.warning(f"Session {session_id} not found and auto_create=False")
        
        return cm

    async def _migrate_to_new_format(self) -> None:
        """Migrate legacy session data to hierarchical key structure using _build_redis_key."""
        try:
            # Build keys using instance method
            context_key = self._build_redis_key(Component.CONTEXT)
            history_key = self._build_redis_key(Component.HISTORY)
            ttl = self.key_manager.get_ttl(DataType.CONVERSATION)
            
            # Store in new hierarchical format using direct Redis operations
            await self.redis_mgr.redis_client.set(
                context_key, 
                json.dumps(self.context, ensure_ascii=False), 
                ex=ttl
            )
            await self.redis_mgr.redis_client.set(
                history_key, 
                json.dumps(self.hist, ensure_ascii=False), 
                ex=ttl
            )
            
            # Clean up legacy key
            legacy_key = self.build_legacy_redis_key(self.session_id)
            await self.redis_mgr.redis_client.delete(legacy_key)
            
            logger.info(f"Successfully migrated session {self.session_id} to hierarchical key format")
        except Exception as e:
            logger.error(f"Failed to migrate session {self.session_id}: {e}")
            # Continue using legacy format if migration fails

    async def load_from_redis(self, session_id: str) -> "ConversationManager":
        """Load session data from Redis into this instance using hierarchical keys."""
        # Build keys using the updated session_id
        self.session_id = session_id
        context_key = self._build_redis_key(Component.CONTEXT)
        history_key = self._build_redis_key(Component.HISTORY)
        
        # Try new hierarchical format first
        context_data = await self.redis_mgr.redis_client.get(context_key)
        history_data = await self.redis_mgr.redis_client.get(history_key)
        
        if context_data or history_data:
            # New format found
            if context_data:
                self.context = json.loads(context_data)
            if history_data:
                self.hist = json.loads(history_data)
        else:
            # Try legacy format
            legacy_key = self.build_legacy_redis_key(session_id)
            data = await self.redis_mgr.get_session_data(legacy_key)
            
            if data:
                if "history" in data:
                    self.hist = json.loads(data["history"])
                if "context" in data:
                    self.context = json.loads(data["context"])
                
                # Migrate to new format
                await self._migrate_to_new_format()
            else:
                logger.warning(f"Session {session_id} not found in Redis. Initializing empty session.")
                self.hist = []
                self.context = {"authenticated": False}
        return self
    
    async def persist_to_redis(
        self, 
        session_type: DataType = DataType.CONVERSATION,
        ttl_seconds: Optional[int] = None
    ) -> str:
        """Persist session data to Redis using hierarchical key structure via _build_redis_key."""
        # Build keys using instance method
        context_key = self._build_redis_key(Component.CONTEXT)
        history_key = self._build_redis_key(Component.HISTORY)
        
        # Get TTL from key manager
        ttl = self.key_manager.get_ttl(session_type, ttl_seconds)

        # Store context and history separately using direct Redis operations
        await self.redis_mgr.redis_client.set(
            context_key, 
            json.dumps(self.context, ensure_ascii=False), 
            ex=ttl
        )
        await self.redis_mgr.redis_client.set(
            history_key, 
            json.dumps(self.hist, ensure_ascii=False), 
            ex=ttl
        )
        
        logger.info(
            f"Persisted session {self.session_id} with hierarchical keys – "
            f"history={len(self.hist)}, ctx_keys={list(self.context.keys())}, ttl={ttl}s"
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
    
    # ACS Call Management Methods
    async def set_call_participants(self, participants: List[Dict[str, Any]]) -> None:
        """Store call participants data using hierarchical keys."""
        participants_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.PARTICIPANTS
        )
        ttl = self.key_manager.get_ttl(DataType.CALL)
        
        await self.redis_mgr.redis_client.set(
            participants_key,
            json.dumps({
                "participants": participants,
                "event_type": "participants_updated",
                "timestamp": json.dumps({"$date": {"$numberLong": str(int(datetime.datetime.now().timestamp() * 1000))}})
            }),
            ex=ttl
        )
        
    async def get_call_participants(self) -> List[Dict[str, Any]]:
        """Retrieve call participants data."""
        participants_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.PARTICIPANTS
        )
        participants_data = await self.redis_mgr.redis_client.get(participants_key)
        if participants_data:
            data = json.loads(participants_data)
            return data.get("participants", [])
        return []
    
    async def set_call_recording_state(self, recording_data: Dict[str, Any]) -> None:
        """Store call recording state data."""
        recording_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.RECORDING
        )
        ttl = self.key_manager.get_ttl(DataType.CALL)
        
        await self.redis_mgr.redis_client.set(
            recording_key,
            json.dumps(recording_data),
            ex=ttl
        )
    
    async def get_call_recording_state(self) -> Optional[Dict[str, Any]]:
        """Retrieve call recording state data."""
        recording_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.RECORDING
        )
        recording_data = await self.redis_mgr.redis_client.get(recording_key)
        return json.loads(recording_data) if recording_data else None
    
    async def set_media_streaming_status(self, status_data: Dict[str, Any]) -> None:
        """Store media streaming status."""
        status_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.MEDIA_STREAM
        )
        ttl = self.key_manager.get_ttl(DataType.CALL)
        
        await self.redis_mgr.redis_client.set(
            status_key,
            json.dumps(status_data),
            ex=ttl
        )
    
    async def clear_call_session(self) -> bool:
        """Clear all call-related data from Redis."""
        try:
            # Get all call-related keys
            pattern = self.key_manager.build_key(
                data_type=DataType.CALL,
                identifier=self.session_id,
                component="*"
            )
            # Use scan to find all matching keys
            keys_to_delete = []
            cursor = 0
            while True:
                cursor, keys = await self.redis_mgr.redis_client.scan(cursor, match=pattern)
                keys_to_delete.extend(keys)
                if cursor == 0:
                    break
            
            if keys_to_delete:
                await self.redis_mgr.redis_client.delete(*keys_to_delete)
                logger.info(f"Cleared {len(keys_to_delete)} call session keys for {self.session_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error clearing call session {self.session_id}: {e}")
            return False
        
    # === Media WebSocket Session Management ===
    async def get_media_stream_state(self) -> Dict[str, Any]:
        """Get media websocket stream session state (user_raw_id, connection status, etc.)"""
        session_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.MEDIA_STREAM
        )
        session_data = await self.redis_mgr.redis_client.get(session_key)
        if session_data:
            return json.loads(session_data)
        
        # Default state for new media sessions
        return {
            "user_raw_id": None,
            "connected": False,
            "greeted": False,
            "participant_count": 0,
            "last_activity": None
        }

    async def set_media_stream_state(self, session_data: Dict[str, Any]) -> None:
        """Store media websocket stream session state"""
        session_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.MEDIA_STREAM
        )
        ttl = self.key_manager.get_ttl(DataType.CALL)

        # Add timestamp
        session_data["last_updated"] = datetime.datetime.now().isoformat()

        await self.redis_mgr.redis_client.set(
            session_key,
            json.dumps(session_data),
            ex=ttl
        )

    async def update_user_raw_id(self, user_raw_id: str) -> None:
        """Update the primary user participant ID for media filtering"""
        session_state = await self.get_media_stream_state()
        session_state["user_raw_id"] = user_raw_id
        session_state["participant_identified"] = True
        await self.set_media_stream_state(session_state)

    async def get_user_raw_id(self) -> Optional[str]:
        """Get the primary user participant ID"""
        session_state = await self.get_media_stream_state()
        return session_state.get("user_raw_id")

    async def is_call_greeted(self) -> bool:
        """Check if initial greeting has been sent"""
        session_state = await self.get_media_stream_state()
        return session_state.get("greeted", False)

    async def mark_call_greeted(self) -> None:
        """Mark that initial greeting has been sent"""
        session_state = await self.get_media_stream_state()
        session_state["greeted"] = True
        await self.set_media_stream_state(session_state)

    async def get_media_streaming_status(self) -> Optional[Dict[str, Any]]:
        """Get current media streaming status"""
        status_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.MEDIA_STREAM
        )
        status_data = await self.redis_mgr.redis_client.get(status_key)
        return json.loads(status_data) if status_data else None

        return {
            "user_raw_id": None,
            "connected": False,
            "greeted": False,
            "participant_count": 0,
            "last_activity": None
        }

    async def set_media_stream_state(self, session_data: Dict[str, Any]) -> None:
        """Store media websocket session state"""
        session_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.MEDIA_STREAM
        )
        ttl = self.key_manager.get_ttl(DataType.CALL)
        
        # Add timestamp
        session_data["last_updated"] = datetime.datetime.now().isoformat()
        
        await self.redis_mgr.redis_client.set(
            session_key,
            json.dumps(session_data),
            ex=ttl
        )

    async def update_user_raw_id(self, user_raw_id: str) -> None:
        """Update the primary user participant ID for media filtering"""
        session_state = await self.get_media_stream_state()
        session_state["user_raw_id"] = user_raw_id
        session_state["participant_identified"] = True
        await self.set_media_stream_state(session_state)

    async def get_user_raw_id(self) -> Optional[str]:
        """Get the primary user participant ID"""
        session_state = await self.get_media_stream_state()
        return session_state.get("user_raw_id")

    async def is_call_greeted(self) -> bool:
        """Check if initial greeting has been sent"""
        session_state = await self.get_media_stream_state()
        return session_state.get("greeted", False)

    async def mark_call_greeted(self) -> None:
        """Mark that initial greeting has been sent"""
        session_state = await self.get_media_stream_state()
        session_state["greeted"] = True
        await self.set_media_stream_state(session_state)

    async def get_media_streaming_status(self) -> Optional[Dict[str, Any]]:
        """Get current media streaming status"""
        status_key = self.key_manager.build_key(
            data_type=DataType.CALL,
            identifier=self.session_id,
            component=Component.MEDIA_STREAM
        )
        status_data = await self.redis_mgr.redis_client.get(status_key)
        return json.loads(status_data) if status_data else None