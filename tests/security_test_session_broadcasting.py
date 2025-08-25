#!/usr/bin/env python3
"""
Security test for session-aware broadcasting.

This script tests that messages are properly isolated between sessions
and that no cross-user data leakage occurs.
"""

import asyncio
import json
import websockets
import uuid
from typing import Dict, List

class SessionTest:
    def __init__(self, base_url: str = "ws://localhost:8000"):
        self.base_url = base_url
        self.sessions: Dict[str, Dict] = {}
        
    async def create_session(self, session_id: str) -> Dict:
        """Create a conversation session and dashboard client for testing."""
        conversation_uri = f"{self.base_url}/api/v1/realtime/conversation"
        dashboard_uri = f"{self.base_url}/api/v1/realtime/dashboard/relay"
        
        session_data = {
            "session_id": session_id,
            "conversation_ws": None,
            "dashboard_ws": None,
            "received_messages": []
        }
        
        try:
            # Create conversation WebSocket
            session_data["conversation_ws"] = await websockets.connect(
                conversation_uri,
                extra_headers={"x-ms-call-connection-id": session_id}
            )
            
            # Create dashboard WebSocket (session-specific)
            session_data["dashboard_ws"] = await websockets.connect(
                f"{dashboard_uri}?session_id={session_id}"
            )
            
            print(f"✅ Created session {session_id}")
            self.sessions[session_id] = session_data
            return session_data
            
        except Exception as e:
            print(f"❌ Failed to create session {session_id}: {e}")
            return None
    
    async def listen_to_dashboard(self, session_id: str, duration: int = 10):
        """Listen to dashboard messages for a specific session."""
        session = self.sessions.get(session_id)
        if not session or not session["dashboard_ws"]:
            print(f"❌ No dashboard connection for session {session_id}")
            return
            
        try:
            async with asyncio.timeout(duration):
                async for message in session["dashboard_ws"]:
                    data = json.loads(message)
                    session["received_messages"].append(data)
                    print(f"📨 Session {session_id} received: {data.get('message', '')[:50]}...")
                    
        except asyncio.TimeoutError:
            print(f"⏰ Dashboard listening timeout for session {session_id}")
        except Exception as e:
            print(f"❌ Dashboard listening error for session {session_id}: {e}")
    
    async def send_message(self, session_id: str, message: str):
        """Send a message through the conversation WebSocket."""
        session = self.sessions.get(session_id)
        if not session or not session["conversation_ws"]:
            print(f"❌ No conversation connection for session {session_id}")
            return
            
        try:
            await session["conversation_ws"].send(json.dumps({
                "type": "audio_data",
                "audio": "",  # Empty for text-only test
                "text": message
            }))
            print(f"📤 Session {session_id} sent: {message}")
            
        except Exception as e:
            print(f"❌ Failed to send message for session {session_id}: {e}")
    
    async def cleanup_session(self, session_id: str):
        """Clean up a session's WebSocket connections."""
        session = self.sessions.get(session_id)
        if not session:
            return
            
        try:
            if session["conversation_ws"]:
                await session["conversation_ws"].close()
            if session["dashboard_ws"]:
                await session["dashboard_ws"].close()
            print(f"🧹 Cleaned up session {session_id}")
            
        except Exception as e:
            print(f"❌ Cleanup error for session {session_id}: {e}")
        
        del self.sessions[session_id]
    
    def analyze_security(self) -> bool:
        """Analyze if there was any cross-session message leakage."""
        print("\n" + "="*60)
        print("🔒 SECURITY ANALYSIS")
        print("="*60)
        
        security_passed = True
        
        for session_id, session in self.sessions.items():
            messages = session["received_messages"]
            print(f"\n📊 Session {session_id}: {len(messages)} messages received")
            
            for msg in messages:
                # Check if this message should belong to this session
                msg_session = msg.get("session_id") or msg.get("call_connection_id")
                if msg_session and msg_session != session_id:
                    print(f"🚨 SECURITY BREACH: Session {session_id} received message from session {msg_session}")
                    print(f"   Message: {msg}")
                    security_passed = False
        
        if security_passed:
            print("\n✅ SECURITY TEST PASSED: No cross-session message leakage detected")
        else:
            print("\n❌ SECURITY TEST FAILED: Cross-session message leakage detected")
            
        return security_passed

async def run_security_test():
    """Run the complete security test."""
    print("🧪 Starting Session Security Test")
    print("="*60)
    
    test = SessionTest()
    
    # Create multiple test sessions
    session_ids = [str(uuid.uuid4())[:8] for _ in range(3)]
    
    try:
        # Create sessions
        for session_id in session_ids:
            await test.create_session(session_id)
        
        # Start dashboard listeners for all sessions
        listen_tasks = [
            asyncio.create_task(test.listen_to_dashboard(session_id, 15))
            for session_id in session_ids
        ]
        
        # Wait a bit for connections to stabilize
        await asyncio.sleep(2)
        
        # Send different messages from each session
        for i, session_id in enumerate(session_ids):
            message = f"Secret message from session {i+1}: {uuid.uuid4().hex[:8]}"
            await test.send_message(session_id, message)
            await asyncio.sleep(1)  # Space out messages
        
        # Wait for all listeners to complete
        await asyncio.gather(*listen_tasks, return_exceptions=True)
        
        # Analyze security
        security_passed = test.analyze_security()
        
        return security_passed
        
    finally:
        # Cleanup
        for session_id in list(test.sessions.keys()):
            await test.cleanup_session(session_id)

if __name__ == "__main__":
    print("Session Broadcasting Security Test")
    print("This test verifies that messages are properly isolated between sessions.")
    print()
    
    try:
        result = asyncio.run(run_security_test())
        exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n💥 Test failed with error: {e}")
        exit(1)
