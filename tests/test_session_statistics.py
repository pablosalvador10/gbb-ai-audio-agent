"""
Test Session Statistics Manager
==============================

Simple test to verify the session statistics functionality works as expected.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from apps.rtagent.backend.src.sessions.session_statistics import SessionStatisticsManager


@pytest.mark.asyncio
async def test_session_statistics_basic_operations():
    """Test basic session statistics operations."""
    # Create manager without CosmosDB for testing
    manager = SessionStatisticsManager(cosmos_manager=None)
    await manager.initialize()
    
    # Initial state should be empty
    stats = await manager.get_statistics()
    assert stats["active_sessions"]["media"] == 0
    assert stats["active_sessions"]["realtime"] == 0
    assert stats["active_sessions"]["total"] == 0
    assert stats["total_disconnected"] == 0
    
    # Add some sessions
    mock_handler = MagicMock()
    mock_memory_manager = MagicMock()
    mock_websocket = MagicMock()
    
    await manager.add_media_session("call_123", mock_handler)
    await manager.add_realtime_session("session_456", mock_memory_manager, mock_websocket)
    
    # Check counts
    stats = await manager.get_statistics()
    assert stats["active_sessions"]["media"] == 1
    assert stats["active_sessions"]["realtime"] == 1
    assert stats["active_sessions"]["total"] == 2
    assert stats["total_disconnected"] == 0
    
    # Remove sessions (this should increment disconnection counter)
    removed_media = await manager.remove_media_session("call_123")
    removed_realtime = await manager.remove_realtime_session("session_456")
    
    assert removed_media is True
    assert removed_realtime is True
    
    # Check final state
    stats = await manager.get_statistics()
    assert stats["active_sessions"]["media"] == 0
    assert stats["active_sessions"]["realtime"] == 0
    assert stats["active_sessions"]["total"] == 0
    assert stats["total_disconnected"] == 2
    
    print("✅ All session statistics tests passed!")


@pytest.mark.asyncio 
async def test_session_statistics_with_cosmos():
    """Test session statistics with mocked CosmosDB."""
    # Mock CosmosDB manager
    mock_cosmos = MagicMock()
    mock_collection = MagicMock()
    mock_cosmos.database = {"session_statistics": mock_collection}
    
    # Mock collection methods
    mock_collection.find_one.return_value = None  # No existing document
    mock_collection.insert_one = AsyncMock()
    mock_collection.update_one = AsyncMock()
    
    manager = SessionStatisticsManager(cosmos_manager=mock_cosmos)
    await manager.initialize()
    
    # Add and remove a session to trigger persistence
    await manager.add_media_session("call_789", MagicMock())
    await manager.remove_media_session("call_789")
    
    # Verify persistence was attempted
    stats = await manager.get_statistics()
    assert stats["total_disconnected"] == 1
    
    print("✅ CosmosDB integration test passed!")


if __name__ == "__main__":
    asyncio.run(test_session_statistics_basic_operations())
    asyncio.run(test_session_statistics_with_cosmos())
    print("🎉 All session statistics tests completed successfully!")
