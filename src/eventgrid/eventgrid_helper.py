import os
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential

class EventGridPublishHelper:
    def __init__(self, endpoint: str, credential=None):
        if not endpoint:
            raise ValueError("Event Grid endpoint must be provided.")
        if credential is None:
            credential = DefaultAzureCredential()
        self.client = EventGridPublisherClient(endpoint, credential)

    def publish_audio_event(self, audio_session_id: str, event_type: str, audio_metadata: dict, data_version: str = "1.0"):
        event = EventGridEvent(
            subject=f"audio/session/{audio_session_id}",
            event_type=event_type,
            data=audio_metadata,
            data_version=data_version
        )
        self.client.send([event])

    def test_connection(self) -> bool:
        try:
            # Attempt to send a test event to verify connectivity
            test_event = EventGridEvent(
                subject="test/connection",
                event_type="TestConnection",
                data={"message": "This is a test event"},
                data_version="1.0"
            )
            self.client.send([test_event])
            return True
        except Exception as e:
            print(f"Failed to connect to Event Grid: {e}")
            return False
        
    @classmethod
    def from_env(cls):
        endpoint = os.getenv("EVENTGRID_ENDPOINT")
        key = os.getenv("EVENTGRID_KEY")
        use_default_credential = not key

        if not endpoint:
            raise ValueError("EVENTGRID_ENDPOINT must be set in environment variables.")

        credential = DefaultAzureCredential() if use_default_credential else AzureKeyCredential(key)
        return cls(endpoint, credential)
