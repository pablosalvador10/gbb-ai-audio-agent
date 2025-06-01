import asyncio
from azure.cognitiveservices.speech.audio import (
    PullAudioInputStreamCallback,
    AudioStreamFormat,
    PullAudioInputStream
)

class InterruptiblePullStream(PullAudioInputStreamCallback):
    def __init__(self, sample_rate=16000, bits_per_sample=16, channels=1):
        super().__init__()
        self.buffer = asyncio.Queue()
        self.paused = asyncio.Event()
        self.paused.set()  # Start unpaused
        self.sample_rate = sample_rate
        self.bits_per_sample = bits_per_sample
        self.channels = channels

    def read(self, data_buffer: memoryview) -> int:
        if not self.paused.is_set():
            return 0  # Pause: STT receives silence

        try:
            audio_chunk = self.buffer.get_nowait()
            length = len(audio_chunk)
            data_buffer[:length] = audio_chunk
            return length
        except asyncio.QueueEmpty:
            return 0  # Nothing to pull → simulate silence

    def write_audio(self, audio_bytes: bytes):
        self.buffer.put_nowait(audio_bytes)

    def pause_streaming(self):
        self.paused.clear()

    def resume_streaming(self):
        self.paused.set()

    def get_stream_format(self):
        return AudioStreamFormat(
            samples_per_second=self.sample_rate,
            bits_per_sample=self.bits_per_sample,
            channels=self.channels
        )

    def create_audio_config(self):
        return PullAudioInputStream(self, self.get_stream_format())