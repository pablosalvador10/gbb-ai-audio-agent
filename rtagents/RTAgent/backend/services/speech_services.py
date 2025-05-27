"""
services/speech_services.py
---------------------------
Re-export thin wrappers around Azure Speech SDK that your code already
implements in `src.speech.*`.  Keeping them here isolates the rest of
the app from the direct SDK dependency.
"""

from src.speech.text_to_speech import SpeechSynthesizer
from src.speech.speech_to_text import SpeechCoreTranslator

__all__ = [
    "SpeechSynthesizer",
    "SpeechCoreTranslator",
]
