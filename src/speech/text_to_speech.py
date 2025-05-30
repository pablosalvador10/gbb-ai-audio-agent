import os
import html
import re

import azure.cognitiveservices.speech as speechsdk
from dotenv import load_dotenv

from utils.ml_logging import get_logger

# Load environment variables from a .env file if present
load_dotenv()

# Initialize logger
logger = get_logger()
logger.setLevel("DEBUG")


class SpeechSynthesizer:
    def __init__(
        self,
        key: str = None,
        region: str = None,
        language: str = "en-US",
        voice: str = "en-US-JennyMultilingualNeural",
    ):
        # Retrieve Azure Speech credentials from parameters or environment variables
        self.key = key or os.getenv("AZURE_SPEECH_KEY")
        self.region = region or os.getenv("AZURE_SPEECH_REGION")
        self.language = language
        self.voice = voice        # Initialize the speech synthesizer for speaker playback
        self.speaker_synthesizer = self._create_speaker_synthesizer()

    def _sanitize_text_for_ssml(self, text: str) -> str:
        """
        Sanitize text for safe use in SSML by escaping XML special characters
        and removing potentially problematic characters.
        """
        if not text:
            return ""
            
        # First, escape XML special characters
        # Use html.escape() which handles &, <, > and optionally quotes
        sanitized = html.escape(text, quote=False)
        
        # Replace smart quotes and apostrophes with regular ones FIRST
        # This prevents "let s" type issues from contraction handling
        sanitized = sanitized.replace(''', "'").replace(''', "'")
        sanitized = sanitized.replace('"', '"').replace('"', '"')
        
        # Fix common contraction issues that cause SSML problems
        # These patterns can cause speech synthesis issues
        sanitized = re.sub(r"(\w)\s+(')\s*(\w)", r"\1'\3", sanitized)  # "let s" -> "let's"
        sanitized = re.sub(r"(\w)\s+(')([sdtmvre])\b", r"\1'\3", sanitized)  # "don t" -> "don't"
        
        # Remove any control characters that might cause SSML parsing issues
        # Keep only printable characters, spaces, and common punctuation
        # Allow letters, digits, spaces, and common punctuation
        sanitized = re.sub(r'[^\w\s\.\,\!\?\;\:\-\'\"\(\)\[\]\/\\]', ' ', sanitized)
        
        # Remove any remaining problematic characters that could cause SSML errors
        # Specifically target characters that have caused issues in error logs
        problematic_chars = ['`', '~', '@', '#', '$', '%', '^', '*', '+', '=', '|', '{', '}']
        for char in problematic_chars:
            sanitized = sanitized.replace(char, ' ')
        
        # Collapse multiple spaces into single spaces
        sanitized = ' '.join(sanitized.split())
        
        return sanitized.strip()

    def _create_speech_config(self):
        """
        Helper method to create and configure the SpeechConfig object.
        """
        speech_config = speechsdk.SpeechConfig(
            subscription=self.key, region=self.region
        )
        speech_config.speech_synthesis_language = self.language
        speech_config.speech_synthesis_voice_name = self.voice
        # Set the output format to 24kHz 16-bit mono PCM WAV
        speech_config.set_speech_synthesis_output_format(
            speechsdk.SpeechSynthesisOutputFormat.Riff24Khz16BitMonoPcm
        )
        return speech_config

    def _create_speaker_synthesizer(self):
        """
        Create a SpeechSynthesizer instance for playing audio through the server's default speaker.
        """
        speech_config = self._create_speech_config()
        audio_config = speechsdk.audio.AudioOutputConfig(use_default_speaker=True)
        return speechsdk.SpeechSynthesizer(
            speech_config=speech_config, audio_config=audio_config
        )

    def start_speaking_text(self, text: str) -> None:
        """
        Asynchronously play synthesized speech through the server's default speaker.
        """
        try:
            logger.info(f"[🔊] Speaking text (server speaker): {text[:30]}...")
            self.speaker_synthesizer.start_speaking_text_async(text)
        except Exception as e:
            logger.error(f"[❗] Error starting speech synthesis: {e}")

    def stop_speaking(self) -> None:
        """
        Stop any ongoing speech synthesis playback on the server's speaker.
        """
        try:
            logger.info("[🛑] Stopping speech synthesis on server speaker...")
            self.speaker_synthesizer.stop_speaking_async()
        except Exception as e:
            logger.error(f"[❗] Error stopping speech synthesis: {e}")

    def synthesize_speech(self, text: str) -> bytes:
        """
        Synthesizes text to speech in memory (returning WAV bytes).
        Does NOT play audio on server speakers.
        """
        try:
            speech_config = speechsdk.SpeechConfig(
                subscription=self.key, region=self.region
            )
            speech_config.speech_synthesis_language = self.language
            speech_config.speech_synthesis_voice_name = self.voice
            speech_config.set_speech_synthesis_output_format(
                speechsdk.SpeechSynthesisOutputFormat.Riff48Khz16BitMonoPcm
            )

            synthesizer = speechsdk.SpeechSynthesizer(
                speech_config=speech_config, audio_config=None
            )

            result = synthesizer.speak_text_async(text).get()

            if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
                audio_data_stream = speechsdk.AudioDataStream(result)
                wav_bytes = audio_data_stream.read_data()  # ✅ USE read_data()
                return bytes(
                    wav_bytes
                )  # ✅ Ensure it's converted from bytearray to bytes            else:
                logger.error(f"Speech synthesis failed: {result.reason}")
                return b""
        except Exception as e:
            logger.error(f"Error synthesizing speech: {e}")
            return b""

    def synthesize_to_base64_frames(
        self, text: str, sample_rate: int = 16000
    ) -> bytes:
        """
        Synthesize `text` via Azure TTS into raw 16-bit PCM mono at either 16 kHz or 24 kHz.
        Returns raw PCM bytes that can be sent to ACS.

        - sample_rate: 16000 or 24000
        - Returns: Raw PCM bytes
        """
        if not text or not text.strip():
            logger.warning("Empty or whitespace-only text provided for TTS")
            return b""
              # Sanitize and escape text for SSML
        try:
            # Remove any characters that might cause SSML parsing issues
            sanitized_text = self._sanitize_text_for_ssml(text.strip())
            if not sanitized_text:
                logger.warning("Text became empty after sanitization")
                return b""
                
        except Exception as e:
            logger.error(f"Error sanitizing text for SSML: {e}")
            return b""

        # Select SDK output format and packet size
        fmt_map = {
            16000: speechsdk.SpeechSynthesisOutputFormat.Raw16Khz16BitMonoPcm,
            24000: speechsdk.SpeechSynthesisOutputFormat.Raw24Khz16BitMonoPcm,
        }
        sdk_format = fmt_map.get(sample_rate)
        if not sdk_format:
            raise ValueError("sample_rate must be 16000 or 24000")

        # 1) Configure Speech SDK using class attributes
        speech_config = speechsdk.SpeechConfig(
            subscription=self.key, region=self.region
        )
        speech_config.speech_synthesis_language = self.language
        speech_config.speech_synthesis_voice_name = self.voice
        speech_config.set_speech_synthesis_output_format(sdk_format)        # 2) Synthesize to memory (audio_config=None)
        synth = speechsdk.SpeechSynthesizer(
            speech_config=speech_config, audio_config=None
        )        # 3) Build an SSML envelope using the configurable template with fallback
        ssml = self._create_ssml_template(
            text=sanitized_text,
            language=speech_config.speech_synthesis_language,
            voice=speech_config.speech_synthesis_voice_name
        )        # 4) Synthesize with improved error handling and fallback attempts
        try:
            # Log SSML for debugging (truncate if too long)
            ssml_preview = ssml[:200] + "..." if len(ssml) > 200 else ssml
            logger.debug(f"Synthesizing SSML: {ssml_preview}")
            
            result = synth.speak_ssml_async(ssml).get()
            
            if result.reason != speechsdk.ResultReason.SynthesizingAudioCompleted:
                error_details = result.cancellation_details
                logger.error(f"TTS failed: {result.reason}")
                if error_details:
                    logger.error(f"Error details: {error_details.error_details}")
                    logger.error(f"Error code: {error_details.error_code}")
                    if error_details.error_code == speechsdk.CancellationErrorCode.AuthenticationFailure:
                        logger.error("Authentication failure: Check your subscription key and region.")
                    elif error_details.error_code == speechsdk.CancellationErrorCode.BadRequest:
                        logger.error("Bad request: Verify the SSML structure and input parameters.")
                        logger.error(f"Problematic text was: {text[:100]}...")
                        logger.error(f"Sanitized text was: {sanitized_text[:100]}...")
                        
                        # Try fallback with plain text synthesis
                        logger.info("Attempting fallback synthesis with plain text...")
                        try:
                            fallback_result = synth.speak_text_async(sanitized_text).get()
                            if fallback_result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
                                logger.info("Fallback synthesis succeeded")
                                return bytes(fallback_result.audio_data)
                        except Exception as fallback_error:
                            logger.error(f"Fallback synthesis also failed: {fallback_error}")
                
                # Return empty bytes instead of raising an exception to prevent loops
                logger.warning("Returning empty audio data due to TTS failure")
                return b""
                
        except Exception as e:
            logger.error(f"Exception during TTS synthesis: {e}")
            logger.error(f"Problematic text: {text[:100]}...")
            return b""

        # 5) Get raw PCM bytes from the result
        pcm_bytes = result.audio_data  # Access audio data directly from the result        return bytes(pcm_bytes)  # Ensure it's bytes type

    def _create_ssml_template(self, text: str, language: str, voice: str, 
                             style: str = "chat", rate: str = "15%", 
                             pitch: str = "default", volume: str = "+0dB") -> str:
        """
        Create SSML template with fallback for compatibility.
        
        Args:
            text: The sanitized text to synthesize
            language: XML language attribute (e.g., "en-US")
            voice: Voice name (e.g., "en-US-JennyNeural")
            style: Expression style (default: "chat")
            rate: Speech rate (default: "15%")
            pitch: Pitch adjustment (default: "default")
            volume: Volume adjustment (default: "+0dB")
            
        Returns:
            Complete SSML string ready for synthesis
        """        # Use a simpler SSML structure to avoid the 0x80045003 error
        # This error often occurs with incompatible voice/style combinations or turn state issues
        
        # Try the most basic SSML first to avoid turn state issues
        # Some voices don't support express-as or have turn state conflicts
        try:
            # First attempt: Basic SSML without express-as
            return f"""<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="{language}">
    <voice name="{voice}">
        <prosody rate="{rate}" pitch="{pitch}" volume="{volume}">
            {text}
        </prosody>
    </voice>
</speak>"""
        except:
            # Fallback: Even simpler structure
            return f"""<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="{language}">
    <voice name="{voice}">
        {text}
    </voice>
</speak>"""
