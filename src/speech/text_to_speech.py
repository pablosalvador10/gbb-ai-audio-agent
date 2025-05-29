import os

import azure.cognitiveservices.speech as speechsdk
from dotenv import load_dotenv

from utils.ml_logging import get_logger

# Load environment variables from a .env file if present
load_dotenv()

# Initialize logger
logger = get_logger()


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
        self.voice = voice

        # Initialize the speech synthesizer for speaker playback
        self.speaker_synthesizer = self._create_speaker_synthesizer()

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
                )  # ✅ Ensure it's converted from bytearray to bytes
            else:
                logger.error(f"Speech synthesis failed: {result.reason}")
                return b""
        except Exception as e:
            logger.error(f"Error synthesizing speech: {e}")
            return b""

    def synthesize_to_base64_frames(
        self, text: str, sample_rate: int = 16000
    ) -> list[str]:
        """
        Synthesize `text` via Azure TTS into raw 16-bit PCM mono at either 16 kHz or 24 kHz,
        then split into 20 ms frames (50 fps), returning each frame as a base64 string.

        - sample_rate: 16000 or 24000
        - frame_size:  0.02s * sample_rate * 2 bytes/sample
                    =  640 bytes @16 kHz, 960 bytes @24 kHz
        """
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
        speech_config.set_speech_synthesis_output_format(sdk_format)

        # 2) Synthesize to memory (audio_config=None)
        synth = speechsdk.SpeechSynthesizer(
            speech_config=speech_config, audio_config=None
        )

        # 3) Build an SSML envelope with reduced rate (80%)
        ##  If you would like to speed up the speech, you can increase the `prosody rate`% accordingly.

        ssml = f"""
        <speak version="1.0" xmlns:mstts="http://www.w3.org/2001/mstts" xml:lang="en-US">
          <voice name="{speech_config.speech_synthesis_voice_name}">
            <mstts:express-as style="chat">
              <prosody rate="15%" pitch="default">
                {text}
              </prosody>
            </mstts:express-as>
          </voice>
        </speak>
        """
        # 4) Synthesize
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
            raise RuntimeError(f"TTS failed: {result.reason}")

        # 5) Get raw PCM bytes from the result
        pcm_bytes = result.audio_data  # Access audio data directly from the result

        return bytes(pcm_bytes)  # Ensure it's bytes type
