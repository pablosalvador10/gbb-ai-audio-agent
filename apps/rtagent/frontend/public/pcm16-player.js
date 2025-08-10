// public/pcm16-player.js
class PCM16Player extends AudioWorkletProcessor {
  constructor() {
    super();
    this.inSR = 16000;         // incoming sample rate (from server)
    this.queue = [];           // queue of Int16Array frames
    this.current = null;
    this.readIdx = 0;

    this.port.onmessage = (e) => {
      const { pcm16, sampleRate } = e.data || {};
      if (sampleRate) this.inSR = sampleRate | 0;
      if (pcm16 && pcm16.length) {
        this.queue.push(pcm16);
      }
    };
  }

  _pull() {
    if (!this.current || this.readIdx >= this.current.length) {
      this.current = this.queue.shift() || null;
      this.readIdx = 0;
    }
    return this.current;
  }

  process(inputs, outputs) {
    const out = outputs[0][0]; // mono
    if (!out) return true;

    const targetSR = sampleRate; // context sample rate (e.g., 48000)
    const src = this._pull();

    if (!src) {
      out.fill(0);
      return true;
    }

    // naive resample: map each output sample to a source index
    const ratio = this.inSR / targetSR;
    const srcLen = src.length;

    for (let i = 0; i < out.length; i++) {
      const srcIndex = Math.min(srcLen - 1, Math.floor((this.readIdx + i) * ratio));
      out[i] = Math.max(-1, Math.min(1, src[srcIndex] / 32768));
    }
    this.readIdx += out.length;
    return true;
  }
}

registerProcessor("pcm16-player", PCM16Player);
