// components/pcm16-player.js
class PCM16Player extends AudioWorkletProcessor {
  constructor() {
    super();
    this.inSR = 16000;      // incoming sample rate
    this.queue = [];        // queue of Int16Array chunks
    this.cur = null;        // current chunk
    this.readIdx = 0;
    this.port.onmessage = (e) => {
      const { pcm16, sampleRate } = e.data;
      if (sampleRate) this.inSR = sampleRate;
      this.queue.push(pcm16);
    };
  }
  _pull() {
    if (!this.cur || this.readIdx >= this.cur.length) {
      this.cur = this.queue.shift() || null;
      this.readIdx = 0;
    }
    return this.cur;
  }
  process(inputs, outputs) {
    const out = outputs[0][0]; // mono
    if (!out) return true;

    const chunk = this._pull();
    if (!chunk) {
      out.fill(0);
      return true;
    }

    // simple resample: map output sample index -> source index
    const targetSR = sampleRate; // context rate (often 48000)
    const ratio = this.inSR / targetSR;
    const src = this.cur;
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
