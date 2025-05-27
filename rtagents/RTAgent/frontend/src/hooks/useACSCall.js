// src/hooks/useACSCall.js
import { useState } from "react";

const API_BASE = import.meta.env.VITE_BACKEND_BASE_URL;

export default function useACSCall(appendLog, addMindMapNode, lastAssistantId) {
  const [targetPhoneNumber, setTargetPhoneNumber] = useState("");

  const startACSCall = async () => {
    if (!/^\+\d+$/.test(targetPhoneNumber)) {
      alert("Enter phone in E.164 format, e.g. +15551234567");
      return;
    }
    try {
      const res = await fetch(`${API_BASE}/api/call`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_number: targetPhoneNumber }),
      });
      const json = await res.json();
      if (!res.ok) {
        appendLog(`Call error: ${json.detail || res.statusText}`);
        return;
      }
      const msg = `📞 Call started → ${targetPhoneNumber}`;
      appendLog(msg);
      addMindMapNode({
        speaker: "Assistant",
        text: msg,
        parentId: lastAssistantId.current,
      });
    } catch (e) {
      appendLog(`Network error starting call: ${e.message}`);
      addMindMapNode({
        speaker: "Assistant",
        text: `Network error starting call: ${e.message}`,
        parentId: lastAssistantId.current,
      });
    }
  };

  return { targetPhoneNumber, setTargetPhoneNumber, startACSCall };
}
