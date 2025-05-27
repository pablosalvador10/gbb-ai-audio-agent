// src/components/App.jsx
import React, { useEffect } from "react";

import ChatSection from "./ChatSection";
import ControlBar from "./ControlBar";
import MindMap from "./MindMap";
import PhoneWidget from "./PhoneWidget";
import LogsPanel from "./LogsPanel";

// hooks
import useLogs from "../hooks/useLogs";
import useMindMap from "../hooks/useMindMap";
import useWebSocket from "../hooks/useWebSocket";
import useSpeechRecognizer from "../hooks/useSpeechRecognizer";
import useACSCall from "../hooks/useACSCall";

import styles from "../utils/styles";

const phoneFrame = `${import.meta.env.BASE_URL}phoneimage.png`;

export default function App() {
  //
  // — LOGS (text log & callActive flag)
  //
  const { log, appendLog, callActive, setCallActive } = useLogs();

  //
  // — MIND MAP
  //
  const {
    nodes,
    edges,
    setNodes,
    setEdges,
    resetMindMap,
    addMindMapNode,
    lastUserId,
    lastAssistantId,
    activeSpeaker,
    setActiveSpeaker,
  } = useMindMap();

  //
  // — WEBSOCKET (chat + TTS + streaming + tools)
  //
  const { messages, sendText, socketRef } = useWebSocket({
    appendLog,
    addMindMapNode,
    setActiveSpeaker,
    lastUserId,
    lastAssistantId,
  });

  //
  // — SPEECH RECOGNITION
  //    • onRecognizing: send “interrupt” frames
  //    • onRecognized: forward final text to sendText()
  //
  const { recording, startRecognition, stopRecognition } =
    useSpeechRecognizer(
      // onRecognizing
      (_, e) => {
        if (
          e.result.text.trim() &&
          socketRef.current?.readyState === WebSocket.OPEN
        ) {
          socketRef.current.send(JSON.stringify({ type: "interrupt" }));
          appendLog("→ Sent interrupt");
        }
      },
      // onRecognized
      (_, e) => {
        const text = e.result.text.trim();
        if (text) sendText(text);
      }
    );

  //
  // — AUTO-SCROLL CHAT & CALL ACTIVE SIDE-EFFECTS
  //
  useEffect(() => {
    if (log.includes("Call connected")) setCallActive(true);
    if (log.includes("Call ended") || log.includes("❌ Call disconnected"))
      setCallActive(false);
  }, [log, setCallActive]);

  //
  // — ACS OUTBOUND CALL
  //
  const {
    targetPhoneNumber,
    setTargetPhoneNumber,
    startACSCall,
  } = useACSCall(appendLog, addMindMapNode, lastAssistantId);

  //
  // — WRAP startRecognition TO ALSO RESET CHAT+MINDMAP
  //
  const handleStart = () => {
    resetMindMap();
    startRecognition();
  };

  return (
    <div style={styles.root}>
      {/* ------- HEADER ------- */}
      <header style={styles.header}>
        <h1 style={styles.headerTitle}>🎙️ RTMedAgent</h1>
        <p style={styles.headerSubtitle}>
          Transforming patient care with real-time, intelligent voice
          interactions powered by Azure AI
        </p>
      </header>

      {/* ------- CHAT ------- */}
      <ChatSection messages={messages} />

      {/* ------- CONTROL BAR ------- */}
      <ControlBar
        recording={recording}
        onStart={handleStart}
        onStop={stopRecognition}
      />

      {/* ------- AVATAR + MIND MAP ------- */}
      <div style={styles.secondRow}>
        <div style={styles.card}>Avatar Placeholder</div>
        <MindMap
          nodes={nodes}
          edges={edges}
          activeSpeaker={activeSpeaker}
          setNodes={setNodes}
          setEdges={setEdges}
        />
      </div>

      {/* ------- LOGS ------- */}
      <LogsPanel log={log} />

      {/* ------- PHONE WIDGET ------- */}
      <PhoneWidget
        callActive={callActive}
        targetPhoneNumber={targetPhoneNumber}
        setTargetPhoneNumber={setTargetPhoneNumber}
        startACSCall={startACSCall}
        phoneFrame={phoneFrame}
      />
    </div>
  );
}
