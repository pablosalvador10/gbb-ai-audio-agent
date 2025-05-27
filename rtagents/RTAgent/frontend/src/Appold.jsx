import React, { useEffect, useRef, useState } from "react";
import {
  AudioConfig,
  SpeechConfig,
  SpeechRecognizer,
  PropertyId,
} from "microsoft-cognitiveservices-speech-sdk";
import ReactFlow, { ReactFlowProvider, MiniMap, Controls } from "reactflow";
import "reactflow/dist/style.css";

/* ------------------------------------------------------------------ *
 *  ENV VARS
 * ------------------------------------------------------------------ */
const {
  VITE_AZURE_SPEECH_KEY: AZURE_SPEECH_KEY,
  VITE_AZURE_REGION: AZURE_REGION,
  VITE_BACKEND_BASE_URL: API_BASE_URL,
} = import.meta.env;
const WS_URL = API_BASE_URL.replace(/^https?/, "wss");

/* ------------------------------------------------------------------ *
 *  ASSETS
 * ------------------------------------------------------------------ */
const phoneFrame = `${import.meta.env.BASE_URL}phoneimage.png`;


if (typeof document !== "undefined") {
  const styleTag =
    document.getElementById("callFX") ?? document.createElement("style");

  styleTag.id = "callFX";
  styleTag.textContent = `
    /* ---------- call‑widget animations ---------- */
    @keyframes ring {
      0%   { transform: scale(.9); opacity: .35; }
      70%  { transform: scale(1.3); opacity: 0; }
      100% { transform: scale(.9); opacity: 0; }
    }
    @keyframes led {
      0%, 100% { opacity: .2; }
      50%      { opacity: .6; }
    }
    @keyframes btnGlow {
      0%, 100% { box-shadow: 0 0 0 0 rgba(220,38,38,.6); }
      50%      { box-shadow: 0 0 8px 4px rgba(220,38,38,0); }
    }

    /* ---------- React‑Flow dark‑mode overrides ---------- */
    .react-flow,
    .react-flow__renderer,
    .react-flow__viewport {
      background: transparent !important;
    }

    /* minimap panel */
    .react-flow__minimap {
      background: #1F2933 !important;
      border: 1px solid #374151;
    }

    /* ---------- root‑node highlight ---------- */
    @keyframes pulseNode {
      0%   { box-shadow:0 0 0 0   rgba(252,211,77,.6); }
      70%  { box-shadow:0 0 12px 6px rgba(252,211,77,0); }
      100% { box-shadow:0 0 0 0   rgba(252,211,77,0); }
    }

    /* zoom / lock buttons */
    .react-flow__controls-button {
      background: #374151;
      color: #E5E7EB;
      border: none;
    }
    .react-flow__controls-button:hover {
      background: #4B5563;
    }
  `;

  /* inject once */
  if (!styleTag.parentNode) document.head.appendChild(styleTag);
}


/* ------------------------------------------------------------------ *
 *  STYLES
 * ------------------------------------------------------------------ */
const styles = {
  root: { fontFamily:"Segoe UI, Roboto, sans-serif", background:"#1F2933",
          color:"#E5E7EB", minHeight:"100vh", padding:32,
          display:"flex", flexDirection:"column", alignItems:"center", gap:40 },
  header:{ width:"100%", maxWidth:1080, textAlign:"center" },
  headerTitle:{ fontSize:"3rem", fontWeight:700, marginBottom:8 },
  headerSubtitle:{ fontSize:"1.1rem", color:"#9CA3AF" },

  chatWrapper:{ background:"#263238", borderRadius:12, padding:20,
                width:"95%", maxWidth:1080, height:480, overflow:"hidden",
                display:"flex", flexDirection:"column" },
  chatScroll:{ flex:1, overflowY:"auto", padding:"12px 18px" },

  controlBar:{ textAlign:"center", width:"100%" },
  primaryBtn:(rec)=>({ padding:"14px 40px", border:"none", borderRadius:10,
                       fontWeight:600, fontSize:"1rem", cursor:"pointer",
                       background: rec ? "#D13438" : "#107C10", color:"#fff" }),

  secondRow:{ display:"flex", gap:32, width:"100%", maxWidth:1080 },
  card:{ flex:1, background:"#263238", borderRadius:12, padding:20,
         height:300, display:"flex", alignItems:"center", justifyContent:"center",
         color:"#9CA3AF", fontStyle:"italic" },

  logsWrapper:{ width:"100%", maxWidth:1080 },
  logsPre:{ background:"#17202A", padding:14, borderRadius:10,
            fontSize:"0.9rem", maxHeight:260, overflow:"auto", whiteSpace:"pre-wrap" },

  phoneWidget:{ position:"fixed", right:28, bottom:28, width:220, height:275,
                display:"flex", alignItems:"center", justifyContent:"center" },
};

/* ------------------------------------------------------------------ *
 *  CHAT BUBBLE
 * ------------------------------------------------------------------ */
const ChatMessage = ({ msg }) => {
  const isUser = msg.speaker === "User";
  const isTool = msg.isTool === true;

  return (
    <div
      style={{
        display: "flex",
        justifyContent: isUser ? "flex-end" : "flex-start",
        marginBottom: 16,
      }}
    >
      <div
        style={{
          background: isTool ? "rgba(26, 101, 112, 0.2)" : isUser ? "#0078D4" : "#394B59",
          color: "#fff",
          padding: "12px 16px",
          borderRadius: 20,
          maxWidth: "75%",
          lineHeight: 1.5,
          boxShadow: "0 2px 6px rgba(0,0,0,.2)",
        }}
      >
        <span style={{ opacity: msg.streaming ? 0.7 : 1 }}>
          {msg.text.split("\n").map((p, i) => (
            <p key={i} style={{ margin: "4px 0" }}>
              {p}
            </p>
          ))}
          {msg.streaming && <em style={{ marginLeft: 4 }}>▌</em>}
        </span>
        <span
          style={{
            display: "block",
            fontSize: "0.8rem",
            color: "#B0BEC5",
            marginTop: 8,
            textAlign: isUser ? "right" : "left",
          }}
        >
          {isTool ? "🛠️ Assistant Called Tool" : isUser ? "👤 User" : "🤖 Assistant"}
        </span>
      </div>
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  MAIN COMPONENT
 * ------------------------------------------------------------------ */
export default function RealTimeVoiceApp() {
  /* ---------- state ---------- */
  const [messages, setMessages] = useState([]);
  const [log, setLog] = useState("");
  const [recording, setRecording] = useState(false);
  const [targetPhoneNumber, setTargetPhoneNumber] = useState("");
  const [callActive, setCallActive] = useState(false);
  const [activeSpeaker, setActiveSpeaker] = useState(null);


  /* ---------- mind‑map state ---------- */
  const rootUser      = { id:"user-root",      data:{label:"👤 User"},      position:{x:-220,y:0},
                          style:{background:"#0F766E",color:"#fff"} };
  const rootAssistant = { id:"assistant-root", data:{label:"🤖 Assistant"}, position:{x: 220,y:0},
                          style:{background:"#4338CA",color:"#fff"} };

  const [nodes, setNodes] = useState([rootUser, rootAssistant]);
  const [edges, setEdges] = useState([]);

  /* helpers for unique ids */
  const idRef = useRef(0);
  const nextId = () => `n-${Date.now()}-${idRef.current++}`;
  const lastUserId      = useRef(null);
  const lastAssistantId = useRef(null);

  /* ---------- refs ---------- */
  const chatRef = useRef(null);
  const socketRef = useRef(null);
  const recognizerRef = useRef(null);

  /* ---------- helpers ---------- */
  const appendLog = (m) =>
    setLog((p) => `${p}\n${new Date().toLocaleTimeString()} - ${m}`);

  /* ---------- effects ---------- */
  useEffect(()=>{ if(chatRef.current) chatRef.current.scrollTop = chatRef.current.scrollHeight; },[messages]);
  useEffect(()=>()=>stopRecognition(),[]);
  useEffect(()=>{
    if(log.includes("Call connected")) setCallActive(true);
    if(log.includes("Call ended")||log.includes("❌ Call disconnected")) setCallActive(false);
  },[log]);

  useEffect(() => {
    setNodes((prev) =>
      prev.map((n) => {
        if (n.id === "user-root") {
          return {
            ...n,
            style: {
              ...n.style,
              border:
                activeSpeaker === "User"
                  ? "2px solid #FCD34D"
                  : "2px solid transparent",
              animation:
                activeSpeaker === "User"
                  ? "pulseNode 1.6s ease-out infinite"
                  : undefined,
            },
          };
        }
        if (n.id === "assistant-root") {
          return {
            ...n,
            style: {
              ...n.style,
              border:
                activeSpeaker === "Assistant"
                  ? "2px solid #FCD34D"
                  : "2px solid transparent",
              animation:
                activeSpeaker === "Assistant"
                  ? "pulseNode 1.6s ease-out infinite"
                  : undefined,
            },
          };
        }
        return n;
      }),
    );
  }, [activeSpeaker]);

  /* ---------- mind‑map functions ---------- */
  const resetMindMap = () => {
    setNodes([ rootUser, rootAssistant ]);
  
    setEdges([
      {
        id: 'e-user-to-assistant',
        source: 'user-root',
        target: 'assistant-root',
        animated: false,
        style: { stroke: '#94A3B8' },
      },
      {
        id: 'e-assistant-to-user',
        source: 'assistant-root',
        target: 'user-root',
        animated: false,
        style: { stroke: '#94A3B8' },
      },
    ]);
  
    lastUserId.current      = 'user-root';
    lastAssistantId.current = 'assistant-root';
    setActiveSpeaker(null);
  };

  const addMindMapNode = ({ speaker, text, functionCall, parentId, toolStatus }) => {
    const isTool = !!functionCall;
  
    if (!isTool) {
      // ——— update one of the TWO root nodes ———
      const rootId = speaker === 'User' ? 'user-root' : 'assistant-root';
      setNodes((nds) =>
        nds.map((n) =>
          n.id === rootId
            ? {
                ...n,
                data: {
                  ...n.data,
                  label: text.length > 40 ? text.slice(0, 37) + '…' : text,
                },
                style: {
                  padding:     8,
                  fontSize:    12,
                  width:       200,
                  height:      60,
                  borderRadius: 8,
                  border: speaker === "User"
                    ? "2px solid #FCD34D"
                    : "2px solid transparent",
                  border: speaker === "Assistant"
                  ? "2px solid #FCD34D"
                  : "2px solid transparent",
                },
              }
            : n
        )
      );
  
      // ——— animate the correct chat‐edge ———
      const edgeId = speaker === 'User'
        ? 'e-user-to-assistant'
        : 'e-assistant-to-user';
  
      setEdges((eds) =>
        eds.map((e) =>
          e.id === edgeId
            ? { ...e, animated: true, style: { ...e.style, stroke: speaker === 'User' ? '#22C55E' : '#4338CA' } }
            : { ...e, animated: false }
        )
      );
  
      // ——— update lastSpeaker ref ———
      if (speaker === 'User') lastUserId.current = rootId;
      else lastAssistantId.current = rootId;
  
      return rootId;
    }
  
    // ——— otherwise: it’s a Tool call, so add a brand-new node ———
    const newId = nextId();
    const toolCount = nodes.filter(n => n.data.speaker === 'Tool').length;
    const toolNode = {
      id: newId,
      data: {
        speaker: 'Tool',
        label: `🛠️ ${functionCall} ${toolStatus === 'running' ? '🔄'
                                      : toolStatus === 'completed' ? '✔️'
                                      : '❌'}`,
      },
      position: { x: 400, y: toolCount * 80 + 50 },
      style: {
        background: '#F59E0B',
        color:       '#000',
        padding:     8,
        fontSize:    12,
        width:    200,
        height:   60,
        boxShadow:"344px 0 0 0 rgba(252,211,77,.6)",
        borderRadius: 8,
      },
    };
  
    const edge = {
      id:     `e-${parentId}-${newId}`,
      source: parentId,
      target: newId,
      animated: true,
      style: { stroke: '#F59E0B', strokeDasharray: '4 2' },
    };
  
    setNodes((nds) => [...nds, toolNode]);
    setEdges((eds) => [...eds, edge]);
  
    return newId;
  };
  

  
  /* ------------------------------------------------------------------ *
   *  VOICE RECOGNITION & WEBSOCKET
   * ------------------------------------------------------------------ */
  const sendToBackend = (text) => {
    socketRef.current?.readyState === WebSocket.OPEN &&
      socketRef.current.send(JSON.stringify({ text }));
  };

  const handleUserSpeech = (text) =>{
    setMessages(m=>[...m,{ speaker:"User", text }]);
    addMindMapNode({ speaker:"User", text });
    setActiveSpeaker("User");
    appendLog(`User: ${text}`);
    sendToBackend(text);
  };

  const startRecognition = () => {
    /* reset mind‑map & chat */
    resetMindMap();
    setMessages([]);

    /* Speech recognizer */
    const cfg = SpeechConfig.fromSubscription(AZURE_SPEECH_KEY, AZURE_REGION);
    cfg.speechRecognitionLanguage = "en-US";

    const rec = new SpeechRecognizer(
      cfg,
      AudioConfig.fromDefaultMicrophoneInput(),
    );
    rec.properties.setProperty(
      PropertyId.Speech_SegmentationSilenceTimeoutMs,
      "800",
    );
    rec.properties.setProperty(
      PropertyId.Speech_SegmentationStrategy,
      "Semantic",
    );
    recognizerRef.current = rec;

    let lastInterrupt = Date.now();
    rec.recognizing = (_, e) => {
      if (
        e.result.text.trim() &&
        socketRef.current?.readyState === WebSocket.OPEN &&
        Date.now() - lastInterrupt > 1000
      ) {
        socketRef.current.send(JSON.stringify({ type: "interrupt" }));
        appendLog("→ Sent interrupt");
        lastInterrupt = Date.now();
      }
    };

    rec.recognized = (_, e) => {
      const text = e.result.text.trim();
      if (text) handleUserSpeech(text);
    };

    rec.startContinuousRecognitionAsync();
    setRecording(true);
    appendLog("🎤 Recognition started");

    /* WebSocket to backend */
    const socket = new WebSocket(`${WS_URL}/realtime`);
    socket.binaryType = "arraybuffer";
    socketRef.current = socket;
    socket.onopen = () => appendLog("🔌 WS open");
    socket.onclose = () => appendLog("🔌 WS closed");
    socket.onmessage = handleSocketMessage;
  };

  const stopRecognition = () => {
    recognizerRef.current?.stopContinuousRecognitionAsync();
    socketRef.current?.readyState === WebSocket.OPEN && socketRef.current.close();
    setRecording(false);
    appendLog("🛑 Recognition stopped");
  };

  const handleSocketMessage = async (event) => {
    if (typeof event.data !== "string") {
      const ctx = new AudioContext();
      const buf = await event.data.arrayBuffer();
      const audioBuf = await ctx.decodeAudioData(buf);
      const src = ctx.createBufferSource();
      src.buffer = audioBuf;
      src.connect(ctx.destination);
      src.start();
      appendLog("🔊 Audio played");
      return;
    }
  
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch {
      appendLog("Ignored non‑JSON frame");
      return;
    }
    const { type, content = "", message = "", function_call } = payload;
    const txt = content || message;
  
    if (type === "assistant_streaming") {
      setActiveSpeaker("Assistant");
      setMessages((prev) => {
        if (prev.at(-1)?.streaming) {
          return prev.map((m, i) => i === prev.length - 1 ? { ...m, text: txt } : m);
        }
        return [...prev, { speaker: "Assistant", text: txt, streaming: true }];
      });
      return;
    }
  
    if (type === "assistant" || type === "status") {
      addMindMapNode({
        speaker: "Assistant",
        text: txt,
        parentId: lastUserId.current,
      });
  
      setActiveSpeaker("Assistant");
      setMessages((prev) => {
        if (prev.at(-1)?.streaming) {
          return prev.map((m, i) => i === prev.length - 1 ? { speaker: "Assistant", text: txt } : m);
        }
        return [...prev, { speaker: "Assistant", text: txt }];
      });
      appendLog("🤖 Assistant responded");
      return;
    }
  
    if (type === "tool_start") {
      const parentId = lastAssistantId.current;
    
      // Always create a new tool node
      addMindMapNode({
        speaker: "Assistant",
        functionCall: payload.tool,
        parentId,
        toolStatus: "running",
      });
    
      setMessages((prev) => [
        ...prev,
        {
          speaker: "Assistant",
          isTool: true,
          text: `🛠️ tool ${payload.tool} started 🔄`,
        },
      ]);
    
      appendLog(`⚙️ ${payload.tool} started`);
      return;
    }
    
  
    if (type === "tool_progress") {
      setMessages((prev) =>
        prev.map((m, i, arr) =>
          i === arr.length - 1 && m.text.startsWith(`🛠️ tool ${payload.tool}`)
            ? { ...m, text: `🛠️ tool ${payload.tool} ${payload.pct}% 🔄` }
            : m,
        ),
      );
      appendLog(`⚙️ ${payload.tool} ${payload.pct}%`);
      return;
    }
  
    if (type === "tool_end") {
      setNodes((prevNodes) =>
        prevNodes.map((node) => {
          if (node.data.label.includes(payload.tool)) {
            return {
              ...node,
              data: {
                ...node.data,
                label: `🛠️ ${payload.tool} ${
                  payload.status === "success" ? "✔️ completed" : "❌ error"
                }`,
              },
              style: {
                ...node.style,
                background: payload.status === "success" ? "#22C55E" : "#EF4444",
              },
            };
          }
          return node;
        }),
      );
    
      const finalText =
        payload.status === "success"
          ? `🛠️ tool ${payload.tool} completed ✔️\n${JSON.stringify(
              payload.result,
              null,
              2,
            )}`
          : `🛠️ tool ${payload.tool} failed ❌\n${payload.error}`;
    
      setMessages((prev) =>
        prev.map((m, i, arr) =>
          i === arr.length - 1 && m.text.startsWith(`🛠️ tool ${payload.tool}`)
            ? { ...m, text: finalText }
            : m,
        ),
      );
    
      appendLog(`⚙️ ${payload.tool} ${payload.status} (${payload.elapsedMs} ms)`);
    }
  };
  

  /* ------------------------------------------------------------------ *
  *  OUTBOUND CALL (ACS)
  * ------------------------------------------------------------------ */
  const startACSCall = async () => {
    if (!targetPhoneNumber || !/^\+\d+$/.test(targetPhoneNumber)) {
      alert("Enter phone in E.164 format e.g. +15551234567");
      return;
    }

    try {
      const res = await fetch(`${API_BASE_URL}/api/call`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_number: targetPhoneNumber }),
      });
      const json = await res.json();

      if (!res.ok) {
        appendLog(`Call error: ${json.detail || res.statusText}`);
        return;
      }

      // 1) Chat bubble
      setMessages((m) => [
        ...m,
        { speaker: "Assistant", text: `📞 Call started → ${targetPhoneNumber}` },
      ]);

      // 2) Mind-map node
      addMindMapNode({
        speaker: "Assistant",
        text: `📞 Call started → ${targetPhoneNumber}`,
        parentId: lastAssistantId.current,
      });
      setActiveSpeaker("Assistant");

      /* optional: relay socket to surface call messages */
      const relay = new WebSocket(`${WS_URL}/relay`);

      relay.onopen = () => {
        appendLog("Relay WS connected");
        // you could also add a mind-map node here if you like:
        // addMindMapNode({ speaker:"Assistant", text:"Relay WS connected", parentId:lastAssistantId.current })
      };

      relay.onerror = (e) => {
        appendLog(`Relay WS error: ${e.message}`);
        addMindMapNode({
          speaker: "Assistant",
          text: `Relay WS error: ${e.message}`,
          parentId: lastAssistantId.current,
        });
        setActiveSpeaker("Assistant");
      };

      relay.onmessage = ({ data }) => {
        try {
          const obj = JSON.parse(data);
          if (obj.type?.startsWith("tool_")) {
            handleSocketMessage({ data: JSON.stringify(obj) });
            return;                              // already handled
          }
        } catch { /* not JSON – fall through */ }

        try {
          const { sender, message } = JSON.parse(data);
          setMessages((m)=>[ ...m, { speaker: sender, text: message } ]);
          addMindMapNode({
            speaker:  sender,
            text:     message,
            parentId: sender==="User" ? lastUserId.current : lastAssistantId.current,
          });
          setActiveSpeaker(sender);
          appendLog(`[Relay] ${sender}: ${message}`);
        } catch {
          appendLog("Relay message parse error");
        }
      };

      relay.onclose = () => {
        appendLog("Relay WS disconnected");
        // Mind-map “call ended”
        addMindMapNode({
          speaker: "Assistant",
          text: "📞 Call ended",
          parentId: lastAssistantId.current,
        });
        setActiveSpeaker("Assistant");
      };
    } catch (e) {
      appendLog(`Network error starting call: ${e.message}`);
      // Mind-map error
      addMindMapNode({
        speaker: "Assistant",
        text: `Network error starting call: ${e.message}`,
        parentId: lastAssistantId.current,
      });
      setActiveSpeaker("Assistant");
    }
  };


  /* ------------------------------------------------------------------ *
   *  RENDER
   * ------------------------------------------------------------------ */
  return (
    <div style={styles.root}>
      {/* ------- HEADER ------- */}
      <header style={styles.header}>
        <h1 style={styles.headerTitle}>🎙️ RTMedAgent</h1>
        <p style={styles.headerSubtitle}>
          Transforming patient care with real‑time, intelligent voice
          interactions powered by Azure AI
        </p>
      </header>

      {/* ------- CHAT ------- */}
      <section style={styles.chatWrapper}>
        <div ref={chatRef} style={styles.chatScroll}>
          {messages.map((m, i) => (
            <ChatMessage key={i} msg={m} />
          ))}
        </div>
      </section>

      {/* ------- START/STOP -------*/}
      <div style={styles.controlBar}>
        <button
          style={styles.primaryBtn(recording)}
          onClick={recording ? stopRecognition : startRecognition}
        >
          {recording ? "⏹ End Conversation" : "▶ Start Conversation"}
        </button>
      </div>

      {/* ------- AVATAR + MIND MAP ------- */}
      <div style={styles.secondRow}>
        <div style={styles.card}>Avatar Placeholder</div>

        <div style={{ ...styles.card, padding:20 }}>
          <ReactFlowProvider>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              fitView
              panOnScroll
              zoomOnScroll
              defaultEdgeOptions={{ markerEnd:{type:"arrowclosed", width:12, height:12} }}
              style={{ width:"100%", height:"100%", background:"transparent" }}
            >
              <MiniMap nodeColor={(n)=>
                n.id==="user-root"      ? "#0F766E" :
                n.id==="assistant-root" ? "#4338CA" :
                n.style?.background || "#334155"
              }/>
              <Controls />
            </ReactFlow>
          </ReactFlowProvider>
        </div>
      </div>

      {/* ------- LOGS ------- */}
      <div style={styles.logsWrapper}>
        <h3 style={{ marginBottom: 8 }}>System Logs</h3>
        <pre style={styles.logsPre}>{log}</pre>
      </div>

      {/* ------- PHONE WIDGET ------- */}
      <div style={{
        position:"fixed",right:28,bottom:0,width:260,height:350,
        display:"flex",alignItems:"center",justifyContent:"center"
      }}>
        {/* pulsating ring */}
        {callActive && (
          <div style={{
            position:"absolute",left: "7%", width:"80%",height:"120%",
            borderRadius:20,background:"rgba(0, 183, 255, 0.88)",
            animation:"ring 1.6s ease-out infinite"
          }}/>
        )}

        {/* phone frame */}
        <img src={phoneFrame} alt="Phone" style={{width:"100%",height:"auto"}} />

        {/* blinking LED */}
        {callActive && (
          <div
            style={{
              position: "absolute",
              top: 88,         
              left: "63%",    
              transform: "translate(-50%, -50%)",
              width: 12,
              height: 12,
              borderRadius: "0%",
              background: "#A3FF12",
              boxShadow: "0 0 6px 3px rgba(73,255,18,.79)",
              animation: "led 1.2s infinite",
              pointerEvents: "none",
            }}
          />
        )}

        {/* overlay controls */}
        <div style={{
          position:"absolute",top:112,left:48,width:134,
          display:"flex",flexDirection:"column",gap:8
        }}>
          <input
            type="tel"
            disabled={callActive}
            value={targetPhoneNumber}
            placeholder="+15551234567"
            onChange={(e)=>setTargetPhoneNumber(e.target.value)}
            style={{
              background:"#1E293B",color:"#E5E7EB",
              border:"1px solid #374151",borderRadius:4,
              padding:"6px 6px",textAlign:"center",
              fontSize:"0.8rem",opacity:callActive?.6:1
            }}
          />

          <button
            onClick={startACSCall}
            style={{
              background:callActive?"#DC2626":"#2563EB",
              color:"#fff",border:"none",borderRadius:4,
              fontSize:"0.8rem",fontWeight:600,padding:"8px 0",
              cursor:"pointer",
              animation:callActive?"btnGlow 1.4s infinite":undefined
            }}
          >
            {callActive?"End":"Call"}
          </button>
        </div>
      </div>

    </div>
  );
}
