// src/RealTimeVoiceApp.jsx
import React, { useEffect, useRef, useState, useCallback } from 'react';
// Removed unused import: reactflow styles
// import "reactflow/dist/style.css";

/* ------------------------------------------------------------------ *
 *  ENV VARS
 * ------------------------------------------------------------------ */
// Simple placeholder that gets replaced at container startup, with fallback for local dev
const backendPlaceholder = '__BACKEND_URL__';
const API_BASE_URL = backendPlaceholder.startsWith('__') 
  ? (import.meta?.env?.VITE_BACKEND_BASE_URL || 'http://localhost:8010')
  : backendPlaceholder;

// Normalize trailing slash
const trimSlash = (s) => s?.replace(/\/+$/, '') || '';
const API_BASE = trimSlash(API_BASE_URL);

// Derive WS URL
const WS_URL = API_BASE.replace(/^https?/, 'wss');

const DEBUG = false;

/* ------------------------------------------------------------------ *
 *  STYLES
 * ------------------------------------------------------------------ */
const styles = {
  root: {
    width: "768px",
    maxWidth: "768px",
    fontFamily: "Segoe UI, Roboto, sans-serif",
    background: "transparent",
    minHeight: "100vh",
    display: "flex",
    flexDirection: "column",
    color: "#1e293b",
    position: "relative",
    alignItems: "center",
    justifyContent: "center",
    padding: "8px",
    border: "0px solid #0e4bf3ff",
  },
  mainContainer: {
    width: "100%",
    maxWidth: "100%",
    height: "90vh",
    maxHeight: "900px",
    background: "white",
    borderRadius: "20px",
    boxShadow: "0 20px 60px rgba(0,0,0,0.15)",
    border: "0px solid #ce1010ff",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    position: "relative",
  },
  appHeader: {
    backgroundColor: "#f8fafc",
    background: "linear-gradient(180deg, #ffffff 0%, #f8fafc 100%)",
    padding: "16px 24px 12px 24px",
    borderBottom: "1px solid #e2e8f0",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    position: "relative",
  },
  appTitleContainer: { display: "flex", flexDirection: "column", alignItems: "center", gap: "4px" },
  appTitleWrapper: { display: "flex", alignItems: "center", gap: "8px" },
  appTitleIcon: { fontSize: "20px", opacity: 0.7 },
  appTitle: { fontSize: "18px", fontWeight: "600", color: "#334155", textAlign: "center", margin: 0, letterSpacing: "0.1px" },
  appSubtitle: {
    fontSize: "12px", fontWeight: "400", color: "#64748b", textAlign: "center", margin: 0,
    letterSpacing: "0.1px", maxWidth: "350px", lineHeight: "1.3", opacity: 0.8,
  },

  waveformSection: {
    backgroundColor: "#f1f5f9",
    background: "linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%)",
    padding: "12px 4px",
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    borderBottom: "1px solid #e2e8f0",
    height: "22%",
    minHeight: "90px",
    position: "relative",
  },
  waveformSectionTitle: { fontSize: "12px", fontWeight: "500", color: "#64748b", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px", opacity: 0.8 },
  sectionDivider: { position: "absolute", bottom: "-1px", left: "20%", right: "20%", height: "1px", backgroundColor: "#cbd5e1", borderRadius: "0.5px", opacity: 0.6 },
  waveformContainer: {
    display: "flex", alignItems: "center", justifyContent: "center",
    width: "100%", height: "60%", padding: "0 10px",
    background: "radial-gradient(ellipse at center, rgba(100, 116, 139, 0.05) 0%, transparent 70%)",
    borderRadius: "6px",
  },
  waveformSvg: {
    width: "100%", height: "60px",
    filter: "drop-shadow(0 1px 2px rgba(100, 116, 139, 0.1))",
    transition: "filter 0.3s ease",
  },

  chatSection: {
    flex: 1, padding: "15px 20px 15px 5px", width: "100%", overflowY: "auto",
    backgroundColor: "#ffffff", borderBottom: "1px solid #e2e8f0",
    display: "flex", flexDirection: "column", position: "relative",
  },
  chatSectionIndicator: { position: "absolute", left: "0", top: "0", bottom: "0", width: "0px", backgroundColor: "#3b82f6" },
  messageContainer: { display: "flex", flexDirection: "column", gap: "16px", flex: 1, overflowY: "auto", padding: "0" },

  userMessage: { alignSelf: "flex-end", maxWidth: "75%", marginRight: "15px", marginBottom: "4px" },
  userBubble: {
    background: "#e0f2fe", color: "#0f172a", padding: "12px 16px", borderRadius: "20px",
    fontSize: "14px", lineHeight: "1.5", border: "1px solid #bae6fd",
    boxShadow: "0 2px 8px rgba(14,165,233,0.15)", wordWrap: "break-word", overflowWrap: "break-word", hyphens: "auto", whiteSpace: "pre-wrap",
  },

  assistantMessage: { alignSelf: "flex-start", maxWidth: "80%", marginLeft: "0px", marginBottom: "4px" },
  assistantBubble: {
    background: "#67d8ef", color: "white", padding: "12px 16px", borderRadius: "20px",
    fontSize: "14px", lineHeight: "1.5", boxShadow: "0 2px 8px rgba(103,216,239,0.3)",
    wordWrap: "break-word", overflowWrap: "break-word", hyphens: "auto", whiteSpace: "pre-wrap",
  },
  agentNameLabel: { fontSize: "10px", fontWeight: "400", color: "#64748b", opacity: 0.7, marginBottom: "2px", marginLeft: "8px", letterSpacing: "0.5px", fontStyle: "italic" },

  controlSection: {
    padding: "12px",
    backgroundColor: "#f1f5f9",
    background: "linear-gradient(180deg, #f1f5f9 0%, #e2e8f0 100%)",
    display: "flex", justifyContent: "center", alignItems: "center",
    height: "15%", minHeight: "100px", borderTop: "1px solid #e2e8f0", position: "relative",
  },
  controlContainer: {
    display: "flex", gap: "8px", background: "white", padding: "12px 16px",
    borderRadius: "24px", boxShadow: "0 4px 16px rgba(100, 116, 139, 0.08), 0 1px 4px rgba(100, 116, 139, 0.04)",
    border: "1px solid #e2e8f0", width: "fit-content",
  },

  resetButton: (isActive, isHovered) => ({
    width: "56px", height: "56px", borderRadius: "50%", border: "none",
    display: "flex", alignItems: "center", justifyContent: "center",
    cursor: "pointer", fontSize: "20px", transition: "all 0.3s ease", position: "relative",
    background: "linear-gradient(135deg, #f1f5f9, #e2e8f0)",
    color: isActive ? "#10b981" : "#64748b",
    transform: isHovered ? "scale(1.08)" : (isActive ? "scale(1.05)" : "scale(1)"),
    boxShadow: isHovered 
      ? "0 8px 24px rgba(100,116,139,0.3), 0 0 0 3px rgba(100,116,139,0.15)"
      : (isActive ? "0 6px 20px rgba(16,185,129,0.3), 0 0 0 3px rgba(16,185,129,0.1)" : "0 2px 8px rgba(0,0,0,0.08)"),
  }),
  micButton: (isActive, isHovered) => ({
    width: "56px", height: "56px", borderRadius: "50%", border: "none",
    display: "flex", alignItems: "center", justifyContent: "center",
    cursor: "pointer", fontSize: "20px", transition: "all 0.3s ease", position: "relative",
    background: isHovered 
      ? (isActive ? "linear-gradient(135deg, #10b981, #059669)" : "linear-gradient(135deg, #dcfce7, #bbf7d0)")
      : "linear-gradient(135deg, #f1f5f9, #e2e8f0)",
    color: isHovered ? (isActive ? "white" : "#16a34a") : (isActive ? "#10b981" : "#64748b"),
    transform: isHovered ? "scale(1.08)" : (isActive ? "scale(1.05)" : "scale(1)"),
    boxShadow: isHovered 
      ? "0 8px 25px rgba(16,185,129,0.4), 0 0 0 4px rgba(16,185,129,0.15), inset 0 1px 2px rgba(255,255,255,0.2)"
      : (isActive ? "0 6px 20px rgba(16,185,129,0.3), 0 0 0 3px rgba(16,185,129,0.1)" : "0 2px 8px rgba(0,0,0,0.08)"),
  }),
  phoneButton: (isActive, isHovered) => ({
    width: "56px", height: "56px", borderRadius: "50%", border: "none",
    display: "flex", alignItems: "center", justifyContent: "center",
    cursor: "pointer", fontSize: "20px", transition: "all 0.3s ease", position: "relative",
    background: isHovered 
      ? (isActive ? "linear-gradient(135deg, #3f75a8ff, #2b5d8f)" : "linear-gradient(135deg, #dcfce7, #bbf7d0)")
      : "linear-gradient(135deg, #f1f5f9, #e2e8f0)",
    color: isHovered ? (isActive ? "white" : "#3f75a8ff") : (isActive ? "#3f75a8ff" : "#64748b"),
    transform: isHovered ? "scale(1.08)" : (isActive ? "scale(1.05)" : "scale(1)"),
    boxShadow: isHovered 
      ? "0 8px 25px rgba(16,185,129,0.4), 0 0 0 4px rgba(16,185,129,0.15), inset 0 1px 2px rgba(255,255,255,0.2)"
      : (isActive ? "0 6px 20px rgba(16,185,129,0.3), 0 0 0 3px rgba(16,185,129,0.1)" : "0 2px 8px rgba(0,0,0,0.08)"),
  }),

  buttonTooltip: {
    position: 'absolute', bottom: '-45px', left: '50%', transform: 'translateX(-50%)',
    background: 'rgba(51, 65, 85, 0.95)', color: '#f1f5f9', padding: '8px 12px', borderRadius: '8px',
    fontSize: '11px', fontWeight: '500', whiteSpace: 'nowrap', backdropFilter: 'blur(10px)',
    boxShadow: '0 4px 12px rgba(0,0,0,0.15)', border: '1px solid rgba(255,255,255,0.1)', pointerEvents: 'none',
    opacity: 0, transition: 'opacity 0.2s ease, transform 0.2s ease', zIndex: 1000,
  },
  buttonTooltipVisible: { opacity: 1, transform: 'translateX(-50%) translateY(-2px)' },

  phoneInputSection: {
    position: "absolute", bottom: "60px", left: "500px", background: "white",
    padding: "20px", borderRadius: "20px", boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
    border: "1px solid #e2e8f0", display: "flex", flexDirection: "column", gap: "12px",
    minWidth: "240px", zIndex: 90,
  },
  phoneInput: {
    padding: "12px 16px", border: "1px solid #d1d5db", borderRadius: "12px", fontSize: "14px", outline: "none",
  },

  backendIndicator: {
    position: "fixed", bottom: "20px", left: "20px", display: "flex", flexDirection: "column", gap: "8px",
    padding: "12px 16px", backgroundColor: "rgba(255, 255, 255, 0.98)", border: "1px solid #e2e8f0",
    borderRadius: "12px", fontSize: "11px", color: "#64748b", boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
    zIndex: 1000, minWidth: "280px", maxWidth: "320px", backdropFilter: "blur(8px)",
  },
  backendHeader: { display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px", cursor: "pointer" },
  backendStatus: { width: "8px", height: "8px", borderRadius: "50%", backgroundColor: "#10b981", animation: "pulse 2s ease-in-out infinite", flexShrink: 0 },
  backendUrl: { fontFamily: "monospace", fontSize: "10px", color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  backendLabel: { fontWeight: "600", color: "#334155", fontSize: "12px", letterSpacing: "0.3px" },

  developerHub: {
    position: "fixed", bottom: "20px", right: "20px", display: "flex", flexDirection: "column", gap: "8px",
    padding: "12px 16px", backgroundColor: "rgba(255, 255, 255, 0.98)", border: "1px solid #e2e8f0",
    borderRadius: "12px", fontSize: "11px", color: "#64748b", boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
    zIndex: 1000, minWidth: "280px", maxWidth: "min(320px, calc(50vw - 400px))", backdropFilter: "blur(8px)",
    maxHeight: "60vh", overflowY: "auto",
  },
  devHubHeader: { display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px", cursor: "pointer" },
  devHubStatus: { width: "8px", height: "8px", borderRadius: "50%", backgroundColor: "#10b981", animation: "pulse 2s ease-in-out infinite", flexShrink: 0 },
  devHubLabel: { fontWeight: "600", color: "#334155", fontSize: "12px", letterSpacing: "0.3px" },
  devSection: { marginBottom: "8px", border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden" },
  devSectionHeader: { display: "flex", alignItems: "center", gap: "8px", padding: "8px 12px", backgroundColor: "#f8fafc", cursor: "pointer", borderBottom: "1px solid #e2e8f0" },
  devSectionIcon: { fontSize: "14px" },
  devSectionTitle: { fontWeight: "600", color: "#334155", fontSize: "11px", flex: 1 },
  devSectionContent: { padding: "8px 12px", backgroundColor: "rgba(255, 255, 255, 0.98)" },
  devSectionArrow: { fontSize: "10px", color: "#64748b", transition: "transform 0.2s ease" },
  devDetailRow: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "4px", fontSize: "10px" },
  devDetailLabel: { color: "#64748b", fontWeight: "500" },
  devDetailValue: { color: "#334155", fontFamily: "monospace", fontSize: "9px" },
  devLogEntry: { fontSize: "10px", color: "#475569", fontFamily: "Consolas, 'Courier New', monospace", padding: "1px 0", borderBottom: "1px solid #f1f5f9", marginBottom: "1px" },
  expandIcon: { marginLeft: "auto", fontSize: "12px", color: "#94a3b8", transition: "transform 0.2s ease" },

  callMeButton: (isActive) => ({
    padding: "12px 24px", background: isActive ? "#ef4444" : "#67d8ef", color: "white",
    border: "none", borderRadius: "8px", cursor: "pointer", fontSize: "14px", fontWeight: "600",
    transition: "all 0.2s ease", boxShadow: "0 2px 8px rgba(0,0,0,0.1)", minWidth: "120px",
  }),

  helpButton: {
    position: "absolute", top: "16px", right: "16px", width: "32px", height: "32px", borderRadius: "50%",
    border: "1px solid #e2e8f0", background: "#f8fafc", color: "#64748b", cursor: "pointer", display: "flex",
    alignItems: "center", justifyContent: "center", fontSize: "14px", transition: "all 0.2s ease", zIndex: 1000,
    boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
  },
  helpButtonHover: { background: "#f1f5f9", color: "#334155", boxShadow: "0 4px 12px rgba(0,0,0,0.1)", transform: "scale(1.05)" },
  helpTooltip: {
    position: "absolute", top: "40px", right: "0px", background: "white", border: "1px solid #e2e8f0",
    borderRadius: "12px", padding: "16px", width: "280px", boxShadow: "0 8px 32px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.08)",
    fontSize: "12px", lineHeight: "1.5", color: "#334155", zIndex: 1001, opacity: 0, transform: "translateY(-8px)",
    pointerEvents: "none", transition: "all 0.2s ease",
  },
  helpTooltipVisible: { opacity: 1, transform: "translateY(0px)", pointerEvents: "auto" },
  helpTooltipTitle: { fontSize: "13px", fontWeight: "600", color: "#1e293b", marginBottom: "8px", display: "flex", alignItems: "center", gap: "6px" },
  helpTooltipText: { marginBottom: "12px", color: "#64748b" },
  helpTooltipContact: {
    fontSize: "11px", color: "#67d8ef", fontFamily: "monospace", background: "#f8fafc",
    padding: "4px 8px", borderRadius: "6px", border: "1px solid #e2e8f0",
  },
};

// Guarded keyframe injection (SSR-safe)
const ensurePulseKeyframes = () => {
  if (typeof document === 'undefined') return;
  const id = 'rt-pulse-keyframes';
  if (!document.getElementById(id)) {
    const style = document.createElement('style');
    style.id = id;
    style.textContent = `
      @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4); }
        70% { box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
      }`;
    document.head.appendChild(style);
  }
};

/* ------------------------------------------------------------------ *
 *  BACKEND HELP BUTTON
 * ------------------------------------------------------------------ */
const BackendHelpButton = () => {
  const [isHovered, setIsHovered] = useState(false);
  const [isClicked, setIsClicked] = useState(false);

  const handleClick = (e) => { e.preventDefault(); e.stopPropagation(); setIsClicked((v) => !v); };

  return (
    <div
      style={{
        width: '14px', height: '14px', borderRadius: '50%', backgroundColor: isHovered ? '#3b82f6' : '#64748b',
        color: 'white', fontSize: '9px', display: 'flex', alignItems: 'center', justifyContent: 'center',
        cursor: 'pointer', transition: 'all 0.2s ease', fontWeight: '600', position: 'relative', flexShrink: 0
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      onClick={handleClick}
    >
      ?
      <div style={{
        visibility: (isHovered || isClicked) ? 'visible' : 'hidden', opacity: (isHovered || isClicked) ? 1 : 0,
        position: 'absolute', bottom: '20px', left: '0', backgroundColor: 'rgba(0, 0, 0, 0.95)', color: 'white',
        padding: '12px', borderRadius: '8px', fontSize: '11px', lineHeight: '1.4', minWidth: '280px', maxWidth: '320px',
        boxShadow: '0 4px 12px rgba(0,0,0,0.3)', zIndex: 10000, transition: 'all 0.2s ease', backdropFilter: 'blur(8px)'
      }}>
        <div style={{ fontSize: '12px', fontWeight: '600', color: '#67d8ef', marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '6px' }}>
          🔧 Backend Status Monitor
        </div>
        <div style={{ marginBottom: '8px' }}>
          Real-time health monitoring for RTAgent backend services including Redis, Azure OpenAI, Speech, and ACS.
        </div>
        <div style={{ marginBottom: '8px' }}>
          <strong>Status Colors:</strong><br/>
          🟢 Healthy • 🟡 Degraded • 🔴 Unhealthy
        </div>
        <div style={{ fontSize: '10px', color: '#94a3b8', fontStyle: 'italic' }}>
          Auto-refreshes every 30 seconds • Click to expand for details
        </div>
        {isClicked && (
          <div style={{ textAlign: 'center', marginTop: '8px', fontSize: '9px', color: '#94a3b8', fontStyle: 'italic' }}>
            Click ? again to close
          </div>
        )}
      </div>
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  HELP BUTTON
 * ------------------------------------------------------------------ */
const HelpButton = () => {
  const [isHovered, setIsHovered] = useState(false);
  const [isClicked, setIsClicked] = useState(false);

  const handleClick = (e) => {
    if (e.target.tagName !== 'A') {
      e.preventDefault();
      e.stopPropagation();
      setIsClicked((v) => !v);
    }
  };

  return (
    <div 
      style={{ ...styles.helpButton, ...(isHovered ? styles.helpButtonHover : {}) }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => !isClicked && setIsHovered(false)}
      onClick={handleClick}
    >
      ?
      <div style={{ ...styles.helpTooltip, ...((isHovered || isClicked) ? styles.helpTooltipVisible : {}) }}>
        <div style={{ ...styles.helpTooltipText, color: '#dc2626', fontWeight: 600, fontSize: '12px', marginBottom: '12px', padding: '8px', backgroundColor: '#fef2f2', borderRadius: '4px', border: '1px solid #fecaca' }}>
          This is a demo available for Microsoft employees only.
        </div>
        <div style={styles.helpTooltipTitle}>🤖 RTAgent Demo</div>
        <div style={styles.helpTooltipText}>
          RTAgent delivers a low-latency, Azure-native stack for AI voice (IVR, phone dial, or web “Call Me”).
        </div>
        <div style={styles.helpTooltipText}>
          Orchestrate multiple specialist agents, configure memory/actions, and tune STT/TTS layers.
        </div>
        <div style={styles.helpTooltipText}>
          🤔 <strong>Try asking about:</strong> claims, policy questions, authentication, or general inquiries.
        </div>
        <div style={styles.helpTooltipText}>
          📑 <a 
            href="https://microsoft.sharepoint.com/teams/rtaudioagent" 
            target="_blank" 
            rel="noopener noreferrer"
            style={{ color: '#3b82f6', textDecoration: 'underline' }}
            onClick={(e) => e.stopPropagation()}
          >
            Visit the Project Hub
          </a> for instructions, deep dives and more.
        </div>
        <div style={styles.helpTooltipText}>
          📧 Questions or feedback? <a 
            href="mailto:rtvoiceagent@microsoft.com?subject=RTAgent Feedback"
            style={{ color: '#3b82f6', textDecoration: 'underline' }}
            onClick={(e) => e.stopPropagation()}
          >
            Contact the team
          </a>
        </div>
        {isClicked && (
          <div style={{ textAlign: 'center', marginTop: '8px', fontSize: '10px', color: '#64748b', fontStyle: 'italic' }}>
            Click ? again to close
          </div>
        )}
      </div>
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  BACKEND INDICATOR (health + agents)
 * ------------------------------------------------------------------ */
const BackendIndicator = ({ url }) => {
  const [isConnected, setIsConnected] = useState(null);
  const [displayUrl, setDisplayUrl] = useState(url);
  const [readinessData, setReadinessData] = useState(null);
  const [agentsData, setAgentsData] = useState(null);
  const [error, setError] = useState(null);
  const [isExpanded, setIsExpanded] = useState(false);
  const [screenWidth, setScreenWidth] = useState(typeof window !== 'undefined' ? window.innerWidth : 1280);

  useEffect(() => {
    const onResize = () => setScreenWidth(window.innerWidth);
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  const checkReadiness = async () => {
    try {
      const response = await fetch(`${url}/readiness`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (data?.status && Array.isArray(data?.checks)) {
        setReadinessData(data);
        setIsConnected(data.status === "ready");
        setError(null);
      } else {
        throw new Error("Invalid response structure");
      }
    } catch (err) {
      if (DEBUG) console.error("Readiness check failed:", err);
      setIsConnected(false);
      setError(err.message);
      setReadinessData(null);
    }
  };

  const checkAgents = async () => {
    try {
      const response = await fetch(`${url}/agents`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (data?.status === "success" && Array.isArray(data?.agents)) {
        setAgentsData(data);
      } else {
        throw new Error("Invalid agents response structure");
      }
    } catch (err) {
      if (DEBUG) console.error("Agents check failed:", err);
      setAgentsData(null);
    }
  };

  useEffect(() => {
    // display url shortener
    try {
      const u = new URL(url);
      const host = u.hostname;
      const protocol = u.protocol.replace(':', '');
      if (host.includes('.azurewebsites.net')) {
        setDisplayUrl(`${protocol}://${host.split('.')[0]}.azure...`);
      } else if (host === 'localhost') {
        setDisplayUrl(`${protocol}://localhost:${u.port || '8010'}`);
      } else {
        setDisplayUrl(`${protocol}://${host}`);
      }
    } catch { setDisplayUrl(url); }

    checkReadiness();
    checkAgents();
    const interval = setInterval(() => { checkReadiness(); checkAgents(); }, 30000);
    return () => clearInterval(interval);
  }, [url]);

  const getOverallStatus = () => {
    if (isConnected === null) return "checking";
    if (!isConnected || !readinessData?.checks) return "unhealthy";
    const hasUnhealthy = readinessData.checks.some(c => c.status === "unhealthy");
    const hasDegraded = readinessData.checks.some(c => c.status === "degraded");
    if (hasUnhealthy) return "unhealthy";
    if (hasDegraded) return "degraded";
    return "healthy";
  };
  const overallStatus = getOverallStatus();
  const statusColor = overallStatus === "healthy" ? "#10b981" : overallStatus === "degraded" ? "#f59e0b" : overallStatus === "unhealthy" ? "#ef4444" : "#6b7280";

  const getResponsiveStyle = () => {
    const baseStyle = { ...styles.backendIndicator, transition: "all 0.3s ease" };
    const containerWidth = 768;
    const containerLeftEdge = (screenWidth / 2) - (containerWidth / 2);
    const availableWidth = containerLeftEdge - 40 - 20;
    if (availableWidth < 200) {
      return { ...baseStyle, minWidth: "150px", maxWidth: "180px", padding: !isExpanded && overallStatus === "healthy" ? "8px 12px" : "10px 14px", fontSize: "10px" };
    } else if (availableWidth < 280) {
      return { ...baseStyle, minWidth: "180px", maxWidth: "250px", padding: !isExpanded && overallStatus === "healthy" ? "10px 14px" : "12px 16px" };
    }
    return { ...baseStyle, minWidth: !isExpanded && overallStatus === "healthy" ? "200px" : "280px", maxWidth: "320px", padding: !isExpanded && overallStatus === "healthy" ? "10px 14px" : "12px 16px" };
  };

  const componentIcons = { redis: "💾", azure_openai: "🧠", speech_services: "🎙️", acs_caller: "📞", rt_agents: "🤖" };
  const componentDescriptions = {
    redis: "Redis Cache - Session & state management",
    azure_openai: "Azure OpenAI - GPT models & embeddings",
    speech_services: "Speech Services - STT/TTS processing",
    acs_caller: "Communication Services - Voice calling",
    rt_agents: "RT Agents - Real-time Voice Agents"
  };

  return (
    <div
      style={getResponsiveStyle()}
      title="Click to expand backend status"
      onClick={() => setIsExpanded(v => !v)}
      onMouseEnter={() => !isExpanded && setIsExpanded(true)}
      onMouseLeave={() => setIsExpanded(false)}
    >
      <div style={styles.backendHeader}>
        <div style={{ ...styles.backendStatus, backgroundColor: statusColor }} />
        <span style={styles.backendLabel}>Backend Status</span>
        <BackendHelpButton />
        <span style={{ ...styles.expandIcon, transform: isExpanded ? "rotate(180deg)" : "rotate(0deg)" }}>▼</span>
      </div>

      {!isExpanded && (
        <div style={{ ...styles.backendUrl, fontSize: "9px", opacity: 0.7, marginTop: "2px" }}>{displayUrl}</div>
      )}

      {(isExpanded || overallStatus !== "healthy") && (
        <>
          {isExpanded && (
            <>
              <div style={{ padding: "8px 10px", backgroundColor: "#f8fafc", borderRadius: "8px", marginBottom: "10px", fontSize: "10px", border: "1px solid #e2e8f0" }}>
                <div style={{ fontWeight: 600, color: "#475569", marginBottom: "4px", display: "flex", alignItems: "center", gap: "6px" }}>🌐 Backend API Entry Point</div>
                <div style={{ color: "#64748b", fontSize: "9px", fontFamily: "monospace", marginBottom: "6px", padding: "3px 6px", backgroundColor: "white", borderRadius: "4px", border: "1px solid #f1f5f9" }}>
                  {url}
                </div>
                <div style={{ color: "#64748b", fontSize: "9px", lineHeight: 1.3 }}>FastAPI server handling WebSockets, voice processing, and agent orchestration</div>
              </div>

              {readinessData && (
                <div style={{
                  padding: "6px 8px",
                  backgroundColor: overallStatus === "healthy" ? "#f0fdf4" : overallStatus === "degraded" ? "#fffbeb" : "#fef2f2",
                  borderRadius: "6px", marginBottom: "8px", fontSize: "10px",
                  border: `1px solid ${overallStatus === "healthy" ? "#bbf7d0" : overallStatus === "degraded" ? "#fed7aa" : "#fecaca"}`
                }}>
                  <div style={{ fontWeight: 600, color: overallStatus === "healthy" ? "#166534" : overallStatus === "degraded" ? "#92400e" : "#dc2626", marginBottom: "2px" }}>
                    System Status: {overallStatus.charAt(0).toUpperCase() + overallStatus.slice(1)}
                  </div>
                  <div style={{ color: "#64748b", fontSize: "9px" }}>
                    {readinessData.checks.length} components • Last check: {new Date().toLocaleTimeString()}
                  </div>
                </div>
              )}
            </>
          )}

          {error ? (
            <div style={{ fontSize: "10px", color: "#ef4444", marginTop: "4px", fontStyle: "italic" }}>⚠️ Connection failed: {error}</div>
          ) : readinessData?.checks ? (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: "8px", marginTop: "8px", paddingTop: "8px", borderTop: "1px solid #f1f5f9" }}>
                {readinessData.checks.map((check, idx) => (
                  <div
                    key={idx}
                    style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", padding: "8px 10px", backgroundColor: "#f8fafc", borderRadius: "8px", fontSize: "10px", border: "1px solid #f1f5f9" }}
                    title={check.details || `${check.component} status: ${check.status}`}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: "6px", width: "100%" }}>
                      <span>{componentIcons[check.component] || "•"}</span>
                      <div style={{ width: "6px", height: "6px", borderRadius: "50%", backgroundColor:
                        check.status === "healthy" ? "#10b981" : check.status === "degraded" ? "#f59e0b" :
                        check.status === "unhealthy" ? "#ef4444" : "#6b7280" }} />
                      <span style={{ fontWeight: 500, color: "#475569", textTransform: "capitalize" }}>
                        {String(check.component || '').replace(/_/g, ' ')}
                      </span>
                      {typeof check.check_time_ms === 'number' && (
                        <span style={{ fontSize: "9px", color: "#94a3b8", marginLeft: "auto" }}>{check.check_time_ms.toFixed(0)}ms</span>
                      )}
                    </div>
                    {isExpanded && (
                      <>
                        <div style={{ fontSize: "9px", color: "#64748b", marginTop: "4px", lineHeight: 1.3, fontStyle: "italic" }}>
                          {componentDescriptions[check.component] || "Backend service component"}
                        </div>
                        {check.details && (
                          <div style={{ fontSize: "9px", color: check.status === "healthy" ? "#10b981" : check.status === "degraded" ? "#f59e0b" : "#ef4444", marginTop: "2px", fontWeight: 500 }}>
                            {check.details}
                          </div>
                        )}
                      </>
                    )}
                  </div>
                ))}
              </div>
              {isExpanded && readinessData.checks.some(c => c.details) && (
                <div style={{ marginTop: "8px", paddingTop: "8px", borderTop: "1px solid #f1f5f9", fontSize: "9px", color: "#64748b" }}>
                  {readinessData.checks.filter(c => c.details).map((check, idx) => (
                    <div key={idx} style={{ marginBottom: "4px" }}>
                      <strong>{check.component.replace(/_/g, ' ')}:</strong> {check.details}
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : (
            <div style={{ fontSize: "10px", color: "#64748b", marginTop: "4px", fontStyle: "italic" }}>Checking components...</div>
          )}

          {readinessData?.response_time_ms && isExpanded && (
            <div style={{ fontSize: "9px", color: "#94a3b8", marginTop: "8px", paddingTop: "8px", borderTop: "1px solid #f1f5f9", textAlign: "center", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span>Health check latency: {readinessData.response_time_ms.toFixed(0)}ms</span>
              <span title="Auto-refreshes every 30 seconds">🔄</span>
            </div>
          )}

          {isExpanded && agentsData?.agents && (
            <div style={{ marginTop: "10px", paddingTop: "10px", borderTop: "2px solid #e2e8f0" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "8px", padding: "6px 8px", backgroundColor: "#f1f5f9", borderRadius: "6px" }}>
                <div style={{ fontWeight: 600, color: "#475569", fontSize: "11px", display: "flex", alignItems: "center", gap: "6px" }}>🤖 RT Agents ({agentsData.agents.length})</div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: "6px", fontSize: "10px" }}>
                {agentsData.agents.map((agent, idx) => (
                  <div key={idx} style={{ padding: "8px 10px", border: "1px solid #e2e8f0", borderRadius: "6px", backgroundColor: "white", transition: "all 0.2s ease" }}
                       title={agent.description || `${agent.name} - Real-time voice agent`}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "4px" }}>
                      <div style={{ fontWeight: 600, color: "#374151", display: "flex", alignItems: "center", gap: "6px" }}>
                        <span style={{ width: "8px", height: "8px", borderRadius: "50%", backgroundColor: agent.status === "loaded" ? "#10b981" : "#ef4444", display: "inline-block" }} />
                        {agent.name}
                      </div>
                      <div style={{ fontSize: "9px", color: "#64748b", display: "flex", alignItems: "center", gap: "6px" }}>
                        {agent.model?.deployment_id && <span title={`Model: ${agent.model.deployment_id}`}>💭 {agent.model.deployment_id.replace('gpt-', '')}</span>}
                        {agent.voice?.current_voice && <span title={`Voice: ${agent.voice.current_voice}`}>🔊 {agent.voice.current_voice.split('-').pop()?.replace('Neural', '')}</span>}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ fontSize: "8px", color: "#94a3b8", marginTop: "8px", textAlign: "center", fontStyle: "italic" }}>
                Runtime configuration • Changes require restart for persistence • Contact rtvoiceagent@microsoft.com
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  DEVELOPER HUB (compact + logs)
 * ------------------------------------------------------------------ */
const DeveloperHub = ({ socketRef }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [expandedSections, setExpandedSections] = useState({ session: false, logs: false });
  const [devLogs, setDevLogs] = useState([]);
  const [sessionData, setSessionData] = useState({
    sessionId: null, connectionId: null, startTime: null, duration: 0, activeAgent: 'AutoAuth', authenticated: false
  });

  const toggleSection = (section) => setExpandedSections(prev => ({ ...prev, [section]: !prev[section] }));

  const addDevLog = useCallback((message) => {
    const timestamp = new Date().toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 });
    setDevLogs(prev => [...prev.slice(-49), { timestamp, message }]);
  }, []);

  // Intercept socket messages (without losing the main handler)
  useEffect(() => {
    if (!socketRef?.current) return;
    const socket = socketRef.current;
    const originalOnMessage = socket.onmessage;

    socket.onmessage = (event) => {
      if (typeof originalOnMessage === 'function') originalOnMessage(event);
      if (typeof event.data === 'string') {
        try {
          const data = JSON.parse(event.data);
          if (DEBUG) console.log('WS Message (dev hub):', data);

          if (data.type === "status" && data.session_id) {
            setSessionData(prev => ({
              ...prev,
              sessionId: data.session_id,
              connectionId: data.callId || prev.connectionId,
              startTime: prev.startTime || new Date(),
              activeAgent: data.orchestrator || prev.activeAgent
            }));
            addDevLog(`📋 Session ${data.session_id} detected`);
          }

          const t = data.type;
          if (t && t !== "keepalive" && t !== "heartbeat") {
            const summary =
              t === "assistant_streaming" ? `💬 Assistant Streaming: ${(data.chunk || '').slice(0, 50)}${(data.chunk || '').length > 50 ? '...' : ''}` :
              t === "error" ? `❌ Error: ${data.message || data.error || 'Unknown error'}` :
              t === "warning" ? `⚠️ Warning: ${data.message || 'Unknown warning'}` :
              t === "connection" ? `🔗 Connection: ${data.status || data.message || 'event'}` :
              t === "tool_start" ? `🛠️ Tool Started: ${data.tool}` :
              t === "tool_end" ? `✅ Tool Completed: ${data.tool} (${data.elapsedMs}ms)` :
              `📝 ${t}: ${JSON.stringify(data).slice(0, 80)}${JSON.stringify(data).length > 80 ? '...' : ''}`;
            addDevLog(summary);
          }

          if (data.sender && data.sender !== "User" && data.sender !== "System") {
            const realAgent = data.orchestrator || (data.sender.includes("Auth") ? "AutoAuth"
              : data.sender.includes("Claim") ? "Claims"
              : data.sender.includes("General") ? "General" : data.sender);
            setSessionData(prev => {
              if (prev.activeAgent !== realAgent) {
                addDevLog(`🔄 Agent Handoff: ${prev.activeAgent} → ${realAgent}`);
                return { ...prev, activeAgent: realAgent };
              }
              return prev;
            });
            if (data.message) addDevLog(`🤖 ${realAgent}: ${data.message.slice(0, 80)}${data.message.length > 80 ? '...' : ''}`);
          }

          if (data.message && (data.message.includes("authenticated") || data.message.includes("caller"))) {
            setSessionData(prev => ({ ...prev, authenticated: true }));
            addDevLog('🔐 User Authentication Success');
          }
        } catch {
          if (typeof event.data === 'string' && event.data.length < 1000) {
            addDevLog(`🔍 Raw Message: ${event.data.slice(0, 100)}${event.data.length > 100 ? '...' : ''}`);
          }
        }
      } else {
        // Binary frame
        const size = event.data?.byteLength || event.data?.size || 0;
        if (size > 0) addDevLog(`🎵 Audio Data: ${size} bytes`);
      }
    };

    return () => { socket.onmessage = originalOnMessage; };
  }, [socketRef, addDevLog]);

  // Duration ticker
  useEffect(() => {
    if (!sessionData.startTime) return;
    const id = setInterval(() => {
      setSessionData(prev => ({ ...prev, duration: Math.floor((Date.now() - prev.startTime) / 1000) }));
    }, 1000);
    return () => clearInterval(id);
  }, [sessionData.startTime]);

  // Init session (UI-side)
  useEffect(() => {
    if (socketRef?.current?.readyState === WebSocket.OPEN && !sessionData.sessionId) {
      const sessionId = `sess_${Math.random().toString(36).slice(2, 10)}`;
      const connectionId = `conn_${Math.random().toString(36).slice(2, 8)}`;
      setSessionData(prev => ({ ...prev, sessionId, connectionId, startTime: new Date(), activeAgent: 'AutoAuth', authenticated: false }));
      addDevLog('🔄 Developer Hub initialized');
      addDevLog(`📋 Session ${sessionId} created`);
      addDevLog(`🔗 Connection ${connectionId} established`);
      addDevLog('🤖 AutoAuth agent activated');
      addDevLog('📡 Real-time logging active');
    }
  }, [socketRef, sessionData.sessionId, addDevLog]);

  // Console interception (error/warn/info)
  useEffect(() => {
    const orig = { log: console.log, error: console.error, warn: console.warn, info: console.info };
    console.error = (...args) => { orig.error(...args); addDevLog(`❌ Console Error: ${args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ').slice(0, 120)}`); };
    console.warn  = (...args) => { orig.warn(...args);  addDevLog(`⚠️ Console Warning: ${args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ').slice(0, 120)}`); };
    console.info  = (...args) => { orig.info(...args);  if (DEBUG) addDevLog(`ℹ️ Console Info: ${args.join(' ')}`); };
    console.log   = (...args) => { orig.log(...args);   if (DEBUG) addDevLog(`📝 Console Log: ${args.join(' ').slice(0, 120)}`); };
    return () => { console.error = orig.error; console.warn = orig.warn; console.info = orig.info; console.log = orig.log; };
  }, [addDevLog]);

  const formatDuration = (seconds) => {
    if (!seconds || seconds < 60) return `${seconds || 0}s`;
    const m = Math.floor(seconds / 60), s = seconds % 60;
    return `${m}m ${s}s`;
  };

  return (
    <div style={styles.developerHub} onClick={(e) => e.stopPropagation()}>
      <div style={styles.devHubHeader} onClick={() => setIsExpanded(v => !v)}>
        <div style={styles.devHubStatus} />
        <span style={styles.devHubLabel}>Developer Hub</span>
        <span style={{ ...styles.expandIcon, transform: isExpanded ? "rotate(180deg)" : "rotate(0deg)" }}>▼</span>
      </div>

      {!isExpanded && (
        <div style={{ fontSize: "9px", color: "#64748b", marginTop: "2px" }}>
          {sessionData.sessionId ? `Session: ${sessionData.sessionId} • Agent: ${sessionData.activeAgent} • ${formatDuration(sessionData.duration)}`
            : "Waiting for session..."}
        </div>
      )}

      {isExpanded && (
        <div onClick={(e) => e.stopPropagation()}>
          {/* Session Context */}
          <div style={styles.devSection}>
            <div style={styles.devSectionHeader} onClick={() => toggleSection('session')}>
              <span style={styles.devSectionIcon}>📋</span>
              <span style={styles.devSectionTitle}>Session Context</span>
              <span style={{ ...styles.devSectionArrow, transform: expandedSections.session ? "rotate(180deg)" : "rotate(0deg)" }}>▼</span>
            </div>
            {expandedSections.session && (
              <div style={styles.devSectionContent}>
                <div style={styles.devDetailRow}><span style={styles.devDetailLabel}>ID:</span><span style={styles.devDetailValue}>{sessionData.sessionId || 'N/A'}</span></div>
                <div style={styles.devDetailRow}><span style={styles.devDetailLabel}>Duration:</span><span style={styles.devDetailValue}>{formatDuration(sessionData.duration)}</span></div>
                <div style={styles.devDetailRow}><span style={styles.devDetailLabel}>Connection:</span><span style={styles.devDetailValue}>{sessionData.connectionId || 'N/A'}</span></div>
                <div style={styles.devDetailRow}><span style={styles.devDetailLabel}>Active Agent:</span><span style={styles.devDetailValue}>{sessionData.activeAgent}</span></div>
                <div style={styles.devDetailRow}>
                  <span style={styles.devDetailLabel}>Status:</span>
                  <span style={{ ...styles.devDetailValue, color: sessionData.authenticated ? '#10b981' : '#f59e0b' }}>
                    {sessionData.authenticated ? '✅ Authenticated' : '⏳ Pending Auth'}
                  </span>
                </div>
              </div>
            )}
          </div>

          {/* Logs */}
          <div style={styles.devSection}>
            <div style={styles.devSectionHeader} onClick={() => toggleSection('logs')}>
              <span style={styles.devSectionIcon}>📡</span>
              <span style={styles.devSectionTitle}>Real-Time Logs</span>
              <span style={{ ...styles.devSectionArrow, transform: expandedSections.logs ? "rotate(180deg)" : "rotate(0deg)" }}>▼</span>
            </div>
            {expandedSections.logs && (
              <div style={styles.devSectionContent}>
                <div style={{ maxHeight: '140px', overflowY: 'auto', fontSize: '10px' }}>
                  {devLogs.length > 0 ? (
                    devLogs.slice(-20).reverse().map((log, idx) => {
                      let color = '#64748b';
                      if (log.message.includes('❌') || /error/i.test(log.message)) color = '#ef4444';
                      else if (log.message.includes('⚠️') || /warn/i.test(log.message)) color = '#f59e0b';
                      else if (log.message.includes('✅') || /success/i.test(log.message)) color = '#10b981';
                      else if (log.message.includes('🔧') || /backend/i.test(log.message)) color = '#8b5cf6';
                      else if (log.message.includes('🤖') || /Agent/i.test(log.message)) color = '#06b6d4';
                      else if (log.message.includes('🎵') || /Audio/i.test(log.message)) color = '#14b8a6';
                      else if (log.message.includes('🔗') || /Connection/i.test(log.message)) color = '#3b82f6';

                      return (
                        <div key={idx} style={{ ...styles.devLogEntry, color, borderLeft: `2px solid ${color}`, paddingLeft: '6px', marginBottom: '1px' }}>
                          <span style={{ color: '#64748b', fontSize: '9px' }}>[{log.timestamp}]</span>{' '}
                          <span style={{ color }}>{log.message}</span>
                        </div>
                      );
                    })
                  ) : (
                    <div style={{ color: '#64748b', fontStyle: 'italic', padding: '8px', fontSize: '10px' }}>
                      Waiting for activity... logs will appear here.
                    </div>
                  )}
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '8px', padding: '4px', borderTop: '1px solid #e2e8f0', fontSize: '9px' }}>
                  <span style={{ color: '#64748b' }}>{devLogs.length} logs captured</span>
                  <div style={{ display: 'flex', gap: '8px' }}>
                    <button
                      onClick={(e) => { e.stopPropagation(); setDevLogs([]); addDevLog('🧹 Logs cleared by user'); }}
                      style={{ background: 'none', border: '1px solid #e2e8f0', borderRadius: '3px', padding: '2px 6px', color: '#64748b', fontSize: '9px', cursor: 'pointer' }}
                    >
                      Clear
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        const txt = devLogs.map(l => `[${l.timestamp}] ${l.message}`).join('\n');
                        navigator.clipboard?.writeText(txt);
                        addDevLog('📋 Logs copied to clipboard');
                      }}
                      style={{ background: 'none', border: '1px solid #e2e8f0', borderRadius: '3px', padding: '2px 6px', color: '#64748b', fontSize: '9px', cursor: 'pointer' }}
                    >
                      Copy
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  WAVEFORM
 * ------------------------------------------------------------------ */
const WaveformVisualization = ({ speaker, audioLevel = 0, outputAudioLevel = 0 }) => {
  const [waveOffset, setWaveOffset] = useState(0);
  const [amplitude, setAmplitude] = useState(5);
  const animationRef = useRef();

  useEffect(() => {
    const animate = () => {
      setWaveOffset(prev => (prev + (speaker ? 2 : 1)) % 1000);
      setAmplitude(() => {
        if (audioLevel > 0.01) {
          const scaled = audioLevel * 25;
          const varr = Math.sin(Date.now() * 0.002) * (scaled * 0.2);
          return Math.max(8, scaled + varr);
        } else if (outputAudioLevel > 0.01) {
          const scaled = outputAudioLevel * 20;
          const varr = Math.sin(Date.now() * 0.0018) * (scaled * 0.25);
          return Math.max(6, scaled + varr);
        } else if (speaker) {
          const t = Date.now() * 0.002;
          return 10 + Math.sin(t) * 5;
        } else {
          const t = Date.now() * 0.0008;
          return 3 + Math.sin(t) * 1.5;
        }
      });
      animationRef.current = requestAnimationFrame(animate);
    };
    animationRef.current = requestAnimationFrame(animate);
    return () => { if (animationRef.current) cancelAnimationFrame(animationRef.current); };
  }, [speaker, audioLevel, outputAudioLevel]);

  const generateWavePath = () => {
    const width = 750, height = 100, centerY = height / 2, frequency = 0.02, points = 100;
    let d = `M 0 ${centerY}`;
    for (let i = 0; i <= points; i++) {
      const x = (i / points) * width;
      const y = centerY + Math.sin((x * frequency + waveOffset * 0.1)) * amplitude;
      d += ` L ${x} ${y}`;
    }
    return d;
  };
  const generateSecondaryWave = () => {
    const width = 750, height = 100, centerY = height / 2, frequency = 0.025, points = 100;
    let d = `M 0 ${centerY}`;
    for (let i = 0; i <= points; i++) {
      const x = (i / points) * width;
      const y = centerY + Math.sin((x * frequency + waveOffset * 0.12)) * (amplitude * 0.6);
      d += ` L ${x} ${y}`;
    }
    return d;
  };

  const baseColor = speaker === "User" ? "#ef4444" : speaker === "Assistant" ? "#67d8ef" : "#3b82f6";
  const opacity = speaker ? 0.8 : 0.4;

  return (
    <div style={styles.waveformContainer}>
      <svg style={styles.waveformSvg} viewBox="0 0 750 80" preserveAspectRatio="xMidYMid meet">
        <path d={generateWavePath()} stroke={baseColor} strokeWidth={speaker ? 3 : 2} fill="none" opacity={opacity} strokeLinecap="round" />
        <path d={generateSecondaryWave()} stroke={baseColor} strokeWidth={speaker ? 2 : 1.5} fill="none" opacity={opacity * 0.5} strokeLinecap="round" />
      </svg>
      {window.location.hostname === 'localhost' && (
        <div style={{ position: 'absolute', bottom: '-25px', left: '50%', transform: 'translateX(-50%)', fontSize: '10px', color: '#666', whiteSpace: 'nowrap' }}>
          Input: {(audioLevel * 100).toFixed(1)}% | Amp: {amplitude.toFixed(1)}
        </div>
      )}
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  CHAT BUBBLE
 * ------------------------------------------------------------------ */
const ChatBubble = ({ message }) => {
  const { speaker, text, isTool, streaming } = message;
  const isUser = speaker === "User";
  const isSpecialist = speaker?.includes("Specialist");
  const isAuthAgent = speaker === "Auth Agent";

  if (isTool) {
    return (
      <div style={{ ...styles.assistantMessage, alignSelf: "center" }}>
        <div style={{ ...styles.assistantBubble, background: "#8b5cf6", textAlign: "center", fontSize: "14px" }}>
          {text}
        </div>
      </div>
    );
  }

  const lines = String(text || '').split("\n");

  return (
    <div style={isUser ? styles.userMessage : styles.assistantMessage}>
      {!isUser && (isSpecialist || isAuthAgent) && <div style={styles.agentNameLabel}>{speaker}</div>}
      <div style={isUser ? styles.userBubble : styles.assistantBubble}>
        {lines.map((line, i) => (<div key={i}>{line}</div>))}
        {streaming && <span style={{ opacity: 0.7 }}>▌</span>}
      </div>
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  MAIN
 * ------------------------------------------------------------------ */
function RealTimeVoiceApp() {
  useEffect(() => { ensurePulseKeyframes(); }, []);

  const [messages, setMessages] = useState([]);
  const [log, setLog] = useState("");
  const [recording, setRecording] = useState(false);
  const [targetPhoneNumber, setTargetPhoneNumber] = useState("");
  const [callActive, setCallActive] = useState(false);
  const [activeSpeaker, setActiveSpeaker] = useState(null);
  const [showPhoneInput, setShowPhoneInput] = useState(false);

  const [showResetTooltip, setShowResetTooltip] = useState(false);
  const [showMicTooltip, setShowMicTooltip] = useState(false);
  const [showPhoneTooltip, setShowPhoneTooltip] = useState(false);
  const [resetHovered, setResetHovered] = useState(false);
  const [micHovered, setMicHovered] = useState(false);
  const [phoneHovered, setPhoneHovered] = useState(false);

  const chatRef = useRef(null);
  const messageContainerRef = useRef(null);
  const socketRef = useRef(null);

  const audioContextRef = useRef(null);
  const processorRef = useRef(null);
  const analyserRef = useRef(null);
  const micStreamRef = useRef(null);
  const outputAudioCtxRef = useRef(null);

  const [audioLevel, setAudioLevel] = useState(0);

  const appendLog = (m) => setLog(p => `${p}\n${new Date().toLocaleTimeString()} - ${m}`);

  // autoscroll on new message
  useEffect(() => {
    const el = messageContainerRef.current || chatRef.current;
    if (el) el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
  }, [messages]);

  // teardown
  useEffect(() => {
    return () => {
      try { processorRef.current?.disconnect(); } catch {}
      processorRef.current = null;
      try { audioContextRef.current?.close(); } catch {}
      audioContextRef.current = null;
      try { socketRef.current?.close(); } catch {}
      socketRef.current = null;
      try { micStreamRef.current?.getTracks()?.forEach(t => t.stop()); } catch {}
      micStreamRef.current = null;
    };
  }, []);

  // derive callActive from log
  useEffect(() => {
    if (log.includes("Call connected")) setCallActive(true);
    if (log.includes("Call ended")) setCallActive(false);
  }, [log]);

  const startRecognition = async () => {
    setMessages([]);
    appendLog("🎤 PCM streaming started");

    // WS
    const socket = new WebSocket(`${WS_URL}/api/v1/realtime/conversation`);
    socket.binaryType = "arraybuffer";

    socket.onopen = () => { appendLog("🔌 WS open"); if (DEBUG) console.log("WebSocket OPEN"); };
    socket.onclose = () => { if (DEBUG) console.log("WebSocket CLOSED"); };
    socket.onerror = (err) => { console.error("WebSocket error:", err); };
    socket.onmessage = handleSocketMessage;
    socketRef.current = socket;

    // MIC
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    micStreamRef.current = stream;

    const audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
    audioContextRef.current = audioCtx;

    const source = audioCtx.createMediaStreamSource(stream);
    const analyser = audioCtx.createAnalyser();
    analyser.fftSize = 256;
    analyser.smoothingTimeConstant = 0.3;
    analyserRef.current = analyser;

    source.connect(analyser);

    // ScriptProcessor (deprecated but widely supported). For production consider AudioWorklet.
    const bufferSize = 512;
    const processor = audioCtx.createScriptProcessor(bufferSize, 1, 1);
    processorRef.current = processor;
    analyser.connect(processor);

    processor.onaudioprocess = (evt) => {
      const float32 = evt.inputBuffer.getChannelData(0);

      // level
      let sum = 0;
      for (let i = 0; i < float32.length; i++) sum += float32[i] * float32[i];
      const rms = Math.sqrt(sum / float32.length);
      const level = Math.min(1, rms * 10);
      setAudioLevel(level);

      // PCM int16
      const int16 = new Int16Array(float32.length);
      for (let i = 0; i < float32.length; i++) {
        const s = Math.max(-1, Math.min(1, float32[i]));
        int16[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
      }

      if (socket.readyState === WebSocket.OPEN) {
        socket.send(int16.buffer);
      }
    };

    source.connect(processor);
    // DO NOT connect to destination to avoid feedback:
    // processor.connect(audioCtx.destination);

    setRecording(true);
  };

  const stopRecognition = () => {
    try { processorRef.current?.disconnect(); } catch {}
    processorRef.current = null;

    try { audioContextRef.current?.close(); } catch {}
    audioContextRef.current = null;

    try { socketRef.current?.close(); } catch {}
    socketRef.current = null;

    try { micStreamRef.current?.getTracks()?.forEach(t => t.stop()); } catch {}
    micStreamRef.current = null;

    setMessages(m => [...m, { speaker: "System", text: "🛑 Session stopped" }]);
    setActiveSpeaker("System");
    setRecording(false);
    appendLog("🛑 PCM streaming stopped");
  };

  const pushIfChanged = (arr, msg) => {
    if (arr.length === 0) return [msg];
    const last = arr[arr.length - 1];
    if (last.speaker === msg.speaker && last.text === msg.text) return arr;
    return [...arr, msg];
  };

  const ensureOutputAudioCtx = () => {
    if (!outputAudioCtxRef.current) {
      outputAudioCtxRef.current = new (window.AudioContext || window.webkitAudioContext)();
    }
    return outputAudioCtxRef.current;
  };

  const handleSocketMessage = async (event) => {
    // Binary audio frame from server
    if (event.data instanceof ArrayBuffer) {
      try {
        const ctx = ensureOutputAudioCtx();
        // Assume the server sends decodable audio (e.g., WAV/Opus-decoded bytes).
        // If it's raw PCM, you'd need metadata and manual buffer creation.
        const audioBuf = await ctx.decodeAudioData(event.data.slice(0));
        const src = ctx.createBufferSource();
        src.buffer = audioBuf;
        src.connect(ctx.destination);
        src.start();
        appendLog("🔊 Audio played");
      } catch (e) {
        if (DEBUG) console.error("Audio decode/play error:", e);
      }
      return;
    }

    // Text frames
    if (typeof event.data !== "string") return;

    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch {
      appendLog("Ignored non-JSON frame");
      return;
    }

    // Normalize {sender, message}
    if (payload.sender && payload.message) {
      payload.speaker = payload.sender;
      payload.content = payload.message;
    }

    const { type, content = "", message = "", speaker } = payload;
    const txt = content || message;
    const msgType = (type || "").toLowerCase();

    // USER
    if (msgType === "user" || speaker === "User") {
      setActiveSpeaker("User");
      setMessages(prev => [...prev, { speaker: "User", text: txt }]);
      appendLog(`User: ${txt}`);
      return;
    }

    // ASSISTANT STREAM
    if (type === "assistant_streaming") {
      const streamingSpeaker = speaker || "Assistant";
      setActiveSpeaker(streamingSpeaker);
      setMessages(prev => {
        const last = prev[prev.length - 1];
        if (last?.streaming) {
          return prev.map((m, i) => (i === prev.length - 1 ? { ...m, text: txt } : m));
        }
        return [...prev, { speaker: streamingSpeaker, text: txt, streaming: true }];
      });
      return;
    }

    // ASSISTANT FINAL / STATUS
    if (msgType === "assistant" || msgType === "status" || speaker === "Assistant") {
      setActiveSpeaker("Assistant");
      setMessages(prev => {
        const last = prev[prev.length - 1];
        if (last?.streaming) {
          return prev.map((m, i) => (i === prev.length - 1 ? { ...m, text: txt, streaming: false } : m));
        }
        return pushIfChanged(prev, { speaker: "Assistant", text: txt });
      });
      appendLog("🤖 Assistant responded");
      return;
    }

    // TOOLS
    if (type === "tool_start") {
      setMessages(prev => [...prev, { speaker: "Assistant", isTool: true, text: `🛠️ tool ${payload.tool || ''} started 🔄` }]);
      appendLog(`⚙️ ${payload.tool || 'tool'} started`);
      return;
    }
    if (type === "tool_progress") {
      setMessages(prev =>
        prev.map((m, i, arr) =>
          i === arr.length - 1 && typeof m.text === 'string' && m.text.startsWith(`🛠️ tool ${payload.tool || ''}`)
            ? { ...m, text: `🛠️ tool ${payload.tool || ''} ${payload.pct ?? '?'}% 🔄` }
            : m
        )
      );
      appendLog(`⚙️ ${payload.tool || 'tool'} ${payload.pct ?? '?'}%`);
      return;
    }
    if (type === "tool_end") {
      const ok = payload.status === "success";
      const finalText = ok
        ? `🛠️ tool ${payload.tool || ''} completed ✔️\n${JSON.stringify(payload.result, null, 2)}`
        : `🛠️ tool ${payload.tool || ''} failed ❌\n${payload.error || 'Unknown error'}`;
      setMessages(prev =>
        prev.map((m, i, arr) =>
          i === arr.length - 1 && typeof m.text === 'string' && m.text.startsWith(`🛠️ tool ${payload.tool || ''}`)
            ? { ...m, text: finalText }
            : m
        )
      );
      appendLog(`⚙️ ${payload.tool || 'tool'} ${payload.status} (${payload.elapsedMs ?? '?'} ms)`);
      return;
    }
  };

  const startACSCall = async () => {
    if (!/^\+\d+$/.test(targetPhoneNumber)) {
      alert("Enter phone in E.164 format e.g. +15551234567");
      return;
    }
    try {
      const res = await fetch(`${API_BASE}/api/v1/calls/initiate`, {
        method:"POST", headers:{"Content-Type":"application/json"},
        body: JSON.stringify({ target_number: targetPhoneNumber }),
      });
      const json = await res.json();
      if (!res.ok) {
        appendLog(`Call error: ${json?.detail || res.statusText}`);
        return;
      }
      setMessages(m => [...m, { speaker:"Assistant", text:`📞 Call started → ${targetPhoneNumber}` }]);
      appendLog("📞 Call initiated");

      // Relay WS for dashboard messages
      const relay = new WebSocket(`${WS_URL}/api/v1/realtime/dashboard/relay`);
      relay.onopen = () => appendLog("Relay WS connected");
      relay.onmessage = ({ data }) => {
        try {
          const obj = JSON.parse(data);
          if (obj.type?.startsWith("tool_")) {
            handleSocketMessage({ data: JSON.stringify(obj) });
            return;
          }
          const { sender, message } = obj;
          setMessages(m => [...m, { speaker: sender, text: message }]);
          setActiveSpeaker(sender);
          appendLog(`[Relay] ${sender}: ${message}`);
        } catch {
          appendLog("Relay parse error");
        }
      };
      relay.onclose = () => {
        appendLog("Relay WS disconnected");
        setCallActive(false);
        setActiveSpeaker(null);
      };
      setCallActive(true);
    } catch(e) {
      appendLog(`Network error starting call: ${e.message}`);
    }
  };

  return (
    <div style={styles.root}>
      <div style={styles.mainContainer}>
        <BackendIndicator url={API_BASE} />
        <DeveloperHub socketRef={socketRef} />

        <div style={styles.appHeader}>
          <div style={styles.appTitleContainer}>
            <div style={styles.appTitleWrapper}>
              <span style={styles.appTitleIcon}>🎙️</span>
              <h1 style={styles.appTitle}>RTAgent</h1>
            </div>
            <p style={styles.appSubtitle}>Transforming customer interactions with real-time, intelligent voice interactions</p>
          </div>
          <HelpButton />
        </div>

        <div style={styles.waveformSection}>
          <div style={styles.waveformSectionTitle}>Voice Activity</div>
          <WaveformVisualization speaker={activeSpeaker} audioLevel={audioLevel} outputAudioLevel={0} />
          <div style={styles.sectionDivider} />
        </div>

        <div style={styles.chatSection} ref={chatRef}>
          <div style={styles.chatSectionIndicator} />
          <div style={styles.messageContainer} ref={messageContainerRef}>
            {messages.map((message, index) => (<ChatBubble key={index} message={message} />))}
          </div>
        </div>

        <div style={styles.controlSection}>
          <div style={styles.controlContainer}>
            {/* Reset */}
            <div style={{ position: 'relative' }}>
              <button
                style={styles.resetButton(false, resetHovered)}
                onMouseEnter={() => { setShowResetTooltip(true); setResetHovered(true); }}
                onMouseLeave={() => { setShowResetTooltip(false); setResetHovered(false); }}
                onClick={() => {
                  setMessages([]); setActiveSpeaker(null);
                  stopRecognition(); setCallActive(false); setShowPhoneInput(false);
                  appendLog("🔄️ Session reset - starting fresh");
                  setTimeout(() => setMessages([{ speaker: "System", text: "✅ Session restarted. Ready for a new conversation!" }]), 200);
                }}
              >
                ⟲
              </button>
              <div style={{ ...styles.buttonTooltip, ...(showResetTooltip ? styles.buttonTooltipVisible : {}) }}>
                Reset conversation & start fresh
              </div>
            </div>

            {/* Mic */}
            <div style={{ position: 'relative' }}>
              <button
                style={styles.micButton(recording, micHovered)}
                onMouseEnter={() => { setShowMicTooltip(true); setMicHovered(true); }}
                onMouseLeave={() => { setShowMicTooltip(false); setMicHovered(false); }}
                onClick={recording ? stopRecognition : startRecognition}
              >
                {recording ? "🛑" : "🎤"}
              </button>
              <div style={{ ...styles.buttonTooltip, ...(showMicTooltip ? styles.buttonTooltipVisible : {}) }}>
                {recording ? "Stop recording your voice" : "Start voice conversation"}
              </div>
            </div>

            {/* Phone */}
            <div style={{ position: 'relative' }}>
              <button
                style={styles.phoneButton(callActive, phoneHovered)}
                onMouseEnter={() => { setShowPhoneTooltip(true); setPhoneHovered(true); }}
                onMouseLeave={() => { setShowPhoneTooltip(false); setPhoneHovered(false); }}
                onClick={() => {
                  if (callActive) {
                    stopRecognition();
                    setCallActive(false);
                    setMessages(prev => [...prev, { speaker: "System", text: "📞 Call ended" }]);
                  } else {
                    setShowPhoneInput(v => !v);
                  }
                }}
              >
                {callActive ? "📵" : "📞"}
              </button>
              <div style={{ ...styles.buttonTooltip, ...(showPhoneTooltip ? styles.buttonTooltipVisible : {}) }}>
                {callActive ? "Hang up the phone call" : "Make a phone call"}
              </div>
            </div>
          </div>
        </div>

        {showPhoneInput && (
          <div style={styles.phoneInputSection}>
            <div style={{ marginBottom: '8px', fontSize: '12px', color: '#64748b' }}>
              {callActive ? '📞 Call in progress' : '📞 Enter your phone number to get a call'}
            </div>
            <input
              type="tel"
              value={targetPhoneNumber}
              onChange={(e) => setTargetPhoneNumber(e.target.value)}
              placeholder="+15551234567"
              style={styles.phoneInput}
              disabled={callActive}
            />
            <button
              onClick={callActive ? stopRecognition : startACSCall}
              style={styles.callMeButton(callActive)}
              title={callActive ? "🔴 Hang up call" : "📞 Start phone call"}
            >
              {callActive ? "🔴 Hang Up" : "📞 Call Me"}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

function App() {
  return <RealTimeVoiceApp />;
}
export default App;
