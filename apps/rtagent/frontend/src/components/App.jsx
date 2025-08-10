// src/RealTimeVoiceApp.jsx
import React, { useEffect, useRef, useState } from 'react';
import "reactflow/dist/style.css";
// import { useHealthMonitor } from "./hooks/useHealthMonitor";
// import HealthStatusIndicator from "./components/HealthStatusIndicator";

/* ------------------------------------------------------------------ *
 *  ENV VARS
 * ------------------------------------------------------------------ */
const {
  VITE_BACKEND_BASE_URL: API_BASE_URL,
} = import.meta.env;

const WS_URL = API_BASE_URL.replace(/^https?/, "wss");

/* ------------------------------------------------------------------ *
 *  STYLES
 * ------------------------------------------------------------------ */
const styles = {
  root: {
    width: "768px",
    maxWidth: "768px", // Expanded from iPad width
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
  
  // Main iPad-sized container
  mainContainer: {
    width: "100%",
    maxWidth: "100%", // Expanded from iPad width
    height: "90vh",
    maxHeight: "900px", // Adjusted height
    background: "white",
    borderRadius: "20px",
    boxShadow: "0 20px 60px rgba(0,0,0,0.15)",
    border: "0px solid #ce1010ff",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
  },

  // App header with title - more blended approach  
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

  appTitleContainer: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    gap: "4px",
  },

  appTitleWrapper: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
  },

  appTitleIcon: {
    fontSize: "20px",
    opacity: 0.7,
  },

  appTitle: {
    fontSize: "18px",
    fontWeight: "600",
    color: "#334155",
    textAlign: "center",
    margin: 0,
    letterSpacing: "0.1px",
  },

  appSubtitle: {
    fontSize: "12px",
    fontWeight: "400",
    color: "#64748b",
    textAlign: "center",
    margin: 0,
    letterSpacing: "0.1px",
    maxWidth: "350px",
    lineHeight: "1.3",
    opacity: 0.8,
  },
  
  // Waveform section - blended design
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
  
  waveformSectionTitle: {
    fontSize: "12px",
    fontWeight: "500",
    color: "#64748b",
    textTransform: "uppercase",
    letterSpacing: "0.5px",
    marginBottom: "8px",
    opacity: 0.8,
  },
  
  // Section divider line - more subtle
  sectionDivider: {
    position: "absolute",
    bottom: "-1px",
    left: "20%",
    right: "20%",
    height: "1px",
    backgroundColor: "#cbd5e1",
    borderRadius: "0.5px",
    opacity: 0.6,
  },
  
  waveformContainer: {
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    width: "100%",
    height: "60%",
    padding: "0 10px",
    background: "radial-gradient(ellipse at center, rgba(100, 116, 139, 0.05) 0%, transparent 70%)",
    borderRadius: "6px",
  },
  
  waveformSvg: {
    width: "100%",
    height: "60px",
    filter: "drop-shadow(0 1px 2px rgba(100, 116, 139, 0.1))",
    transition: "filter 0.3s ease",
  },
  
  // Chat section (middle section)
  chatSection: {
    flex: 1,
    padding: "15px 20px 15px 5px", // Remove most left padding, keep right padding
    width: "100%",
    overflowY: "auto",
    backgroundColor: "#ffffff",
    borderBottom: "1px solid #e2e8f0",
    display: "flex",
    flexDirection: "column",
    position: "relative",
  },
  
  chatSectionHeader: {
    textAlign: "center",
    marginBottom: "30px",
    paddingBottom: "20px",
    borderBottom: "1px solid #f1f5f9",
  },
  
  chatSectionTitle: {
    fontSize: "14px",
    fontWeight: "600",
    color: "#64748b",
    textTransform: "uppercase",
    letterSpacing: "0.5px",
    marginBottom: "5px",
  },
  
  chatSectionSubtitle: {
    fontSize: "12px",
    color: "#94a3b8",
    fontStyle: "italic",
  },
  
  // Chat section visual indicator
  chatSectionIndicator: {
    position: "absolute",
    left: "0",
    top: "0",
    bottom: "0",
    width: "0px", // Removed blue border
    backgroundColor: "#3b82f6",
  },
  
  messageContainer: {
    display: "flex",
    flexDirection: "column",
    gap: "16px",
    flex: 1,
    overflowY: "auto",
    padding: "0", // Remove all padding for maximum space usage
  },
  
  // User message (right aligned - blue bubble)
  userMessage: {
    alignSelf: "flex-end",
    maxWidth: "75%", // More conservative width
    marginRight: "15px", // Increased margin for more right padding
    marginBottom: "4px",
  },
  
  userBubble: {
    background: "#e0f2fe",
    color: "#0f172a",
    padding: "12px 16px",
    borderRadius: "20px",
    fontSize: "14px",
    lineHeight: "1.5",
    border: "1px solid #bae6fd",
    boxShadow: "0 2px 8px rgba(14,165,233,0.15)",
    wordWrap: "break-word",
    overflowWrap: "break-word",
    hyphens: "auto",
    whiteSpace: "pre-wrap",
  },
  
  // Assistant message (left aligned - teal bubble)
  assistantMessage: {
    alignSelf: "flex-start",
    maxWidth: "80%", // Increased width for maximum space usage
    marginLeft: "0px", // No left margin - flush to edge
    marginBottom: "4px",
  },
  
  assistantBubble: {
    background: "#67d8ef",
    color: "white",
    padding: "12px 16px",
    borderRadius: "20px",
    fontSize: "14px",
    lineHeight: "1.5",
    boxShadow: "0 2px 8px rgba(103,216,239,0.3)",
    wordWrap: "break-word",
    overflowWrap: "break-word",
    hyphens: "auto",
    whiteSpace: "pre-wrap",
  },
  
  // Control section - blended footer design
  controlSection: {
    padding: "12px",
    backgroundColor: "#f1f5f9",
    background: "linear-gradient(180deg, #f1f5f9 0%, #e2e8f0 100%)",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    height: "15%",
    minHeight: "100px",
    borderTop: "1px solid #e2e8f0",
    position: "relative",
  },
  
  controlContainer: {
    display: "flex",
    gap: "8px",
    background: "white",
    padding: "12px 16px",
    borderRadius: "24px",
    boxShadow: "0 4px 16px rgba(100, 116, 139, 0.08), 0 1px 4px rgba(100, 116, 139, 0.04)",
    border: "1px solid #e2e8f0",
    width: "fit-content",
  },
  
  controlButton: (isActive, variant = 'default') => ({
    width: "56px",
    height: "56px",
    borderRadius: "50%",
    border: "none",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    cursor: "pointer",
    fontSize: "20px",
    transition: "all 0.2s ease",
    background: variant === 'phone' ? "#67d8ef" : 
                variant === 'close' ? "#f1f5f9" :
                isActive ? "#67d8ef" : "#f1f5f9",
    color: variant === 'phone' || isActive ? "white" : "#64748b",
    transform: isActive ? "scale(1.05)" : "scale(1)",
    boxShadow: isActive ? "0 4px 16px rgba(103,216,239,0.4)" : "0 2px 8px rgba(0,0,0,0.05)",
  }),
  
  // Input section for phone calls
  phoneInputSection: {
    position: "absolute",
    bottom: "60px", // Moved lower from 140px to 60px to avoid blocking chat bubbles
    left: "500px", // Moved further to the right from 400px to 500px
    background: "white",
    padding: "20px",
    borderRadius: "16px",
    boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
    border: "1px solid #e2e8f0",
    display: "flex",
    flexDirection: "column",
    gap: "12px",
    minWidth: "240px",
    zIndex: 90,
  },
  
  phoneInput: {
    padding: "12px 16px",
    border: "1px solid #d1d5db",
    borderRadius: "8px",
    fontSize: "14px",
    outline: "none",
    transition: "border-color 0.2s ease",
  },
  

  // Backend status indicator - enhanced for component health - relocated to bottom left
  backendIndicator: {
    position: "fixed",
    bottom: "20px",
    left: "20px",
    display: "flex",
    flexDirection: "column",
    gap: "8px",
    padding: "12px 16px",
    backgroundColor: "rgba(255, 255, 255, 0.98)",
    border: "1px solid #e2e8f0",
    borderRadius: "12px",
    fontSize: "11px",
    color: "#64748b",
    boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
    zIndex: 1000,
    minWidth: "280px",
    maxWidth: "320px",
    backdropFilter: "blur(8px)",
  },

  backendHeader: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    marginBottom: "4px",
    cursor: "pointer",
  },

  backendStatus: {
    width: "8px",
    height: "8px",
    borderRadius: "50%",
    backgroundColor: "#10b981",
    animation: "pulse 2s ease-in-out infinite",
    flexShrink: 0,
  },

  backendUrl: {
    fontFamily: "monospace",
    fontSize: "10px",
    color: "#475569",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },

  backendLabel: {
    fontWeight: "600",
    color: "#334155",
    fontSize: "12px",
    letterSpacing: "0.3px",
  },

  expandIcon: {
    marginLeft: "auto",
    fontSize: "12px",
    color: "#94a3b8",
    transition: "transform 0.2s ease",
  },

  componentGrid: {
    display: "grid",
    gridTemplateColumns: "1fr",
    gap: "8px",
    marginTop: "8px",
    paddingTop: "8px",
    borderTop: "1px solid #f1f5f9",
  },

  componentItem: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    padding: "6px 10px",
    backgroundColor: "#f8fafc",
    borderRadius: "8px",
    fontSize: "10px",
    border: "1px solid #f1f5f9",
    transition: "all 0.2s ease",
  },

  componentDot: (status) => ({
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    backgroundColor: status === "healthy" ? "#10b981" : 
                     status === "degraded" ? "#f59e0b" : 
                     status === "unhealthy" ? "#ef4444" : "#6b7280",
    flexShrink: 0,
  }),

  componentName: {
    fontWeight: "500",
    color: "#475569",
    textTransform: "capitalize",
    whiteSpace: "nowrap",
    overflow: "hidden",
    textOverflow: "ellipsis",
  },

  responseTime: {
    fontSize: "9px",
    color: "#94a3b8",
    marginLeft: "auto",
  },

  errorMessage: {
    fontSize: "10px",
    color: "#ef4444",
    marginTop: "4px",
    fontStyle: "italic",
  },

  phoneButton: (isActive) => ({
    padding: "12px 20px",
    background: isActive ? "#ef4444" : "#67d8ef",
    color: "white",
    border: "none",
    borderRadius: "8px",
    cursor: "pointer",
    fontSize: "14px",
    fontWeight: "600",
    transition: "all 0.2s ease",
  }),
};
// Add keyframe animation for pulse effect
const styleSheet = document.createElement("style");
styleSheet.textContent = `
  @keyframes pulse {
    0% {
      box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4);
    }
    70% {
      box-shadow: 0 0 0 6px rgba(16, 185, 129, 0);
    }
    100% {
      box-shadow: 0 0 0 0 rgba(16, 185, 129, 0);
    }
  }
`;
document.head.appendChild(styleSheet);

/* ------------------------------------------------------------------ *
 *  BACKEND STATUS COMPONENT
 * ------------------------------------------------------------------ */
const BackendIndicator = ({ url }) => {
  const [isConnected, setIsConnected] = useState(null);
  const [displayUrl, setDisplayUrl] = useState(url);
  const [readinessData, setReadinessData] = useState(null);
  const [error, setError] = useState(null);
  const [isExpanded, setIsExpanded] = useState(false);

  // Check readiness endpoint
  const checkReadiness = async () => {
    try {
      // Simple GET request without extra headers
      const response = await fetch(`${url}/readiness`);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();
      
      // Validate expected structure
      if (data.status && data.checks && Array.isArray(data.checks)) {
        setReadinessData(data);
        setIsConnected(data.status === "ready");
        setError(null);
      } else {
        throw new Error("Invalid response structure");
      }
    } catch (err) {
      console.error("Readiness check failed:", err);
      setIsConnected(false);
      setError(err.message);
      setReadinessData(null);
    }
  };

  useEffect(() => {
    // Parse and format the URL for display
    try {
      const urlObj = new URL(url);
      const host = urlObj.hostname;
      const protocol = urlObj.protocol.replace(':', '');
      
      // Shorten Azure URLs
      if (host.includes('.azurewebsites.net')) {
        const appName = host.split('.')[0];
        setDisplayUrl(`${protocol}://${appName}.azure...`);
      } else if (host === 'localhost') {
        setDisplayUrl(`${protocol}://localhost:${urlObj.port || '8000'}`);
      } else {
        setDisplayUrl(`${protocol}://${host}`);
      }
    } catch (e) {
      setDisplayUrl(url);
    }

    // Initial check
    checkReadiness();

    // Set up periodic checks every 30 seconds
    const interval = setInterval(checkReadiness, 30000);

    return () => clearInterval(interval);
  }, [url]);

  // Get overall health status
  const getOverallStatus = () => {
    if (isConnected === null) return "checking";
    if (!isConnected) return "unhealthy";
    if (!readinessData?.checks) return "unhealthy";
    
    const hasUnhealthy = readinessData.checks.some(c => c.status === "unhealthy");
    const hasDegraded = readinessData.checks.some(c => c.status === "degraded");
    
    if (hasUnhealthy) return "unhealthy";
    if (hasDegraded) return "degraded";
    return "healthy";
  };

  const overallStatus = getOverallStatus();
  const statusColor = overallStatus === "healthy" ? "#10b981" : 
                     overallStatus === "degraded" ? "#f59e0b" :
                     overallStatus === "unhealthy" ? "#ef4444" : "#6b7280";

  // Component icon mapping with descriptions
  const componentIcons = {
    redis: "💾",
    azure_openai: "🧠",
    speech_services: "🎙️",
    acs_caller: "📞",
    rt_agents: "🤖"
  };

  // Component descriptions
  const componentDescriptions = {
    redis: "Redis Cache - Session & state management",
    azure_openai: "Azure OpenAI - GPT models & embeddings",
    speech_services: "Speech Services - STT/TTS processing",
    acs_caller: "Communication Services - Voice calling",
    rt_agents: "RT Agents - Real-time Voice Agents"
  };

  return (
    <div 
      style={{
        ...styles.backendIndicator,
        // Minimize when not expanded and healthy
        minWidth: !isExpanded && overallStatus === "healthy" ? "200px" : "320px",
        padding: !isExpanded && overallStatus === "healthy" ? "10px 14px" : "12px 16px",
        transition: "all 0.3s ease",
      }} 
      title={`Click to expand backend status`}
      onClick={() => setIsExpanded(!isExpanded)}
      onMouseEnter={() => !isExpanded && setIsExpanded(true)}
      onMouseLeave={() => setIsExpanded(false)}
    >
      <div style={styles.backendHeader}>
        <div style={{
          ...styles.backendStatus,
          backgroundColor: statusColor,
        }}></div>
        <span style={styles.backendLabel}>Backend Status</span>
        <span style={{
          ...styles.expandIcon,
          transform: isExpanded ? "rotate(180deg)" : "rotate(0deg)",
        }}>▼</span>
      </div>
      
      {/* Compact URL display when collapsed */}
      {!isExpanded && (
        <div style={{
          ...styles.backendUrl,
          fontSize: "9px",
          opacity: 0.7,
          marginTop: "2px",
        }}>
          {displayUrl}
        </div>
      )}

      {/* Only show component health when expanded or when there's an issue */}
      {(isExpanded || overallStatus !== "healthy") && (
        <>
          {/* Expanded information display */}
          {isExpanded && (
            <>
              
              {/* API Entry Point Info */}
              <div style={{
                padding: "8px 10px",
                backgroundColor: "#f8fafc",
                borderRadius: "8px",
                marginBottom: "10px",
                fontSize: "10px",
                border: "1px solid #e2e8f0",
              }}>
                <div style={{
                  fontWeight: "600",
                  color: "#475569",
                  marginBottom: "4px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px",
                }}>
                  🌐 Backend API Entry Point
                </div>
                <div style={{
                  color: "#64748b",
                  fontSize: "9px",
                  fontFamily: "monospace",
                  marginBottom: "6px",
                  padding: "3px 6px",
                  backgroundColor: "white",
                  borderRadius: "4px",
                  border: "1px solid #f1f5f9",
                }}>
                  {url}
                </div>
                <div style={{
                  color: "#64748b",
                  fontSize: "9px",
                  lineHeight: "1.3",
                }}>
                  Main FastAPI server handling WebSocket connections, voice processing, and AI agent orchestration
                </div>
              </div>

              {/* System status summary */}
              {readinessData && (
                <div style={{
                  padding: "6px 8px",
                  backgroundColor: overallStatus === "healthy" ? "#f0fdf4" : 
                                 overallStatus === "degraded" ? "#fffbeb" : "#fef2f2",
                  borderRadius: "6px",
                  marginBottom: "8px",
                  fontSize: "10px",
                  border: `1px solid ${overallStatus === "healthy" ? "#bbf7d0" : 
                                      overallStatus === "degraded" ? "#fed7aa" : "#fecaca"}`,
                }}>
                  <div style={{
                    fontWeight: "600",
                    color: overallStatus === "healthy" ? "#166534" : 
                          overallStatus === "degraded" ? "#92400e" : "#dc2626",
                    marginBottom: "2px",
                  }}>
                    System Status: {overallStatus.charAt(0).toUpperCase() + overallStatus.slice(1)}
                  </div>
                  <div style={{
                    color: "#64748b",
                    fontSize: "9px",
                  }}>
                    {readinessData.checks.length} components monitored • 
                    Last check: {new Date().toLocaleTimeString()}
                  </div>
                </div>
              )}
            </>
          )}

          {error ? (
            <div style={styles.errorMessage}>
              ⚠️ Connection failed: {error}
            </div>
          ) : readinessData?.checks ? (
            <>
              <div style={styles.componentGrid}>
                {readinessData.checks.map((check, idx) => (
                  <div 
                    key={idx} 
                    style={{
                      ...styles.componentItem,
                      flexDirection: "column",
                      alignItems: "flex-start",
                      padding: "8px 10px",
                    }}
                    title={check.details || `${check.component} status: ${check.status}`}
                  >
                    <div style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "6px",
                      width: "100%",
                    }}>
                      <span>{componentIcons[check.component] || "•"}</span>
                      <div style={styles.componentDot(check.status)}></div>
                      <span style={styles.componentName}>
                        {check.component.replace(/_/g, ' ')}
                      </span>
                      {check.check_time_ms !== undefined && (
                        <span style={styles.responseTime}>
                          {check.check_time_ms.toFixed(0)}ms
                        </span>
                      )}
                    </div>
                    
                    {/* Component description when expanded */}
                    {isExpanded && (
                      <div style={{
                        fontSize: "9px",
                        color: "#64748b",
                        marginTop: "4px",
                        lineHeight: "1.3",
                        fontStyle: "italic",
                      }}>
                        {componentDescriptions[check.component] || "Backend service component"}
                      </div>
                    )}
                    
                    {/* Status details when expanded */}
                    {isExpanded && check.details && (
                      <div style={{
                        fontSize: "9px",
                        color: check.status === "healthy" ? "#10b981" : 
                              check.status === "degraded" ? "#f59e0b" : "#ef4444",
                        marginTop: "2px",
                        fontWeight: "500",
                      }}>
                        {check.details}
                      </div>
                    )}
                  </div>
                ))}
              </div>
              
              {/* Show component details if expanded */}
              {isExpanded && readinessData.checks.some(c => c.details) && (
                <div style={{
                  marginTop: "8px",
                  paddingTop: "8px",
                  borderTop: "1px solid #f1f5f9",
                  fontSize: "9px",
                  color: "#64748b",
                }}>
                  {readinessData.checks
                    .filter(c => c.details)
                    .map((check, idx) => (
                      <div key={idx} style={{ marginBottom: "4px" }}>
                        <strong>{check.component.replace(/_/g, ' ')}:</strong> {check.details}
                      </div>
                    ))}
                </div>
              )}
            </>
          ) : (
            <div style={styles.errorMessage}>
              Checking components...
            </div>
          )}
          
          {readinessData?.response_time_ms && isExpanded && (
            <div style={{
              fontSize: "9px",
              color: "#94a3b8",
              marginTop: "8px",
              paddingTop: "8px",
              borderTop: "1px solid #f1f5f9",
              textAlign: "center",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}>
              <span>Health check latency: {readinessData.response_time_ms.toFixed(0)}ms</span>
              <span title="Auto-refreshes every 30 seconds">🔄</span>
            </div>
          )}
        </>
      )}
    </div>
  );
};
/* ------------------------------------------------------------------ *
 *  WAVEFORM COMPONENT - SIMPLE & SMOOTH
 * ------------------------------------------------------------------ */
const WaveformVisualization = ({ speaker, audioLevel = 0, outputAudioLevel = 0 }) => {
  const [waveOffset, setWaveOffset] = useState(0);
  const [amplitude, setAmplitude] = useState(5);
  const animationRef = useRef();
  
  useEffect(() => {
    const animate = () => {
      setWaveOffset(prev => (prev + (speaker ? 2 : 1)) % 1000);
      
      setAmplitude(() => {
        // React to actual audio levels first, then fall back to speaker state
        if (audioLevel > 0.01) {
          // User is speaking - use real audio level
          const scaledLevel = audioLevel * 25;
          const smoothVariation = Math.sin(Date.now() * 0.002) * (scaledLevel * 0.2);
          return Math.max(8, scaledLevel + smoothVariation);
        } else if (outputAudioLevel > 0.01) {
          // Assistant is speaking - use output audio level
          const scaledLevel = outputAudioLevel * 20;
          const smoothVariation = Math.sin(Date.now() * 0.0018) * (scaledLevel * 0.25);
          return Math.max(6, scaledLevel + smoothVariation);
        } else if (speaker) {
          // Active speaking fallback - gentle rhythmic movement
          const time = Date.now() * 0.002;
          const baseAmplitude = 10;
          const rhythmicVariation = Math.sin(time) * 5;
          return baseAmplitude + rhythmicVariation;
        } else {
          // Idle state - gentle breathing pattern
          const time = Date.now() * 0.0008;
          const breathingAmplitude = 3 + Math.sin(time) * 1.5;
          return breathingAmplitude;
        }
      });
      
      animationRef.current = requestAnimationFrame(animate);
    };
    
    animationRef.current = requestAnimationFrame(animate);
    
    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [speaker, audioLevel, outputAudioLevel]);
  
  // Simple wave path generation
  const generateWavePath = () => {
    const width = 750;
    const height = 100;
    const centerY = height / 2;
    const frequency = 0.02;
    const points = 100; // Reduced points for better performance
    
    let path = `M 0 ${centerY}`;
    
    for (let i = 0; i <= points; i++) {
      const x = (i / points) * width;
      const y = centerY + Math.sin((x * frequency + waveOffset * 0.1)) * amplitude;
      path += ` L ${x} ${y}`;
    }
    
    return path;
  };

  // Secondary wave
  const generateSecondaryWave = () => {
    const width = 750;
    const height = 100;
    const centerY = height / 2;
    const frequency = 0.025;
    const points = 100;
    
    let path = `M 0 ${centerY}`;
    
    for (let i = 0; i <= points; i++) {
      const x = (i / points) * width;
      const y = centerY + Math.sin((x * frequency + waveOffset * 0.12)) * (amplitude * 0.6);
      path += ` L ${x} ${y}`;
    }
    
    return path;
  };

  // Wave rendering
  const generateMultipleWaves = () => {
    const waves = [];
    
    let baseColor, opacity;
    if (speaker === "User") {
      baseColor = "#ef4444";
      opacity = 0.8;
    } else if (speaker === "Assistant") {
      baseColor = "#67d8ef";
      opacity = 0.8;
    } else {
      baseColor = "#3b82f6";
      opacity = 0.4;
    }
    
    // Main wave
    waves.push(
      <path
        key="wave1"
        d={generateWavePath()}
        stroke={baseColor}
        strokeWidth={speaker ? "3" : "2"}
        fill="none"
        opacity={opacity}
        strokeLinecap="round"
      />
    );
    
    // Secondary wave
    waves.push(
      <path
        key="wave2"
        d={generateSecondaryWave()}
        stroke={baseColor}
        strokeWidth={speaker ? "2" : "1.5"}
        fill="none"
        opacity={opacity * 0.5}
        strokeLinecap="round"
      />
    );
    
    return waves;
  };
  
  return (
    <div style={styles.waveformContainer}>
      <svg style={styles.waveformSvg} viewBox="0 0 750 80" preserveAspectRatio="xMidYMid meet">
        {generateMultipleWaves()}
      </svg>
      
      {/* Audio level indicators for debugging */}
      {window.location.hostname === 'localhost' && (
        <div style={{
          position: 'absolute',
          bottom: '-25px',
          left: '50%',
          transform: 'translateX(-50%)',
          fontSize: '10px',
          color: '#666',
          whiteSpace: 'nowrap'
        }}>
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
  
  if (isTool) {
    return (
      <div style={{ ...styles.assistantMessage, alignSelf: "center" }}>
        <div style={{
          ...styles.assistantBubble,
          background: "#8b5cf6",
          textAlign: "center",
          fontSize: "14px",
        }}>
          {text}
        </div>
      </div>
    );
  }
  
  return (
    <div style={isUser ? styles.userMessage : styles.assistantMessage}>
      <div style={isUser ? styles.userBubble : styles.assistantBubble}>
        {text.split("\n").map((line, i) => (
          <div key={i}>{line}</div>
        ))}
        {streaming && <span style={{ opacity: 0.7 }}>▌</span>}
      </div>
    </div>
  );
};

/* ------------------------------------------------------------------ *
 *  MAIN COMPONENT
 * ------------------------------------------------------------------ */
export default function RealTimeVoiceApp() {
  /* ---------- state ---------- */
  const [messages, setMessages] = useState([
    // { speaker: "User", text: "Hello, I need help with my insurance claim." },
    // { speaker: "Assistant", text: "I'd be happy to help you with your insurance claim. Can you please provide me with your policy number?" }
  ]);
  const [log, setLog]                 = useState("");
  const [recording, setRecording]     = useState(false);
  const [targetPhoneNumber, setTargetPhoneNumber] = useState("");
  const [callActive, setCallActive]   = useState(false);
  const [activeSpeaker, setActiveSpeaker] = useState(null);
  const [showPhoneInput, setShowPhoneInput] = useState(false);

  // /* ---------- health monitoring ---------- */
  // const { 
  //   healthStatus = { isHealthy: null, lastChecked: null, responseTime: null, error: null },
  //   readinessStatus = { status: null, timestamp: null, responseTime: null, checks: [], lastChecked: null, error: null },
  //   overallStatus = { isHealthy: false, hasWarnings: false, criticalErrors: [] },
  //   refresh = () => {} 
  // } = useHealthMonitor({
  //   baseUrl: API_BASE_URL,
  //   healthInterval: 30000,
  //   readinessInterval: 15000,
  //   enableAutoRefresh: true,
  // });


  // Function call state (not mind-map)
  // const [functionCalls, setFunctionCalls] = useState([]);
  // const [callResetKey, setCallResetKey]   = useState(0);

  /* ---------- refs ---------- */
  const chatRef      = useRef(null);
  const messageContainerRef = useRef(null);
  const socketRef    = useRef(null);
  // const recognizerRef= useRef(null);

  // Fix: missing refs for audio and processor
  const audioContextRef = useRef(null);
  const processorRef = useRef(null);
  const analyserRef = useRef(null);
  const micStreamRef = useRef(null);
  
  // Audio level tracking for reactive waveforms
  const [audioLevel, setAudioLevel] = useState(0);
  // const [outputAudioLevel, setOutputAudioLevel] = useState(0);
  const audioLevelRef = useRef(0);
  // const outputAudioLevelRef = useRef(0);



  const appendLog = m => setLog(p => `${p}\n${new Date().toLocaleTimeString()} - ${m}`);

  /* ---------- scroll chat on new message ---------- */
  useEffect(()=>{
    // Try both refs to ensure scrolling works
    if(messageContainerRef.current) {
      messageContainerRef.current.scrollTo({
        top: messageContainerRef.current.scrollHeight,
        behavior: 'smooth'
      });
    } else if(chatRef.current) {
      chatRef.current.scrollTo({
        top: chatRef.current.scrollHeight,
        behavior: 'smooth'
      });
    }
  },[messages]);

  /* ---------- teardown on unmount ---------- */
  useEffect(() => {
    return () => {
      if (processorRef.current) {
        try { 
          processorRef.current.disconnect(); 
        } catch (e) {
          console.warn("Cleanup error:", e);
        }
      }
      if (audioContextRef.current) {
        try { 
          audioContextRef.current.close(); 
        } catch (e) {
          console.warn("Cleanup error:", e);
        }
      }
      if (socketRef.current) {
        try { 
          socketRef.current.close(); 
        } catch (e) {
          console.warn("Cleanup error:", e);
        }
      }
    };
  }, []);

  /* ---------- derive callActive from logs ---------- */
  useEffect(()=>{
    if (log.includes("Call connected"))  setCallActive(true);
    if (log.includes("Call ended"))      setCallActive(false);
  },[log]);
  /* ------------------------------------------------------------------ *
   *  START RECOGNITION + WS
   * ------------------------------------------------------------------ */
  const startRecognition = async () => {
      // mind-map reset not needed
      setMessages([]);
      appendLog("🎤 PCM streaming started");

      // 1) open WS
      const socket = new WebSocket(`${WS_URL}/realtime`);
      socket.binaryType = "arraybuffer";

      socket.onopen = () => {
        appendLog("🔌 WS open");
        console.log("WebSocket connection OPENED to backend!");
      };
      socket.onclose = () => {
        console.log("WebSocket connection CLOSED.");
      };
      socket.onerror = (err) => {
        console.error("WebSocket error:", err);
      };
      socket.onmessage = handleSocketMessage;
      socketRef.current = socket;

      // 2) setup Web Audio for raw PCM @16 kHz
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      micStreamRef.current = stream;
      const audioCtx = new (window.AudioContext || window.webkitAudioContext)({
        sampleRate: 16000
      });
      audioContextRef.current = audioCtx;

      const source = audioCtx.createMediaStreamSource(stream);

      // Add analyser for real-time audio level monitoring
      const analyser = audioCtx.createAnalyser();
      analyser.fftSize = 256;
      analyser.smoothingTimeConstant = 0.3;
      analyserRef.current = analyser;
      
      // Connect source to analyser
      source.connect(analyser);

      // 3) ScriptProcessor with small buffer for low latency (256 or 512 samples)
      const bufferSize = 512; 
      const processor  = audioCtx.createScriptProcessor(bufferSize, 1, 1);
      processorRef.current = processor;

      // Connect analyser to processor for audio data flow
      analyser.connect(processor);

      processor.onaudioprocess = (evt) => {
        const float32 = evt.inputBuffer.getChannelData(0);
        
        // Calculate real-time audio level
        let sum = 0;
        for (let i = 0; i < float32.length; i++) {
          sum += float32[i] * float32[i];
        }
        const rms = Math.sqrt(sum / float32.length);
        const level = Math.min(1, rms * 10); // Scale and clamp to 0-1
        
        audioLevelRef.current = level;
        setAudioLevel(level);

        // Debug: Log a sample of mic data
        console.log("Mic data sample:", float32.slice(0, 10)); // Should show non-zero values if your mic is hot

        const int16 = new Int16Array(float32.length);
        for (let i = 0; i < float32.length; i++) {
          int16[i] = Math.max(-1, Math.min(1, float32[i])) * 0x7fff;
        }

        // Debug: Show size before send
        console.log("Sending int16 PCM buffer, length:", int16.length);

        if (socket.readyState === WebSocket.OPEN) {
          socket.send(int16.buffer);
          // Debug: Confirm data sent
          console.log("PCM audio chunk sent to backend!");
        } else {
          console.log("WebSocket not open, did not send audio.");
        }
      };

      source.connect(processor);
      processor.connect(audioCtx.destination);
      setRecording(true);
    };

    const stopRecognition = () => {
      if (processorRef.current) {
        try { 
          processorRef.current.disconnect(); 
        } catch (e) {
          console.warn("Error disconnecting processor:", e);
        }
        processorRef.current = null;
      }
      if (audioContextRef.current) {
        try { 
          audioContextRef.current.close(); 
        } catch (e) {
          console.warn("Error closing audio context:", e);
        }
        audioContextRef.current = null;
      }
      if (socketRef.current) {
        try { 
          socketRef.current.close(); 
        } catch (e) {
          console.warn("Error closing socket:", e);
        }
        socketRef.current = null;
      }
      setRecording(false);
      appendLog("🛑 PCM streaming stopped");
    };

    // Helper to dedupe consecutive identical messages
    const pushIfChanged = (arr, msg) => {
      // Only dedupe if the last message is from the same speaker and has the same text
      if (arr.length === 0) return [...arr, msg];
      const last = arr[arr.length - 1];
      if (last.speaker === msg.speaker && last.text === msg.text) return arr;
      return [...arr, msg];
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
      // --- Handle relay/broadcast messages with {sender, message} ---
      if (payload.sender && payload.message) {
        // Route all relay messages through the same logic
        payload.speaker = payload.sender;
        payload.content = payload.message;
        // fall through to unified logic below
      }
      const { type, content = "", message = "", speaker } = payload;
      const txt = content || message;
      const msgType = (type || "").toLowerCase();

      /* ---------- USER BRANCH ---------- */
      if (msgType === "user" || speaker === "User") {
        setActiveSpeaker("User");
        // Always append user message immediately, do not dedupe
        setMessages(prev => [...prev, { speaker: "User", text: txt }]);

        appendLog(`User: ${txt}`);
        return;
      }

      /* ---------- ASSISTANT STREAM ---------- */
      if (type === "assistant_streaming") {
        setActiveSpeaker("Assistant");
        setMessages(prev => {
          if (prev.at(-1)?.streaming) {
            return prev.map((m,i)=> i===prev.length-1 ? {...m, text:txt} : m);
          }
          return [...prev, { speaker:"Assistant", text:txt, streaming:true }];
        });
        return;
      }

      /* ---------- ASSISTANT FINAL ---------- */
      if (msgType === "assistant" || msgType === "status" || speaker === "Assistant") {
        setActiveSpeaker("Assistant");
        setMessages(prev => {
          if (prev.at(-1)?.streaming) {
            return prev.map((m,i)=> i===prev.length-1 ? {...m, text:txt, streaming:false} : m);
          }
          return pushIfChanged(prev, { speaker:"Assistant", text:txt });
        });

        appendLog("🤖 Assistant responded");
        return;
      }
    
      if (type === "tool_start") {

      
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
   *  OUTBOUND ACS CALL
   * ------------------------------------------------------------------ */
  const startACSCall = async () => {
    if (!/^\+\d+$/.test(targetPhoneNumber)) {
      alert("Enter phone in E.164 format e.g. +15551234567");
      return;
    }
    try {
      const res = await fetch(`${API_BASE_URL}/api/call/initiate`, {
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body: JSON.stringify({ target_number: targetPhoneNumber }),
      });
      const json = await res.json();
      if (!res.ok) {
        appendLog(`Call error: ${json.detail||res.statusText}`);
        return;
      }
      // show in chat
      setMessages(m => [
        ...m,
        { speaker:"Assistant", text:`📞 Call started → ${targetPhoneNumber}` }
      ]);
      appendLog("📞 Call initiated");

      // relay WS
      const relay = new WebSocket(`${WS_URL}/ws/relay`);
      relay.onopen = () => appendLog("Relay WS connected");
      relay.onmessage = ({data}) => {
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
        // setFunctionCalls([]);
        // setCallResetKey(k=>k+1);
      };
    } catch(e) {
      appendLog(`Network error starting call: ${e.message}`);
    }
  };

  /* ------------------------------------------------------------------ *
   *  RENDER
   * ------------------------------------------------------------------ */
  return (
    <div style={styles.root}>
      <div style={styles.mainContainer}>
        {/* Backend Status Indicator */}
        <BackendIndicator url={API_BASE_URL} />

        {/* App Header */}
        <div style={styles.appHeader}>
          <div style={styles.appTitleContainer}>
            <div style={styles.appTitleWrapper}>
              <span style={styles.appTitleIcon}>🎙️</span>
              <h1 style={styles.appTitle}>RTAgent</h1>
            </div>
            <p style={styles.appSubtitle}>Transforming customer interactions with real-time, intelligent voice interactions</p>
          </div>
        </div>

        {/* Waveform Section */}
        <div style={styles.waveformSection}>
          <div style={styles.waveformSectionTitle}>Voice Activity</div>
          <WaveformVisualization 
            isActive={recording} 
            speaker={activeSpeaker} 
            audioLevel={audioLevel}
            outputAudioLevel={0}
          />
          <div style={styles.sectionDivider}></div>
        </div>

        {/* Chat Messages */}
        <div style={styles.chatSection} ref={chatRef}>
          <div style={styles.chatSectionIndicator}></div>
          <div style={styles.messageContainer} ref={messageContainerRef}>
            {messages.map((message, index) => (
              <ChatBubble key={index} message={message} />
            ))}
          </div>
        </div>

        {/* Control Buttons */}
        <div style={styles.controlSection}>
          <div style={styles.controlContainer}>
            {/* Microphone Button */}
            <button
              style={styles.controlButton(recording)}
              onClick={recording ? stopRecognition : startRecognition}
              title={recording ? "Stop Recording" : "Start Recording"}
            >
              🎤
            </button>
            
            {/* Phone Call Button */}
            <button
              style={styles.controlButton(false, 'phone')}
              onClick={() => setShowPhoneInput(!showPhoneInput)}
              title="Phone Call"
            >
              📞
            </button>
            
            {/* Close Button */}
            <button
              style={styles.controlButton(false, 'close')}
              onClick={stopRecognition}
              title="End Session"
            >
              ✕
            </button>
          </div>
        </div>
      </div>

      {/* Phone Input Panel */}
      {showPhoneInput && (
        <div style={styles.phoneInputSection}>
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
            style={styles.phoneButton(callActive)}
          >
            {callActive ? "End Call" : "Call"}
          </button>
        </div>
      )}
    </div>
  );
}
