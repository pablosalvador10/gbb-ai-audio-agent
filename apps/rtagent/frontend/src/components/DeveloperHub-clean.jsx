/* ------------------------------------------------------------------ *
 *  DEVELOPER HUB - 2 SECTIONS ONLY
 * ------------------------------------------------------------------ */
const DeveloperHub = ({ socketRef }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [expandedSections, setExpandedSections] = useState({
    session: false,
    logs: false
  });
  const [devLogs, setDevLogs] = useState([]);
  const [sessionData, setSessionData] = useState({
    sessionId: null,
    connectionId: null,
    startTime: null,
    duration: 0,
    activeAgent: 'AutoAuth',
    authenticated: false
  });

  // Toggle individual sections
  const toggleSection = (section) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  // Add log entry with timestamp
  const addDevLog = useCallback((message) => {
    const timestamp = new Date().toLocaleTimeString('en-US', { 
      hour12: false, 
      hour: '2-digit', 
      minute: '2-digit', 
      second: '2-digit',
      fractionalSecondDigits: 3 
    });
    setDevLogs(prev => [
      ...prev.slice(-49), // Keep only last 50 logs
      { timestamp, message }
    ]);
  }, []);

  // Extract session data from main WebSocket connection
  useEffect(() => {
    if (!socketRef?.current) return;

    const socket = socketRef.current;
    const originalOnMessage = socket.onmessage;

    // Intercept WebSocket messages to extract session data
    socket.onmessage = async (event) => {
      // Call original handler first
      if (originalOnMessage) {
        originalOnMessage(event);
      }

      // Extract developer data from messages
      if (typeof event.data === "string") {
        try {
          const data = JSON.parse(event.data);
          
          // Extract session info from status messages
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

          // Track message types with specific backend error detection
          if (data.type) {
            switch (data.type) {
              case "error":
                const errorMsg = data.message || data.error || 'Unknown error';
                if (errorMsg.includes('recognizer.write_bytes hanging')) {
                  addDevLog(`❌ CRITICAL: Speech recognizer blocked - timeout after ${data.timeout || 'unknown'}s`);
                } else if (errorMsg.includes('Audio processing timeout')) {
                  addDevLog(`⏰ TIMEOUT: Audio processing blocked - ${errorMsg}`);
                } else {
                  addDevLog(`❌ Error: ${errorMsg}`);
                }
                break;
              case "info":
                const infoMsg = data.message || 'Info message';
                if (infoMsg.includes('Call ended normally')) {
                  addDevLog(`📞 CALL END: ${infoMsg} (${data.callId || 'unknown ID'})`);
                } else {
                  addDevLog(`ℹ️ Info: ${infoMsg}`);
                }
                break;
              case "backend_log":
                const backendMsg = data.message;
                if (backendMsg.includes('recognizer.write_bytes hanging')) {
                  addDevLog(`❌ BACKEND CRITICAL: ${backendMsg}`);
                } else if (backendMsg.includes('Persistent storage max capacity')) {
                  addDevLog(`💾 STORAGE FULL: Telemetry capacity reached`);
                } else {
                  addDevLog(`🔧 Backend: [${data.level?.toUpperCase()}] ${backendMsg}`);
                }
                break;
              default:
                if (data.type !== "keepalive" && data.type !== "heartbeat") {
                  addDevLog(`📝 ${data.type}: ${JSON.stringify(data).slice(0, 100)}`);
                }
            }
          }

          // Track agent changes
          if (data.sender && data.sender !== "User" && data.sender !== "System") {
            let realAgent = data.orchestrator || data.sender;
            setSessionData(prev => {
              if (prev.activeAgent !== realAgent) {
                addDevLog(`🔄 Agent Handoff: ${prev.activeAgent} → ${realAgent}`);
                return { ...prev, activeAgent: realAgent };
              }
              return prev;
            });
          }

        } catch (e) {
          // Log parsing errors for debugging
          if (typeof event.data === 'string' && event.data.length < 1000) {
            addDevLog(`🔍 Raw Message: ${event.data.slice(0, 100)}`);
          }
        }
      }
    };

    return () => {
      socket.onmessage = originalOnMessage;
    };
  }, [socketRef, addDevLog]);

  // Update duration counter
  useEffect(() => {
    if (!sessionData.startTime) return;

    const interval = setInterval(() => {
      setSessionData(prev => ({
        ...prev,
        duration: Math.floor((Date.now() - prev.startTime) / 1000)
      }));
    }, 1000);

    return () => clearInterval(interval);
  }, [sessionData.startTime]);

  // Initialize session when WebSocket connects
  useEffect(() => {
    if (socketRef?.current?.readyState === WebSocket.OPEN && !sessionData.sessionId) {
      const sessionId = `sess_${Math.random().toString(36).substr(2, 8)}`;
      const connectionId = `conn_${Math.random().toString(36).substr(2, 6)}`;
      
      setSessionData(prev => ({
        ...prev,
        sessionId,
        connectionId,
        startTime: new Date(),
        activeAgent: 'AutoAuth',
        authenticated: false
      }));

      addDevLog('🔄 Developer Hub initialized');
      addDevLog(`📋 Session ${sessionId} created`);
      addDevLog('🤖 AutoAuth agent activated');
      addDevLog('📡 Real-time logging system active');
    }
  }, [socketRef, sessionData.sessionId, addDevLog]);

  // Backend log streaming via WebSocket (ws_log_broadcaster.py)
  useEffect(() => {
    if (!sessionData.sessionId) return;

    let logSocket = null;
    
    const connectToLogStream = () => {
      try {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.hostname;
        const port = '8010';
        const logStreamUrl = `${protocol}//${host}:${port}/ws/logs?level=DEBUG`;
        
        addDevLog(`🔗 Connecting to backend logs: ${logStreamUrl}`);
        logSocket = new WebSocket(logStreamUrl);
        
        logSocket.onopen = () => {
          addDevLog('✅ Connected to backend log stream');
        };
        
        logSocket.onmessage = (event) => {
          try {
            const logData = JSON.parse(event.data);
            
            if (logData.level && logData.message) {
              const level = logData.level.toUpperCase();
              const logger = logData.logger || 'backend';
              const message = logData.message;
              
              // Pattern matching for specific backend errors
              if (message.includes('recognizer.write_bytes hanging')) {
                addDevLog(`🚨 CRITICAL: ${message} [${logger}]`);
              } else if (message.includes('Audio processing timeout')) {
                addDevLog(`⏰ TIMEOUT: ${message} [${logger}]`);
              } else if (message.includes('Persistent storage max capacity')) {
                addDevLog(`💾 STORAGE: ${message} [${logger}]`);
              } else if (message.includes('Call ended normally')) {
                addDevLog(`📞 CALL: ${message} [${logger}]`);
              } else {
                const shortMessage = message.length > 150 ? message.slice(0, 150) + '...' : message;
                let emoji = level === 'ERROR' ? '❌' : level === 'WARNING' ? '⚠️' : '🖥️';
                addDevLog(`${emoji} [${logger}] ${shortMessage}`);
              }
            }
          } catch (err) {
            addDevLog(`❌ Error parsing backend log: ${err.message}`);
          }
        };
        
        logSocket.onclose = (event) => {
          addDevLog(`🔌 Backend log stream disconnected (${event.code})`);
          
          // Attempt to reconnect after 5 seconds
          setTimeout(() => {
            if (!logSocket || logSocket.readyState === WebSocket.CLOSED) {
              addDevLog('🔄 Attempting to reconnect to backend logs...');
              connectToLogStream();
            }
          }, 5000);
        };
        
        logSocket.onerror = (error) => {
          addDevLog(`❌ Backend log WebSocket error`);
        };
        
      } catch (err) {
        addDevLog(`❌ Failed to connect to backend log stream: ${err.message}`);
      }
    };
    
    connectToLogStream();
    
    return () => {
      if (logSocket) {
        logSocket.close();
      }
    };
  }, [sessionData.sessionId, addDevLog]);

  // Console log interception for capturing frontend errors
  useEffect(() => {
    const originalError = console.error;
    const originalWarn = console.warn;
    
    console.error = (...args) => {
      originalError(...args);
      const message = args.join(' ');
      if (message.includes('recognizer') || message.includes('Audio processing')) {
        addDevLog(`❌ Console Error: ${message.slice(0, 100)}`);
      }
    };
    
    console.warn = (...args) => {
      originalWarn(...args);
      const message = args.join(' ');
      if (message.includes('Message processing took') && message.includes('should be < 5ms')) {
        addDevLog(`⚡ PERF: ${message.slice(0, 100)}`);
      }
    };
    
    return () => { 
      console.error = originalError;
      console.warn = originalWarn;
    };
  }, [addDevLog]);

  // Format duration in mm:ss format  
  const formatDuration = (seconds) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  return (
    <div 
      style={styles.developerHub}
      onClick={() => setIsExpanded(!isExpanded)}
      title="Click to expand developer information"
    >
      <div style={styles.devHubHeader}>
        <div style={styles.devHubStatus}></div>
        <span style={styles.devHubLabel}>Developer Hub</span>
        <span style={{
          ...styles.expandIcon,
          transform: isExpanded ? "rotate(180deg)" : "rotate(0deg)",
        }}>▼</span>
      </div>
      
      {/* Compact view */}
      {!isExpanded && (
        <div style={{
          fontSize: "9px",
          color: "#64748b",
          marginTop: "2px",
        }}>
          {sessionData.sessionId ? (
            `Session: ${sessionData.sessionId} • Agent: ${sessionData.activeAgent} • ${formatDuration(sessionData.duration)}`
          ) : (
            "Waiting for session..."
          )}
        </div>
      )}

      {/* Expanded view */}
      {isExpanded && (
        <div onClick={(e) => e.stopPropagation()}>
          {/* Section 1: Session Context */}
          <div style={styles.devSection}>
            <div 
              style={styles.devSectionHeader}
              onClick={() => toggleSection('session')}
            >
              <span style={styles.devSectionIcon}>📋</span>
              <span style={styles.devSectionTitle}>Session Context</span>
              <span style={{
                ...styles.devSectionArrow,
                transform: expandedSections.session ? "rotate(180deg)" : "rotate(0deg)",
              }}>▼</span>
            </div>
            {expandedSections.session && (
              <div style={styles.devSectionContent}>
                <div style={styles.devContextItem}>
                  <span style={styles.devContextLabel}>Session ID:</span>
                  <span style={styles.devContextValue}>
                    {sessionData.sessionId || 'Not connected'}
                  </span>
                </div>
                <div style={styles.devContextItem}>
                  <span style={styles.devContextLabel}>Connection ID:</span>
                  <span style={styles.devContextValue}>
                    {sessionData.connectionId || 'Not established'}
                  </span>
                </div>
                <div style={styles.devContextItem}>
                  <span style={styles.devContextLabel}>Active Agent:</span>
                  <span style={{
                    ...styles.devContextValue,
                    color: sessionData.activeAgent === 'AutoAuth' ? '#f59e0b' : 
                           sessionData.activeAgent === 'Claims' ? '#06b6d4' : 
                           sessionData.activeAgent === 'General' ? '#10b981' : '#64748b'
                  }}>
                    🤖 {sessionData.activeAgent}
                  </span>
                </div>
                <div style={styles.devContextItem}>
                  <span style={styles.devContextLabel}>Session Duration:</span>
                  <span style={styles.devContextValue}>
                    {sessionData.startTime ? formatDuration(sessionData.duration) : 'Not started'}
                  </span>
                </div>
                <div style={styles.devContextItem}>
                  <span style={styles.devContextLabel}>Authentication:</span>
                  <span style={{
                    ...styles.devContextValue,
                    color: sessionData.authenticated ? '#10b981' : '#f59e0b'
                  }}>
                    {sessionData.authenticated ? '🔐 Authenticated' : '🔓 Pending Auth'}
                  </span>
                </div>
              </div>
            )}
          </div>

          {/* Section 2: Real-Time Logs */}
          <div style={styles.devSection}>
            <div 
              style={styles.devSectionHeader}
              onClick={() => toggleSection('logs')}
            >
              <span style={styles.devSectionIcon}>📊</span>
              <span style={styles.devSectionTitle}>Real-Time Logs</span>
              <span style={{
                ...styles.devSectionArrow,
                transform: expandedSections.logs ? "rotate(180deg)" : "rotate(0deg)",
              }}>▼</span>
            </div>
            {expandedSections.logs && (
              <div style={styles.devSectionContent}>
                <div style={{ maxHeight: '140px', overflowY: 'auto', fontSize: '10px' }}>
                  {devLogs.length > 0 ? (
                    devLogs.slice(-20).reverse().map((log, idx) => {
                      // Determine log color based on content
                      let logColor = '#64748b'; // Default
                      if (log.message.includes('❌') || log.message.includes('Error')) {
                        logColor = '#ef4444'; // Red for errors
                      } else if (log.message.includes('⚠️') || log.message.includes('Warning')) {
                        logColor = '#f59e0b'; // Yellow for warnings  
                      } else if (log.message.includes('✅') || log.message.includes('Success')) {
                        logColor = '#10b981'; // Green for success
                      } else if (log.message.includes('🔧') || log.message.includes('Backend')) {
                        logColor = '#8b5cf6'; // Purple for backend
                      } else if (log.message.includes('🤖') || log.message.includes('Agent')) {
                        logColor = '#06b6d4'; // Cyan for agents
                      } else if (log.message.includes('🎵') || log.message.includes('Audio')) {
                        logColor = '#14b8a6'; // Teal for audio
                      } else if (log.message.includes('🔗') || log.message.includes('Connection')) {
                        logColor = '#3b82f6'; // Blue for connections
                      }

                      return (
                        <div 
                          key={idx} 
                          style={{
                            ...styles.devLogEntry,
                            color: logColor,
                            borderLeft: `2px solid ${logColor}`,
                            paddingLeft: '6px',
                            marginBottom: '1px'
                          }}
                        >
                          <span style={{ color: '#64748b', fontSize: '9px' }}>[{log.timestamp}]</span>{' '}
                          <span style={{ color: logColor }}>{log.message}</span>
                        </div>
                      );
                    })
                  ) : (
                    <div style={{ color: '#64748b', fontStyle: 'italic', padding: '8px', fontSize: '10px' }}>
                      Waiting for activity... Backend logs will appear here.
                    </div>
                  )}
                </div>
                {/* Log Controls */}
                <div style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  marginTop: '8px',
                  padding: '4px',
                  borderTop: '1px solid #e2e8f0',
                  fontSize: '9px'
                }}>
                  <span style={{ color: '#64748b' }}>
                    {devLogs.length} logs captured
                  </span>
                  <div>
                    <button
                      onClick={() => setDevLogs([])}
                      style={{
                        background: 'none',
                        border: '1px solid #e2e8f0',
                        borderRadius: '3px',
                        padding: '2px 6px',
                        color: '#64748b',
                        fontSize: '9px',
                        cursor: 'pointer',
                        marginRight: '4px'
                      }}
                    >
                      Clear
                    </button>
                    <button
                      onClick={() => {
                        const logData = devLogs.map(log => `[${log.timestamp}] ${log.message}`).join('\n');
                        navigator.clipboard.writeText(logData);
                        addDevLog('📋 Logs copied to clipboard');
                      }}
                      style={{
                        background: 'none',
                        border: '1px solid #e2e8f0',
                        borderRadius: '3px',
                        padding: '2px 6px',
                        color: '#64748b',
                        fontSize: '9px',
                        cursor: 'pointer'
                      }}
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
