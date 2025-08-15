// Backend WebSocket Logger Integration
// Simple addition to your existing Developer Hub

// Add this to your existing useEffect hooks in the DeveloperHub component:

useEffect(() => {
  if (!sessionData.sessionId) return;

  let backendSocket = null;
  
  const connectToBackendLogs = () => {
    try {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = window.location.hostname || 'localhost';
      const port = '8010'; // Your backend port
      const wsUrl = `${protocol}//${host}:${port}/ws/logs?level=DEBUG`;
      
      addDevLog(`🔗 Connecting to backend logs: ${wsUrl}`);
      
      backendSocket = new WebSocket(wsUrl);
      
      backendSocket.onopen = () => {
        addDevLog('✅ Connected to backend logging system', 'info');
      };
      
      backendSocket.onmessage = (event) => {
        try {
          const logData = JSON.parse(event.data);
          
          if (logData.type === 'backend_log') {
            const level = logData.level ? logData.level.toLowerCase() : 'info';
            const logger = logData.logger || 'backend';
            const message = logData.message || '';
            
            // Pattern matching for your specific backend errors
            if (message.includes('recognizer.write_bytes hanging') || 
                message.includes('Audio processing timeout')) {
              addDevLog(`🚨 BACKEND CRITICAL: ${message}`, 'error');
            } else if (message.includes('Message processing took') && 
                       message.includes('should be < 5ms')) {
              addDevLog(`⚡ BACKEND PERF: ${message}`, 'warning');
            } else if (message.includes('Persistent storage max capacity')) {
              addDevLog(`💾 BACKEND STORAGE: ${message}`, 'warning');
            } else if (message.includes('Call ended normally')) {
              addDevLog(`📞 BACKEND CALL: ${message}`, 'info');
            } else {
              const shortMessage = message.length > 100 ? message.slice(0, 100) + '...' : message;
              addDevLog(`🖥️ [${logger}] ${shortMessage}`, level);
            }
          }
        } catch (err) {
          addDevLog(`❌ Error parsing backend log: ${err.message}`, 'error');
        }
      };
      
      backendSocket.onclose = (event) => {
        addDevLog(`🔌 Backend logs disconnected (${event.code})`, 'warning');
        // Auto-reconnect after 5 seconds
        setTimeout(connectToBackendLogs, 5000);
      };
      
      backendSocket.onerror = (error) => {
        addDevLog(`❌ Backend WebSocket error`, 'error');
      };
      
    } catch (err) {
      addDevLog(`❌ Failed to connect to backend logs: ${err.message}`, 'error');
    }
  };
  
  connectToBackendLogs();
  
  return () => {
    if (backendSocket) {
      backendSocket.close();
    }
  };
}, [sessionData.sessionId, addDevLog]);
