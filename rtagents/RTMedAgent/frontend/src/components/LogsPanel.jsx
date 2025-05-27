// src/components/LogsPanel.jsx
import React from "react";
import PropTypes from "prop-types";

export default function LogsPanel({ log }) {
  return (
    <div style={{
      width: "100%",
      maxWidth: 1080,
      marginTop: 24,
    }}>
      <h3 style={{ marginBottom: 8 }}>System Logs</h3>
      <pre style={{
        background: "#17202A",
        padding: 14,
        borderRadius: 10,
        fontSize: "0.9rem",
        maxHeight: 260,
        overflow: "auto",
        whiteSpace: "pre-wrap"
      }}>{log}</pre>
    </div>
  );
}
LogsPanel.propTypes = {
  log: PropTypes.string.isRequired
};
