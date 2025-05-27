// src/components/ControlBar.jsx
import React from "react";
import PropTypes from "prop-types";

const styles = {
  controlBar: {
    textAlign: "center",
    width: "100%",
  },
  primaryBtn: (recording) => ({
    padding: "14px 40px",
    border: "none",
    borderRadius: 10,
    fontWeight: 600,
    fontSize: "1rem",
    cursor: "pointer",
    background: recording ? "#D13438" : "#107C10",
    color: "#fff",
  }),
};

export default function ControlBar({ recording, onStart, onStop }) {
  return (
    <div style={styles.controlBar}>
      <button
        style={styles.primaryBtn(recording)}
        onClick={recording ? onStop : onStart}
      >
        {recording ? "⏹ End Conversation" : "▶ Start Conversation"}
      </button>
    </div>
  );
}

ControlBar.propTypes = {
  recording: PropTypes.bool.isRequired,
  onStart: PropTypes.func.isRequired,
  onStop: PropTypes.func.isRequired,
};
