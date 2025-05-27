// src/components/ChatBubble.jsx
import React from "react";
import PropTypes from "prop-types";

export default function ChatBubble({ msg }) {
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
          {(msg.text || "").split("\n").map((p, i) => (
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
}

ChatBubble.propTypes = {
  msg: PropTypes.shape({
    speaker: PropTypes.string.isRequired,
    text: PropTypes.string,
    streaming: PropTypes.bool,
    isTool: PropTypes.bool,
  }).isRequired,
};
