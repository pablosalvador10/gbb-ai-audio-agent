// src/components/ChatSection.jsx
import React, { useRef, useEffect } from "react";
import PropTypes from "prop-types";
import ChatBubble from "./ChatBubble";

const styles = {
  chatWrapper: {
    background: "#263238",
    borderRadius: 12,
    padding: 20,
    width: "95%",
    maxWidth: 1080,
    height: 480,
    overflow: "hidden",
    display: "flex",
    flexDirection: "column",
  },
  chatScroll: {
    flex: 1,
    overflowY: "auto",
    padding: "12px 18px",
  },
};

export default function ChatSection({ messages }) {
  const chatRef = useRef(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (chatRef.current) chatRef.current.scrollTop = chatRef.current.scrollHeight;
  }, [messages]);

  return (
    <section style={styles.chatWrapper}>
      <div ref={chatRef} style={styles.chatScroll}>
        {messages.map((m, i) => (
          <ChatBubble key={i} msg={m} />
        ))}
      </div>
    </section>
  );
}

ChatSection.propTypes = {
  messages: PropTypes.arrayOf(PropTypes.object).isRequired,
};
