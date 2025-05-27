// src/components/ChatHistory.jsx
import React, { forwardRef } from "react";
import PropTypes from "prop-types";
import ChatMessage from "./ChatMessage";

const ChatHistory = forwardRef(function ChatHistory({ messages }, ref) {
  return (
    <div ref={ref} style={{ flex:1, overflowY:"auto", padding:"12px 18px" }}>
      {messages.map((m, i) => <ChatMessage key={i} msg={m} />)}
    </div>
  );
});
ChatHistory.propTypes = {
  messages: PropTypes.array.isRequired
};
export default ChatHistory;