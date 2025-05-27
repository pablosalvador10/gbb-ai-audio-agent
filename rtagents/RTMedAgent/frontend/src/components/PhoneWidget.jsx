// src/components/PhoneWidget.jsx
import React from "react";
import PropTypes from "prop-types";

export default function PhoneWidget({
  callActive,
  targetPhoneNumber,
  setTargetPhoneNumber,
  startACSCall,
  phoneFrame // Pass the img path from parent
}) {
  return (
    <div style={{
      position:"fixed",right:28,bottom:0,width:260,height:350,
      display:"flex",alignItems:"center",justifyContent:"center"
    }}>
      {/* Pulsating ring */}
      {callActive && (
        <div style={{
          position:"absolute",left: "7%", width:"80%",height:"120%",
          borderRadius:20,background:"rgba(0, 183, 255, 0.88)",
          animation:"ring 1.6s ease-out infinite"
        }}/>
      )}

      {/* Phone frame */}
      <img src={phoneFrame} alt="Phone" style={{width:"100%",height:"auto"}} />

      {/* Blinking LED */}
      {callActive && (
        <div
          style={{
            position: "absolute",
            top: 88, left: "63%",
            transform: "translate(-50%, -50%)",
            width: 12, height: 12, borderRadius: "0%",
            background: "#A3FF12",
            boxShadow: "0 0 6px 3px rgba(73,255,18,.79)",
            animation: "led 1.2s infinite",
            pointerEvents: "none",
          }}
        />
      )}

      {/* Controls */}
      <div style={{
        position:"absolute",top:112,left:48,width:134,
        display:"flex",flexDirection:"column",gap:8
      }}>
        <input
          type="tel"
          disabled={callActive}
          value={targetPhoneNumber}
          placeholder="+15551234567"
          onChange={e => setTargetPhoneNumber(e.target.value)}
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
  );
}
PhoneWidget.propTypes = {
  callActive: PropTypes.bool.isRequired,
  targetPhoneNumber: PropTypes.string.isRequired,
  setTargetPhoneNumber: PropTypes.func.isRequired,
  startACSCall: PropTypes.func.isRequired,
  phoneFrame: PropTypes.string.isRequired,
};
