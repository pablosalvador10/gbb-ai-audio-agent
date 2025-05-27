// src/components/MindMap.jsx
import React, { useEffect } from "react";
import PropTypes from "prop-types";
import ReactFlow, { ReactFlowProvider, MiniMap, Controls } from "reactflow";
import "reactflow/dist/style.css";

// MindMap expects to receive all needed props from parent
export default function MindMap({
  nodes,
  edges,
  activeSpeaker,
  setNodes,
  setEdges,
}) {
  // Highlight active speaker node
  useEffect(() => {
    setNodes((prev) =>
      prev.map((n) => {
        if (n.id === "user-root") {
          return {
            ...n,
            style: {
              ...n.style,
              border:
                activeSpeaker === "User"
                  ? "2px solid #FCD34D"
                  : "2px solid transparent",
              animation:
                activeSpeaker === "User"
                  ? "pulseNode 1.6s ease-out infinite"
                  : undefined,
            },
          };
        }
        if (n.id === "assistant-root") {
          return {
            ...n,
            style: {
              ...n.style,
              border:
                activeSpeaker === "Assistant"
                  ? "2px solid #FCD34D"
                  : "2px solid transparent",
              animation:
                activeSpeaker === "Assistant"
                  ? "pulseNode 1.6s ease-out infinite"
                  : undefined,
            },
          };
        }
        return n;
      })
    );
  }, [activeSpeaker, setNodes]);

  return (
    <ReactFlowProvider>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        panOnScroll
        zoomOnScroll
        defaultEdgeOptions={{
          markerEnd: { type: "arrowclosed", width: 12, height: 12 },
        }}
        style={{ width: "100%", height: "100%", background: "transparent" }}
      >
        <MiniMap
          nodeColor={(n) =>
            n.id === "user-root"
              ? "#0F766E"
              : n.id === "assistant-root"
              ? "#4338CA"
              : n.style?.background || "#334155"
          }
        />
        <Controls />
      </ReactFlow>
    </ReactFlowProvider>
  );
}

MindMap.propTypes = {
  nodes: PropTypes.array.isRequired,
  edges: PropTypes.array.isRequired,
  setNodes: PropTypes.func.isRequired,
  setEdges: PropTypes.func.isRequired,
  activeSpeaker: PropTypes.string,
};
