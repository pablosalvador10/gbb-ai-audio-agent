# 🧭 Decision Guide: Breaking Out Components into Microservices

This guide helps determine **when to split components** of a real-time, low-latency voice application (e.g., using FastAPI, WebSockets, Azure Communication Services, Azure OpenAI, EventGrid) into separate services.

---

## ⚙️ Core Criteria for Decomposition

| Criteria                          | Monolith (Single Service)                  | Microservice (Breakout)                                 |
|----------------------------------|--------------------------------------------|----------------------------------------------------------|
| **Latency Sensitivity**         | Real-time <100ms loops (e.g. STT → LLM → TTS) | Can tolerate inter-service latency (>50–100ms)           |
| **Scaling Needs**               | Components scale equally                   | Certain paths (LLM, STT, EventGrid) scale independently  |
| **Failure Containment**         | Failure in one area affects all            | Failures are isolated by service                         |
| **Deployment Cadence**          | Uniform update schedule                    | Components updated/released separately                   |
| **Security Boundaries**         | Uniform access control                     | Needs separate RBAC, network rules per service           |
| **Team Autonomy**               | Same team owns entire stack                | Different teams own and deploy different pipelines       |
| **Observability Needs**         | Centralized logging and tracing is acceptable | Need per-component metrics and alerts                    |
| **DevOps Complexity**           | Want simpler pipelines                     | Can handle multiple build/test/deploy CI pipelines       |

---

## 🔍 Decision Checklist

Answer the following for each component (e.g., STT, WebSocket handler, EventGrid consumer):

### ✅ Latency & Coupling

- [ ] Does this component need direct access to shared memory or in-process calls?
- [ ] Does it participate in <150ms real-time loops (e.g., voice response)?
- [ ] Would introducing 1–2 network hops severely degrade performance?

> **If yes to most → keep in monolith**

---

### ✅ Independent Scale & Resources

- [ ] Does this component have heavier resource usage (CPU/mem/GPU)?
- [ ] Does its load pattern differ (e.g., bursty events, batch postprocessing)?
- [ ] Can it benefit from isolated HPA rules or node pools?

> **If yes → consider breaking out**

---

### ✅ Failure Domain

- [ ] Would failure in this component affect live user experience?
- [ ] Can retries / fallbacks be implemented asynchronously?
- [ ] Does it depend on external integrations (e.g., Blob events, CosmosDB)?

> **If yes → isolate for reliability**

---

### ✅ Ownership and Deployability

- [ ] Does a different team or function own this logic?
- [ ] Is it released or evolved at a different cadence?
- [ ] Do you want smaller blast radius for deploys?

> **If yes → break out into separate container or service**

---

## 🧱 Recommended Boundaries

| Component                  | Suggested Shape            | Justification                                         |
|---------------------------|----------------------------|------------------------------------------------------|
| **FastAPI API**           | Standalone container       | Expose `/start-call`, `/status`, `/agents` etc.     |
| **WebSocket Handler**     | Co-locate with API or split| Real-time user/UI loop needs tight integration       |
| **STT/TTS Processor**     | Separate worker            | May require GPU/high mem, different scale rate       |
| **LLM Pipeline**          | Separate if high load      | Allows throttling, queueing, dedicated resources     |
| **EventGrid Consumer**    | Separate container         | Decoupled logic, often async or non-blocking         |
| **Blob or Recording Postprocessing** | Worker process or serverless | Long latency, doesn’t affect live sessions           |
| **Redis or Session Store**| Shared backing infra       | Used across all services for centralized state       |

---

## 📈 Scalability Theme: Monolith vs Microservices
The choice between a monolithic and a microservices architecture significantly impacts how an application scales, especially under the demanding conditions of real-time voice processing.

**Monolithic Architecture:**
In a monolith, all components (API, WebSocket handler, STT, LLM, TTS) are bundled into a single deployment unit. Scaling involves replicating the entire monolith.

*   **Pros:** Simpler deployment and initial development.
*   **Cons:**
    *   **Inefficient Scaling:** If only one component (e.g., LLM processing) is a bottleneck, you must scale the entire application, leading to underutilization of other components in the new instances.
    *   **Resource Contention:** Resource-heavy processes (like STT or LLM) can starve other less intensive but critical processes (like WebSocket handling) for CPU/memory.
    *   **Larger Blast Radius:** A failure or bug in one component can bring down the entire application.

    ```mermaid
    graph TD
        subgraph "Monolith: Unified Scaling"
            direction LR
            U[User Call] --> M[Monolith Instance 1]
            M --> STT1[STT]
            M --> LLM1[LLM]
            M --> TTS1[TTS]
            M --> WS1[WebSocket]
            M --> API1[API]
            M --> EG1[EventGrid Handler]
            M --> R1[Redis Cache]

            U --> M2[Monolith Instance 2 Scaled]
            M2 --> STT2[STT]
            M2 --> LLM2[LLM]
            M2 --> TTS2[TTS]
            M2 --> WS2[WebSocket]
            M2 --> API2[API]
            M2 --> EG2[EventGrid Handler]
            M2 --> R2[Redis Cache]

            style M fill:#d1eaff,stroke:#3a8ee6
            style M2 fill:#d1eaff,stroke:#3a8ee6
            style EG1 fill:#f9e6ff,stroke:#a259c4
            style EG2 fill:#f9e6ff,stroke:#a259c4
            style R1 fill:#ffe6e6,stroke:#e74c3c
            style R2 fill:#ffe6e6,stroke:#e74c3c
            %% EventGrid and Redis are handled in-process within each monolith instance.
        end
    ```

    **Microservices Architecture:**
    In a microservices architecture, components are deployed as separate services. This allows for independent scaling based on the specific needs of each service.

    *   **Pros:**
        *   **Efficient Scaling:** Resource-intensive services (e.g., STT, LLM, TTS workers) can be scaled independently. If LLM processing is the bottleneck, only the LLM service needs more instances.
        *   **Resource Isolation:** Each service can be allocated resources (CPU, memory, GPU) tailored to its needs, preventing contention.
        *   **Improved Resilience:** Failure in one microservice (e.g., TTS worker) might degrade a specific functionality but won't necessarily bring down the entire call processing pipeline if fallbacks or retries are in place.
    *   **Cons:**
        *   **Increased Complexity:** Managing multiple services, inter-service communication (latency, reliability), and distributed tracing adds operational overhead.
        *   **Network Latency:** Communication between services introduces network hops, which can add latency if not carefully managed, especially critical for real-time loops.

    ```mermaid
    graph TD
        subgraph "Microservices: Independent Scaling"
            direction LR
            U[User Call] --> API[API Service x1 instance]
            API --> WS[WebSocket Service x2 instances]
            WS --> STT[STT Worker x3 instances]
            STT --> LLM[LLM Service x5 instances]
            LLM --> TTS[TTS Worker x3 instances]
            TTS --> WS
            
            ACS --> EG

            API --> EG[EventGrid Consumer Service]
            EG --> STT
            EG --> LLM
            EG --> TTS

            API --> R[Redis Cache]
            WS --> R
            STT --> R
            LLM --> R
            TTS --> R

            style API fill:#e8fce8,stroke:#27ae60
            style WS fill:#e8fce8,stroke:#27ae60
            style STT fill:#fef6d5,stroke:#f1c40f
            style LLM fill:#fef6d5,stroke:#f1c40f
            style TTS fill:#fef6d5,stroke:#f1c40f
            style EG fill:#f9e6ff,stroke:#a259c4
            style R fill:#ffe6e6,stroke:#e74c3c
            %% EventGrid coordinates async events; Redis provides shared state/session.
        end
    ```

    **Tradeoffs Summary:**

    | Aspect                 | Monolith                                       | Microservices                                               |
    |------------------------|------------------------------------------------|-------------------------------------------------------------|
    | **Scalability Granularity** | Coarse all or nothing                        | Fine-grained per service                                  |
    | **Resource Utilization** | Potentially inefficient                        | More efficient, tailored to service needs                   |
    | **Development Complexity** | Lower initially                                | Higher distributed system                                 |
    | **Operational Complexity** | Simpler deployment & management              | More complex service discovery, orchestration, monitoring |
    | **Fault Isolation**    | Low single point of failure                  | High failures can be contained                            |
    | **Latency**            | Lower in-process calls                       | Higher network calls between services                     |
    | **Team Autonomy**      | Limited                                        | Higher services owned by different teams                  |

For real-time call center operations, if specific components like LLM or STT processing are significantly more resource-intensive and have variable load patterns, a microservices approach allows for more cost-effective and responsive scaling. However, this comes at the cost of increased architectural and operational complexity, and careful design is needed to manage inter-service latency.
