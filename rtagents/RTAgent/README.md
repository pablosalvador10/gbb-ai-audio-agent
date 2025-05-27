# 🧠 RTMedAgent – Real-Time Voice AI Assistant (Browser-Based)

Enable **real-time voice-to-voice healthcare interactions** using Azure Speech Services and GPT. This browser-based application listens to patient speech, interprets intent using AI, and responds with synthesized speech via Azure Text-to-Speech (TTS)—all in real time.

## 📂 Folder Structure

```
rtagents/
└── browser_RTMedAgent/
    ├── backend/               # WebSocket server with GPT integration (Python)
    ├── frontend/              # React + Vite UI powered by Azure Speech SDK
    ├── test_cases_scenarios/  # Optional test scripts and scenarios
    └── README.md              # This file
```

## 🧪 Use Case Summary

> #### **📝 Real-Time Voice AI for Healthcare**
>
> RTMedAgent showcases how to deliver real-time, AI-driven healthcare conversations using Azure and OpenAI services. It transforms natural patient speech into actionable, structured outcomes through a seamless, interactive system.

## 🚀 Getting Started

### 1. 🔧 Start the Backend
### 🛰️ Using Azure Communication Services (ACS) for Calling

If you want to enable outbound calling via Azure Communication Services (ACS):

1. **Create a Dev Tunnel for Local Backend Access**

    ACS requires your backend to be accessible from the public internet. Use [Azure Dev Tunnel](https://learn.microsoft.com/en-us/azure/developer/dev-tunnels/overview) to expose your local backend on port `8010`:

    ```bash
    devtunnel create --allow-anonymous
    devtunnel port create -p 8010
    devtunnel host    
    ```

    This will provide a public URL (e.g., `https://<random>-<port>.use.devtunnels.ms`). Use this URL for your ACS webhook configuration.
    Set this as your `BASE_URL` value on your python .env

2. **Update Environment Variables**

    - Copy `.env.sample` to `.env` in the `root` directory:

      ```bash
      cp .env.sample .env
      ```

    - Edit `.env` and update the following variables as needed:
      ```env
      AZURE_OPENAI_API_KEY=your_openai_api_key
      AZURE_OPENAI_ENDPOINT=your_openai_endpoint
      AZURE_OPENAI_DEPLOYMENT=your_openai_deployment_name
      AZURE_OPENAI_CHAT_DEPLOYMENT_VERSION=2024-10-01-preview
      AZURE_SPEECH_KEY=your_speech_service_key
      AZURE_SPEECH_REGION=your_speech_service_region
      BASE_URL=https://<your-devtunnel>.devtunnels.ms
      ACS_CONNECTION_STRING=your_acs_connection_string
      ACS_SOURCE_PHONE_NUMBER=+1234567890
      ```

    Replace `<your-devtunnel>` with the public Dev Tunnel URL from step 1.

3. **Configure ACS Webhook**

    In your Azure Communication Services resource, set the webhook/callback URL to your Dev Tunnel endpoint (e.g., `https://<your-devtunnel>.devtunnels.ms/api/acs-callback`).

---
Navigate to the `backend` folder and start the WebSocket server:

```bash
cd rtagents/browser_RTMedAgent/backend
pip install -r requirements.txt
python server.py
```

✅ The WebSocket server will start at: `ws://localhost:8010/realtime`

### 2. 💻 Start the Frontend

In a new terminal, navigate to the `frontend` folder and start the UI:

```bash
cd rtagents/browser_RTMedAgent/frontend
npm install
npm run dev
```

✅ The UI will be available at: `http://localhost:5173`

### 🔑 Environment Setup (Optional)

If supported, create a `.env` file in the `frontend` directory with the following variables:

  ```env
  VITE_AZURE_SPEECH_KEY=your_speech_key
  VITE_AZURE_REGION=your_region
  VITE_BACKEND_BASE_URL=https://<your-devtunnel>.devtunnels.ms
  ```

If `.env` is not supported, manually update these constants in `App.jsx`.

## 🛠️ System Overview

- **🎤 Speech-to-Text (STT):** Azure Speech SDK
- **🧠 AI Reasoning:** Azure OpenAI GPT (via backend)
- **🔊 Text-to-Speech (TTS):** Azure Neural Voices
- **🔁 Real-Time Streaming:** WebSocket for bidirectional communication
- **🖥️ Frontend:** React + Vite

This system enables seamless, real-time voice interactions for healthcare applications.