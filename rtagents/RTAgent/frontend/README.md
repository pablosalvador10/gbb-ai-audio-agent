## 🚀 Run the Frontend

### Prerequisites

Before running the frontend, ensure you have the following:

- **Node.js**: Version 18 or higher.
- **Azure Speech Key & Region**: Required for speech services.
- **WebSocket Backend Endpoint**: 
    - Must accept `{ text }` as input.
    - Should stream GPT responses and audio chunks.

### Steps to Run

1. **Install Dependencies**

     Run the following command to install the required dependencies:

     ```bash
     npm install
     ```

2. **Configure Environment Variables**

     Create a `.env` file in the project root directory with the following content:

     ```env
     VITE_AZURE_SPEECH_KEY=your_azure_speech_key
     VITE_AZURE_REGION=your_region
     VITE_WS_URL=ws://localhost:8010/realtime
     ```

     Replace `your_azure_speech_key`, `your_region`, and `ws://localhost:8010/realtime` with your actual values.

3. **Start the Application**

     Use the command below to start the development server:

     ```bash
     npm run dev
     ```

     Once the server is running, open [http://localhost:5173](http://localhost:5173) in your browser to access the frontend.
