<!-- markdownlint-disable MD033 -->

# **🎙️ RTMedAgent: Real-Time Voice Intelligence for Healthcare Workflows**

> This project is **part of the [HLS Ignited Program](https://github.com/microsoft/aihlsIgnited)**, a series of hands-on accelerators designed to democratize AI in healthcare. The program supports providers and payers in building transformative solutions using **Azure AI Services**. Explore other accelerators and discover how AI is reshaping care delivery.

<img src="utils/images/medagent.png" align="right" height="200" style="float:right; height:200px;" />

**RTMedAgent** is a real-time voice agent that enables healthcare organizations to deliver **empathetic and intelligent voice-first experiences** using a cascade of **Azure AI services**. Built for patient-facing support scenarios, the system simulates a smart and responsive voice agent capable of assisting users with everyday healthcare tasks by combining the following technologies:

- **Azure Speech-to-Text** for real-time transcription  
- **Azure AI Search** for semantic understanding and intent recognition  
- **Azure Text-to-Speech** to generate human-like responses  
- **Azure OpenAI with Function Calling and Streaming Output** to dynamically trigger backend actions  
- **WebSocket architecture** to ensure low-latency, two-way voice interaction  

This intelligent voice agent assists with common healthcare workflows like appointment scheduling, prescription refills, medication guidance, and prior authorization evaluations, all while maintaining high standards for safety, privacy, and clarity.


## **📞 Addressing Burnout and Enhancing Efficiency in Healthcare Call Centers**

> "Healthcare call centers spend, on average, 43% of their annual operating budget on labor costs but only 0.6% on technologies to prevent agent burnout and turnover"  
> — *[The State of Healthcare Call Centers, Hyro](https://assets-002.noviams.com/novi-file-uploads/pac/PDFs-and-Documents/Industry_Partners/Hyro_-_The_State_of_Healthcare_Call_Centers_2023_Report-fa539649.pdf?utm_source=chatgpt.com)​*


Healthcare call centers are the frontline of patient interaction, yet they grapple with high turnover rates and staff burnout. A study by the Quality Assurance & Training Connection (QATC) indicates that the average call center agent turnover rate ranges between 30%–45%. This attrition not only affects service quality but also escalates operational costs.​ By embracing AI-driven tools, healthcare organizations can enhance efficiency, reduce staff turnover, and ultimately improve patient satisfaction.

## **🚀 How to Get Started**

If you're new to Azure AI Speech and Azure OpenAI, specilaly on the realtime realm of thissg pellas efolfow the labs. Experienced AI engineers can jump straight to the use case sections, which showcase how to create how **** and **X-ray Knowledge Stores** for real-world applications.

### **🧪 [RTMedAgent Labs](labs/README.md)**

+ 🧪 **Building Your Voice-to-Voice Agent with Azure AI Speech and AOAI**: [🧾 Notebook - Building a Voice-to-Voice Agent with Azure AI Speech](labs/01-build-your-audio-agent.ipynb) Follow this step-by-step guide to create a voice-to-voice agent using Azure AI Speech services and Azure OpenAI. Learn how to configure speech recognition, integrate external tools, and generate human-like responses for real-time interactions.

### **🏥 Use Cases**

#### **📝 [Voice-to-Voice Experience with RTMedAgent (Web-Powered AI Assistant)](rtagents/browser_RTMedAgent/README.md)**

This use case demonstrates how to deliver real-time, AI-powered healthcare conversations using Azure and OpenAI services. It transforms natural patient voice interactions into structured, actionable outcomes by orchestrating multiple services into one seamless agentic system.

![alt text](utils/images/arch.png)

1. **Audio Ingestion via Web Service**: Stream audio through a browser-based into a WebSocket connection.  
2. **Azure Speech-to-Text (STT)**: Converts live audio into transcribed text for LLM processing.  
3. **Azure OpenAI with Function Calling & Streaming**: Understands patient intent, routes queries, and dynamically calls backend tools in real time.  
4. **Azure Text-to-Speech (TTS)**: Delivers natural, empathetic voice responses back to the user (chunks).  
5. **RAI, Evaluation & Monitoring (Azure AI Studio)**: Ensures safety, transparency, and continuous performance evaluation of the deployed voice agent.  

## **📚 More Resources**

- **[Azure AI Foundry](https://azure.microsoft.com/en-us/products/ai-foundry/?msockid=0b24a995eaca6e7d3c1dbc1beb7e6fa8#Use-cases-and-Capabilities)** – Develop and deploy custom AI apps and APIs responsibly with a comprehensive platform.
- **[Azure AI Speech Documentation](https://learn.microsoft.com/en-us/azure/ai-services/speech-service/)** – Explore the official documentation for Azure AI Speech to learn about its capabilities, including Speech-to-Text, Text-to-Speech, and Speech Translation.
- **[Azure AI Speech Samples](https://github.com/Azure-Samples/cognitive-services-speech-sdk)** – Access a collection of sample projects and code snippets to help you get started with Azure AI Speech SDK.


<br>

> [!IMPORTANT]  
> This software is provided for demonstration purposes only. It is not intended to be relied upon for any production workload. The creators of this software make no representations or warranties of any kind, express or implied, about the completeness, accuracy, reliability, suitability, or availability of the software or related content. Any reliance placed on such information is strictly at your own risk.
