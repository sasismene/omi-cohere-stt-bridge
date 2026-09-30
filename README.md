# Omi Cohere STT Bridge

A Render-ready WebSocket service that accepts Omi custom-STT audio and sends it to Cohere Transcribe.

## Protocol

Connect to `wss://YOUR-RENDER-SERVICE.onrender.com/stt`.

- Send Omi's binary Opus audio frames.
- Send `{"type":"CloseStream"}` when the utterance ends.
- The bridge returns `{"segments":[{"text":"...","speaker":"SPEAKER_00","start":0,"end":0}]}` and closes.
- `GET /health` returns a Render health response.

The bridge buffers one Omi utterance, decodes Opus to mono 16 kHz WAV with ffmpeg, calls Cohere's multipart audio-transcription endpoint, and normalizes the response to Omi's segment format.

## Deploy on Render

1. Push this repository to GitHub.
2. In Render, choose **New → Blueprint** and select this repository. Render reads `render.yaml`.
3. Set `COHERE_API_KEY` to a Cohere API key.
4. Deploy and check `https://YOUR-RENDER-SERVICE.onrender.com/health`.
5. Configure the Omi custom STT WebSocket URL as `wss://YOUR-RENDER-SERVICE.onrender.com/stt`.

Render's Docker image includes ffmpeg, which is required because Omi sends Opus frames while Cohere receives a supported uploaded audio file.

## Configuration

Copy `.env.example` for local development. `COHERE_LANGUAGE` is an ISO-639-1 language code. Cohere's documented upload limit is 25 MB; `MAX_AUDIO_BYTES` defaults to that limit. Set `BRIDGE_AUTH_ENABLED=true` and `BRIDGE_AUTH_TOKEN` if the client can send an Authorization bearer token.

## Local development

Requires Node.js 20+, npm, and ffmpeg:

```bash
cp .env.example .env
npm install
npm start
curl http://localhost:10000/health
```

Docker:

```bash
docker build -t omi-cohere-stt-bridge .
docker run --rm -p 10000:10000 --env-file .env omi-cohere-stt-bridge
```

## Security and limitations

Do not expose this endpoint without authentication in a public deployment unless that is intentional: anyone who can connect can spend your Cohere quota. The service buffers each utterance in memory and uses batch transcription, so it is not token-by-token live streaming. It emits one final transcript when `CloseStream` arrives.

This project is an independent integration and is not affiliated with Cohere or Based Hardware.
