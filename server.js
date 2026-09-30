import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import dotenv from 'dotenv';
import { WebSocketServer } from 'ws';

dotenv.config();

const PORT = Number(process.env.PORT || 10000);
const MAX_AUDIO_BYTES = Number(process.env.MAX_AUDIO_BYTES || 25_000_000);
const SESSION_TIMEOUT_MS = Number(process.env.SESSION_TIMEOUT_MS || 90_000);
const AUTH_ENABLED = process.env.BRIDGE_AUTH_ENABLED === 'true';

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'omi-cohere-stt-bridge' }));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_AUDIO_BYTES });
server.on('upgrade', (req, socket, head) => {
  if (new URL(req.url || '/', 'http://localhost').pathname !== '/stt') {
    socket.destroy();
    return;
  }
  if (AUTH_ENABLED && req.headers.authorization !== `Bearer ${process.env.BRIDGE_AUTH_TOKEN}`) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws) => {
  const id = randomUUID();
  const chunks = [];
  let bytes = 0;
  let finalized = false;
  let timer = setTimeout(() => finalize('timeout'), SESSION_TIMEOUT_MS);
  console.info(`[${id}] connected`);

  const touch = () => {
    clearTimeout(timer);
    timer = setTimeout(() => finalize('timeout'), SESSION_TIMEOUT_MS);
  };

  const finalize = async (reason) => {
    if (finalized) return;
    finalized = true;
    clearTimeout(timer);
    try {
      if (bytes === 0) {
        await sendResult(ws, { segments: [] });
      } else {
        const audio = Buffer.concat(chunks);
        const wav = await convertToWav(audio);
        const result = await transcribe(wav);
        await sendResult(ws, result);
      }
    } catch (error) {
      console.error(`[${id}] ${reason} failed:`, error.message);
      await sendResult(ws, { segments: [], error: 'transcription_failed' });
    } finally {
      if (ws.readyState === ws.OPEN) ws.close(1000, 'transcription complete');
    }
  };

  ws.on('message', (data, isBinary) => {
    touch();
    if (isBinary) {
      const chunk = Buffer.from(data);
      bytes += chunk.length;
      if (bytes > MAX_AUDIO_BYTES) {
        finalize('size limit');
        return;
      }
      chunks.push(chunk);
      return;
    }
    try {
      const message = JSON.parse(data.toString());
      if (message?.type === 'CloseStream') finalize('close stream');
    } catch {
      console.warn(`[${id}] ignored invalid control message`);
    }
  });
  ws.on('close', () => clearTimeout(timer));
  ws.on('error', (error) => console.warn(`[${id}] websocket error:`, error.message));
});

async function sendResult(ws, result) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(result));
}

async function convertToWav(input) {
  const ffmpeg = spawn('ffmpeg', [
    '-hide_banner', '-loglevel', 'error', '-f', 'opus', '-i', 'pipe:0',
    '-ac', '1', '-ar', '16000', '-f', 'wav', 'pipe:1'
  ]);
  const output = [];
  ffmpeg.stdout.on('data', (chunk) => output.push(chunk));
  ffmpeg.stdin.end(input);
  const [code] = await once(ffmpeg, 'close');
  if (code !== 0) throw new Error('ffmpeg could not decode the Omi audio stream');
  return Buffer.concat(output);
}

async function transcribe(wav) {
  if (!process.env.COHERE_API_KEY) throw new Error('COHERE_API_KEY is not configured');
  const form = new FormData();
  form.append('model', process.env.COHERE_STT_MODEL || 'cohere-transcribe-03-2026');
  form.append('language', process.env.COHERE_LANGUAGE || 'en');
  form.append('file', new Blob([wav], { type: 'audio/wav' }), 'audio.wav');

  const response = await fetch(
    process.env.COHERE_STT_ENDPOINT || 'https://api.cohere.com/v2/audio/transcriptions',
    { method: 'POST', headers: { Authorization: `Bearer ${process.env.COHERE_API_KEY}` }, body: form }
  );
  const body = await response.text();
  let payload;
  try { payload = JSON.parse(body); } catch { payload = {}; }
  if (!response.ok) throw new Error(`Cohere ${response.status}: ${body.slice(0, 500)}`);

  const rawSegments = Array.isArray(payload.segments) ? payload.segments : null;
  const text = payload.text || payload.transcript || payload.output?.text || '';
  const segments = rawSegments
    ? rawSegments.map((segment, index) => ({
        text: String(segment.text || segment.transcript || ''),
        speaker: segment.speaker || `SPEAKER_${String(index).padStart(2, '0')}`,
        start: Number(segment.start || 0),
        end: Number(segment.end || 0)
      })).filter((segment) => segment.text)
    : text ? [{ text: String(text), speaker: 'SPEAKER_00', start: 0, end: 0 }] : [];
  return { segments };
}

server.listen(PORT, '0.0.0.0', () => console.info(`listening on ${PORT}`));
