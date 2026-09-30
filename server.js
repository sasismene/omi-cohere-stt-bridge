import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import dotenv from 'dotenv';
import { WebSocketServer } from 'ws';

dotenv.config();
console.log('✓ Environment variables loaded');

const PORT = Number(process.env.PORT || 10000);
const MAX_AUDIO_BYTES = Number(process.env.MAX_AUDIO_BYTES || 25_000_000);
const SESSION_TIMEOUT_MS = Number(process.env.SESSION_TIMEOUT_MS || 90_000);
const AUDIO_INPUT_FORMAT = process.env.AUDIO_INPUT_FORMAT || 'pcm_s16le';
const AUDIO_SAMPLE_RATE = Number(process.env.AUDIO_SAMPLE_RATE || 16000);
const AUDIO_CHANNELS = Number(process.env.AUDIO_CHANNELS || 1);
const AUTH_ENABLED = process.env.BRIDGE_AUTH_ENABLED === 'true';

console.log(`[CONFIG] PORT: ${PORT} (type: ${typeof PORT})`);
console.log(`[CONFIG] MAX_AUDIO_BYTES: ${MAX_AUDIO_BYTES} (type: ${typeof MAX_AUDIO_BYTES})`);
console.log(`[CONFIG] SESSION_TIMEOUT_MS: ${SESSION_TIMEOUT_MS} (type: ${typeof SESSION_TIMEOUT_MS})`);
console.log(`[CONFIG] AUDIO_INPUT_FORMAT: ${AUDIO_INPUT_FORMAT} (type: ${typeof AUDIO_INPUT_FORMAT})`);
console.log(`[CONFIG] AUDIO_SAMPLE_RATE: ${AUDIO_SAMPLE_RATE} (type: ${typeof AUDIO_SAMPLE_RATE})`);
console.log(`[CONFIG] AUDIO_CHANNELS: ${AUDIO_CHANNELS} (type: ${typeof AUDIO_CHANNELS})`);
console.log(`[CONFIG] AUTH_ENABLED: ${AUTH_ENABLED} (type: ${typeof AUTH_ENABLED})`);

const server = http.createServer((req, res) => {
  console.log(`[HTTP] Incoming request: ${req.method} ${req.url}`);
  const pathname = new URL(req.url || '/', 'http://localhost').pathname;
  console.log(`[HTTP] Parsed pathname: "${pathname}" (type: ${typeof pathname})`);
  
  if (pathname === '/health') {
    console.log('[HTTP] Health check endpoint hit');
    res.writeHead(200, { 'content-type': 'application/json' });
    const healthResponse = {
      ok: true,
      service: 'omi-cohere-stt-bridge',
      audioInputFormat: AUDIO_INPUT_FORMAT,
      sampleRate: AUDIO_SAMPLE_RATE,
      channels: AUDIO_CHANNELS,
    };
    console.log(`[HTTP] Health response: ${JSON.stringify(healthResponse)}`);
    res.end(JSON.stringify(healthResponse));
    return;
  }
  console.log('[HTTP] 404 Not found');
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_AUDIO_BYTES });
console.log(`[WS] WebSocketServer created with maxPayload: ${MAX_AUDIO_BYTES}`);

server.on('upgrade', (req, socket, head) => {
  console.log(`[UPGRADE] Upgrade request: ${req.url}`);
  const pathname = new URL(req.url || '/', 'http://localhost').pathname;
  console.log(`[UPGRADE] Parsed pathname: "${pathname}" (type: ${typeof pathname})`);
  
  if (pathname !== '/stt') {
    console.warn(`[UPGRADE] Invalid path "${pathname}", expected "/stt" - destroying socket`);
    socket.destroy();
    return;
  }

  if (AUTH_ENABLED) {
    console.log('[UPGRADE] AUTH_ENABLED is true, checking authorization');
    const authHeader = req.headers.authorization;
    const expectedToken = `Bearer ${process.env.BRIDGE_AUTH_TOKEN}`;
    console.log(`[UPGRADE] Authorization header present: ${Boolean(authHeader)} (type: ${typeof authHeader})`);
    console.log(`[UPGRADE] Expected token length: ${expectedToken.length}`);
    console.log(`[UPGRADE] Received token length: ${authHeader ? authHeader.length : 'N/A'}`);
    
    if (authHeader !== expectedToken) {
      console.warn('[UPGRADE] Authorization failed - token mismatch');
      socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    console.log('[UPGRADE] Authorization successful');
  } else {
    console.log('[UPGRADE] AUTH_ENABLED is false, skipping authorization check');
  }

  console.log('[UPGRADE] Handling upgrade to WebSocket');
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws) => {
  const id = randomUUID();
  console.log(`\n[SESSION ${id}] ✓ Connected - format=${AUDIO_INPUT_FORMAT}`);
  console.log(`[SESSION ${id}] WebSocket readyState: ${ws.readyState} (type: ${typeof ws.readyState})`);
  
  const chunks = [];
  console.log(`[SESSION ${id}] chunks array initialized (type: ${Array.isArray(chunks) ? 'Array' : 'not an array'})`);
  
  let bytes = 0;
  console.log(`[SESSION ${id}] bytes initialized: ${bytes} (type: ${typeof bytes})`);
  
  let finalized = false;
  console.log(`[SESSION ${id}] finalized flag initialized: ${finalized} (type: ${typeof finalized})`);
  
  let timer = setTimeout(() => void finalize('timeout'), SESSION_TIMEOUT_MS);
  console.log(`[SESSION ${id}] timeout timer set for ${SESSION_TIMEOUT_MS}ms (timer ID: ${timer})`);

  const touch = () => {
    console.log(`[SESSION ${id}] touch() called - resetting timeout`);
    clearTimeout(timer);
    timer = setTimeout(() => void finalize('timeout'), SESSION_TIMEOUT_MS);
    console.log(`[SESSION ${id}] new timeout timer set (timer ID: ${timer})`);
  };

  const finalize = async (reason) => {
    console.log(`\n[SESSION ${id}] finalize("${reason}") called`);
    console.log(`[SESSION ${id}] finalized flag: ${finalized} (type: ${typeof finalized})`);
    
    if (finalized) {
      console.log(`[SESSION ${id}] Already finalized, returning early`);
      return;
    }
    
    finalized = true;
    console.log(`[SESSION ${id}] Setting finalized to true`);
    
    clearTimeout(timer);
    console.log(`[SESSION ${id}] Timeout timer cleared`);
    
    try {
      console.log(`[SESSION ${id}] bytes accumulated: ${bytes} (type: ${typeof bytes})`);
      console.log(`[SESSION ${id}] chunks count: ${chunks.length} (is Array: ${Array.isArray(chunks)})`);
      
      if (bytes === 0) {
        console.log(`[SESSION ${id}] No audio data received, returning empty segments`);
        const result = { segments: [] };
        await sendResult(ws, result);
      } else {
        console.log(`[SESSION ${id}] Converting ${bytes} bytes to WAV format...`);
        const wavBuffer = await convertToWav(Buffer.concat(chunks));
        console.log(`[SESSION ${id}] WAV conversion complete - size: ${wavBuffer.length} bytes (type: ${Buffer.isBuffer(wavBuffer) ? 'Buffer' : 'not a buffer'})`);
        
        console.log(`[SESSION ${id}] Starting transcription...`);
        const result = await transcribe(wavBuffer);
        console.log(`[SESSION ${id}] Transcription complete - segments: ${result.segments.length} (type: ${Array.isArray(result.segments) ? 'Array' : 'not an array'})`);
        console.log(`[SESSION ${id}] Result: ${JSON.stringify(result)}`);
        
        await sendResult(ws, result);
      }
    } catch (error) {
      console.error(`[SESSION ${id}] ${reason} failed:`, error.message);
      console.error(`[SESSION ${id}] Error type: ${error.constructor.name}`);
      console.error(`[SESSION ${id}] Error stack:`, error.stack);
      
      const errorResult = { segments: [], error: 'transcription_failed' };
      await sendResult(ws, errorResult);
    } finally {
      console.log(`[SESSION ${id}] Checking WebSocket readyState: ${ws.readyState}`);
      if (ws.readyState === 1) {
        console.log(`[SESSION ${id}] WebSocket is open, closing with code 1000`);
        ws.close(1000, 'transcription complete');
      } else {
        console.log(`[SESSION ${id}] WebSocket already closed (readyState: ${ws.readyState})`);
      }
      console.log(`[SESSION ${id}] ✓ Session finalized\n`);
    }
  };

  ws.on('message', (data, isBinary) => {
    console.log(`[SESSION ${id}] Message received - isBinary: ${isBinary} (type: ${typeof isBinary}), data length: ${data.length}`);
    touch();
    
    if (isBinary) {
      console.log(`[SESSION ${id}] Processing binary message`);
      const chunk = Buffer.from(data);
      console.log(`[SESSION ${id}] Chunk created (type: ${Buffer.isBuffer(chunk) ? 'Buffer' : 'not a buffer'}, length: ${chunk.length})`);
      
      bytes += chunk.length;
      console.log(`[SESSION ${id}] Total bytes now: ${bytes} (type: ${typeof bytes}), MAX_AUDIO_BYTES: ${MAX_AUDIO_BYTES}`);
      
      if (bytes > MAX_AUDIO_BYTES) {
        console.warn(`[SESSION ${id}] Size limit exceeded! ${bytes} > ${MAX_AUDIO_BYTES}`);
        void finalize('size limit');
        return;
      }
      
      chunks.push(chunk);
      console.log(`[SESSION ${id}] Chunk added - total chunks: ${chunks.length}`);
      return;
    }

    // Omi clients use {"type":"CloseStream"}; tolerate the plain string too.
    console.log(`[SESSION ${id}] Processing text message`);
    const text = data.toString('utf8').trim();
    console.log(`[SESSION ${id}] Text: "${text}" (type: ${typeof text}, length: ${text.length})`);
    
    let closeStream = text === 'CloseStream' || text === '"CloseStream"';
    console.log(`[SESSION ${id}] Simple string match - closeStream: ${closeStream} (type: ${typeof closeStream})`);
    
    if (!closeStream) {
      console.log(`[SESSION ${id}] Attempting JSON parse...`);
      try {
        const message = JSON.parse(text);
        console.log(`[SESSION ${id}] JSON parsed successfully (type: ${typeof message})`);
        console.log(`[SESSION ${id}] Parsed value: ${JSON.stringify(message)}`);
        closeStream = message === 'CloseStream' || message?.type === 'CloseStream';
        console.log(`[SESSION ${id}] JSON match - closeStream: ${closeStream} (type: ${typeof closeStream})`);
      } catch (err) {
        console.warn(`[SESSION ${id}] JSON parse failed: ${err.message}`);
        console.warn(`[SESSION ${id}] Ignored text message: ${text}`);
      }
    }
    
    if (closeStream) {
      console.log(`[SESSION ${id}] CloseStream message detected, finalizing`);
      void finalize('close stream');
    } else {
      console.log(`[SESSION ${id}] Message was not a close command, waiting for more data`);
    }
  });

  ws.on('close', () => {
    console.log(`[SESSION ${id}] WebSocket closed by client`);
    clearTimeout(timer);
  });
  
  ws.on('error', (error) => {
    console.warn(`[SESSION ${id}] WebSocket error: ${error.message}`);
    console.warn(`[SESSION ${id}] Error type: ${error.constructor.name}`);
  });
});

function sendResult(ws, result) {
  console.log(`[SEND] sendResult() called`);
  console.log(`[SEND] WebSocket readyState: ${ws.readyState} (type: ${typeof ws.readyState})`);
  console.log(`[SEND] Result to send: ${JSON.stringify(result)} (type: ${typeof result})`);
  
  if (ws.readyState !== 1) {
    console.log(`[SEND] WebSocket not open (readyState: ${ws.readyState}), skipping send`);
    return Promise.resolve();
  }
  
  const payload = Buffer.from(JSON.stringify(result), 'utf8');
  console.log(`[SEND] Payload created (type: ${Buffer.isBuffer(payload) ? 'Buffer' : 'not a buffer'}, size: ${payload.length} bytes)`);
  
  return new Promise((resolve, reject) => {
    ws.send(payload, { binary: true }, (error) => {
      if (error) {
        console.error(`[SEND] Failed to send: ${error.message}`);
        console.error(`[SEND] Error type: ${error.constructor.name}`);
        reject(error);
      } else {
        console.log(`[SEND] ✓ Result sent successfully`);
        resolve();
      }
    });
  });
}

async function convertToWav(input) {
  console.log(`\n[FFMPEG] convertToWav() called`);
  console.log(`[FFMPEG] Input type: ${Buffer.isBuffer(input) ? 'Buffer' : 'not a buffer'}, size: ${input.length} bytes`);
  
  const args = ['-hide_banner', '-loglevel', 'error'];
  console.log(`[FFMPEG] Initial args: ${JSON.stringify(args)}`);
  
  if (AUDIO_INPUT_FORMAT === 'opus') {
    console.log(`[FFMPEG] AUDIO_INPUT_FORMAT is 'opus', adding opus flags`);
    args.push('-f', 'opus');
  } else if (AUDIO_INPUT_FORMAT === 'pcm_s16le' || AUDIO_INPUT_FORMAT === 's16le') {
    console.log(`[FFMPEG] AUDIO_INPUT_FORMAT is '${AUDIO_INPUT_FORMAT}', adding PCM flags`);
    args.push('-f', 's16le', '-ar', String(AUDIO_SAMPLE_RATE), '-ac', String(AUDIO_CHANNELS));
  } else {
    const error = new Error(`Unsupported AUDIO_INPUT_FORMAT: ${AUDIO_INPUT_FORMAT}`);
    console.error(`[FFMPEG] ${error.message}`);
    throw error;
  }
  
  args.push('-i', 'pipe:0', '-ac', '1', '-ar', '16000', '-f', 'wav', 'pipe:1');
  console.log(`[FFMPEG] Final args: ${JSON.stringify(args)}`);

  console.log(`[FFMPEG] Spawning ffmpeg process...`);
  const ffmpeg = spawn('ffmpeg', args);
  console.log(`[FFMPEG] ffmpeg process spawned (PID: ${ffmpeg.pid}, type: ${typeof ffmpeg.pid})`);
  
  const output = [];
  console.log(`[FFMPEG] output array initialized (type: ${Array.isArray(output) ? 'Array' : 'not an array'})`);
  
  const errors = [];
  console.log(`[FFMPEG] errors array initialized (type: ${Array.isArray(errors) ? 'Array' : 'not an array'})`);
  
  ffmpeg.stdout.on('data', (chunk) => {
    console.log(`[FFMPEG] stdout data received: ${chunk.length} bytes`);
    output.push(chunk);
    console.log(`[FFMPEG] output array now contains ${output.length} chunks`);
  });
  
  ffmpeg.stderr.on('data', (chunk) => {
    console.log(`[FFMPEG] stderr data received: ${chunk.length} bytes`);
    errors.push(chunk);
    console.log(`[FFMPEG] errors array now contains ${errors.length} chunks`);
  });
  
  console.log(`[FFMPEG] Writing input to ffmpeg stdin (${input.length} bytes)...`);
  ffmpeg.stdin.end(input);
  console.log(`[FFMPEG] Input write complete, stdin closed`);
  
  console.log(`[FFMPEG] Waiting for ffmpeg process to close...`);
  const [code] = await once(ffmpeg, 'close');
  console.log(`[FFMPEG] Process closed with code: ${code} (type: ${typeof code})`);
  
  if (code !== 0) {
    const errorMessage = Buffer.concat(errors).toString().slice(0, 300);
    const error = new Error(`ffmpeg failed: ${errorMessage}`);
    console.error(`[FFMPEG] ${error.message}`);
    throw error;
  }
  
  const result = Buffer.concat(output);
  console.log(`[FFMPEG] ✓ Conversion complete - output size: ${result.length} bytes (type: ${Buffer.isBuffer(result) ? 'Buffer' : 'not a buffer'})\n`);
  return result;
}

async function transcribe(wav) {
  console.log(`\n[TRANSCRIBE] transcribe() called`);
  console.log(`[TRANSCRIBE] Input type: ${Buffer.isBuffer(wav) ? 'Buffer' : 'not a buffer'}, size: ${wav.length} bytes`);
  
  if (!process.env.COHERE_API_KEY) {
    const error = new Error('COHERE_API_KEY is not configured');
    console.error(`[TRANSCRIBE] ${error.message}`);
    throw error;
  }
  console.log(`[TRANSCRIBE] COHERE_API_KEY is configured`);
  
  const form = new FormData();
  console.log(`[TRANSCRIBE] FormData created (type: ${form.constructor.name})`);
  
  const model = process.env.COHERE_STT_MODEL || 'cohere-transcribe-03-2026';
  console.log(`[TRANSCRIBE] Model: "${model}" (type: ${typeof model})`);
  form.append('model', model);
  
  const language = process.env.COHERE_LANGUAGE || 'en';
  console.log(`[TRANSCRIBE] Language: "${language}" (type: ${typeof language})`);
  form.append('language', language);
  
  const blob = new Blob([wav], { type: 'audio/wav' });
  console.log(`[TRANSCRIBE] Blob created (type: ${blob.constructor.name}, size: ${blob.size} bytes)`);
  form.append('file', blob, 'audio.wav');

  const endpoint = process.env.COHERE_STT_ENDPOINT || 'https://api.cohere.com/v2/audio/transcriptions';
  console.log(`[TRANSCRIBE] Endpoint: "${endpoint}" (type: ${typeof endpoint})`);
  console.log(`[TRANSCRIBE] Sending request to Cohere API...`);
  
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { Authorization: `Bearer ${process.env.COHERE_API_KEY}` },
    body: form,
  });
  console.log(`[TRANSCRIBE] Response received - status: ${response.status} (type: ${typeof response.status}), ok: ${response.ok} (type: ${typeof response.ok})`);

  const body = await response.text();
  console.log(`[TRANSCRIBE] Response body length: ${body.length} bytes (type: ${typeof body})`);
  console.log(`[TRANSCRIBE] Response body: ${body.slice(0, 500)}`);
  
  let payload;
  try {
    payload = JSON.parse(body);
    console.log(`[TRANSCRIBE] JSON parsed successfully (type: ${typeof payload})`);
    console.log(`[TRANSCRIBE] Payload keys: ${Object.keys(payload).join(', ')}`);
  } catch (err) {
    console.warn(`[TRANSCRIBE] JSON parse failed: ${err.message}, using empty object`);
    payload = {};
  }
  
  if (!response.ok) {
    const error = new Error(`Cohere ${response.status}: ${body.slice(0, 500)}`);
    console.error(`[TRANSCRIBE] ${error.message}`);
    throw error;
  }

  const rawSegments = Array.isArray(payload.segments) ? payload.segments : null;
  console.log(`[TRANSCRIBE] rawSegments present: ${rawSegments !== null} (type: ${Array.isArray(rawSegments) ? 'Array' : typeof rawSegments})`);
  if (rawSegments) {
    console.log(`[TRANSCRIBE] rawSegments count: ${rawSegments.length}`);
  }
  
  const text = payload.text || payload.transcript || payload.output?.text || '';
  console.log(`[TRANSCRIBE] Text extracted: "${text.slice(0, 100)}" (type: ${typeof text}, length: ${text.length})`);
  
  const segments = rawSegments
    ? rawSegments.map((segment, index) => {
        const mappedSegment = {
          text: String(segment.text || segment.transcript || ''),
          speaker: segment.speaker || `SPEAKER_${String(index).padStart(2, '0')}`,
          start: Number(segment.start || 0),
          end: Number(segment.end || 0),
        };
        console.log(`[TRANSCRIBE] Segment ${index}: text="${mappedSegment.text.slice(0, 50)}", speaker="${mappedSegment.speaker}", start=${mappedSegment.start}, end=${mappedSegment.end}`);
        return mappedSegment;
      }).filter((segment) => {
        const included = Boolean(segment.text);
        console.log(`[TRANSCRIBE] Filtering segment: "${segment.text.slice(0, 30)}" - included: ${included}`);
        return included;
      })
    : text ? [{ text: String(text), speaker: 'SPEAKER_00', start: 0, end: 0 }] : [];
  
  console.log(`[TRANSCRIBE] Final segments: ${segments.length} (type: ${Array.isArray(segments) ? 'Array' : 'not an array'})`);
  
  const result = { segments };
  console.log(`[TRANSCRIBE] ✓ Transcription complete - result: ${JSON.stringify(result).slice(0, 200)}\n`);
  return result;
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`✓ Server listening on http://0.0.0.0:${PORT}`);
  console.log(`  Health check: GET http://localhost:${PORT}/health`);
  console.log(`  STT endpoint: WS ws://localhost:${PORT}/stt`);
  console.log(`${'='.repeat(60)}\n`);
});
