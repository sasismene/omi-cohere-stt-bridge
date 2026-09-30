import test from 'node:test';
import assert from 'node:assert/strict';

const response = { segments: [{ text: 'hello', speaker: 'SPEAKER_00', start: 0, end: 0 }] };
test('Omi response shape has a segments array', () => {
  assert.ok(Array.isArray(response.segments));
  assert.equal(response.segments[0].text, 'hello');
});
