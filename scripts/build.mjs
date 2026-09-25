import { cp, mkdir, writeFile } from 'node:fs/promises';
const target = new URL('../dist/', import.meta.url);
await mkdir(target, { recursive: true });
await cp(new URL('../public/', import.meta.url), target, { recursive: true });
const signalUrl = process.env.PUBLIC_SIGNAL_URL || '';
if (signalUrl && new URL(signalUrl).protocol !== 'wss:') {
  throw new Error('PUBLIC_SIGNAL_URL must use wss:// for an HTTPS deployment');
}
await writeFile(new URL('config.js', target), `window.APP_CONFIG = ${JSON.stringify({ signalUrl })};\n`);
console.log(signalUrl ? 'Built Vercel frontend.' : 'Built frontend. Set PUBLIC_SIGNAL_URL in Vercel before deployment.');
