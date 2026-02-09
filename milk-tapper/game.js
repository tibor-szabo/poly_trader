const canvas = document.getElementById('game');
const ctx = canvas.getContext('2d');

const scoreEl = document.getElementById('score');
const livesEl = document.getElementById('lives');
const levelEl = document.getElementById('level');
const highEl = document.getElementById('high');
const serveBtn = document.getElementById('serveBtn');
const startBtn = document.getElementById('startBtn');
const muteBtn = document.getElementById('muteBtn');

const W = canvas.width;
const H = canvas.height;
const lanes = 4;
const laneY = Array.from({ length: lanes }, (_, i) => 90 + i * 110);
const barX = 120;
const endX = W - 90;

let gameState = 'menu'; // menu | playing | gameover
let score = 0;
let lives = 3;
let level = 1;
let bartenderLane = 0;
let kids = [];
let milks = [];
let spawnTimer = 0;
let spawnEvery = 1200;
let lastTime = 0;
let highScore = Number(localStorage.getItem('milkTapperHighScore') || 0);
let soundOn = true;

let audioCtx;
function beep(freq = 440, dur = 0.06, type = 'square', gain = 0.05) {
  if (!soundOn) return;
  if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  if (audioCtx.state === 'suspended') audioCtx.resume();
  const osc = audioCtx.createOscillator();
  const g = audioCtx.createGain();
  osc.type = type;
  osc.frequency.value = freq;
  g.gain.value = gain;
  osc.connect(g);
  g.connect(audioCtx.destination);
  osc.start();
  osc.stop(audioCtx.currentTime + dur);
}

function updateHud() {
  scoreEl.textContent = `Score: ${score}`;
  livesEl.textContent = `Lives: ${lives}`;
  levelEl.textContent = `Level: ${level}`;
  highEl.textContent = `High: ${highScore}`;
}

function resetGame() {
  score = 0;
  lives = 3;
  level = 1;
  bartenderLane = 0;
  kids = [];
  milks = [];
  spawnTimer = 0;
  spawnEvery = 1200;
  gameState = 'playing';
  lastTime = 0;
  updateHud();
}

function startGame() {
  resetGame();
  beep(660, 0.07);
}

function saveHigh() {
  if (score > highScore) {
    highScore = score;
    localStorage.setItem('milkTapperHighScore', String(highScore));
  }
}

function serveMilk() {
  if (gameState !== 'playing') return;
  milks.push({ lane: bartenderLane, x: barX + 30, speed: 300 + level * 30 });
  beep(820, 0.04);
}

function spawnKid() {
  const lane = Math.floor(Math.random() * lanes);
  kids.push({ lane, x: endX, speed: 35 + level * 9 + Math.random() * 15 });
}

function difficultyRamp() {
  level = 1 + Math.floor(score / 15);
  spawnEvery = Math.max(420, 1200 - (level - 1) * 80);
}

function loseLife() {
  lives--;
  beep(180, 0.11, 'sawtooth', 0.06);
  updateHud();
  if (lives <= 0) {
    gameState = 'gameover';
    saveHigh();
    updateHud();
    beep(120, 0.2, 'triangle', 0.07);
  }
}

function update(dtMs) {
  if (gameState !== 'playing') return;
  const dt = dtMs / 1000;

  spawnTimer += dtMs;
  if (spawnTimer >= spawnEvery) {
    spawnTimer = 0;
    spawnKid();
  }

  for (const k of kids) k.x -= k.speed * dt;
  for (const m of milks) m.x += m.speed * dt;

  for (let i = kids.length - 1; i >= 0; i--) {
    if (kids[i].x <= barX + 22) {
      kids.splice(i, 1);
      loseLife();
    }
  }

  for (let i = milks.length - 1; i >= 0; i--) {
    const m = milks[i];
    if (m.x > endX + 20) {
      milks.splice(i, 1);
      continue;
    }
    for (let j = kids.length - 1; j >= 0; j--) {
      const k = kids[j];
      if (k.lane === m.lane && Math.abs(k.x - m.x) < 22) {
        kids.splice(j, 1);
        milks.splice(i, 1);
        score += 1;
        difficultyRamp();
        if (score > highScore) {
          highScore = score;
          localStorage.setItem('milkTapperHighScore', String(highScore));
        }
        updateHud();
        beep(980, 0.05, 'square', 0.05);
        break;
      }
    }
  }
}

// simple pixel-sprite helper
function pixelSprite(x, y, px, palette, map) {
  for (let r = 0; r < map.length; r++) {
    for (let c = 0; c < map[r].length; c++) {
      const ch = map[r][c];
      if (ch === '.') continue;
      ctx.fillStyle = palette[ch] || '#fff';
      ctx.fillRect(x + c * px, y + r * px, px, px);
    }
  }
}

const bartenderMap = [
  '..aaa..',
  '.abbb..',
  '.accc..',
  '.acccdd',
  '.aeeee.',
  '.aeeee.',
  '..f..f.'
];
const bartenderPalette = { a: '#f5c89f', b: '#734f96', c: '#1f2d3d', d: '#ffffff', e: '#2c3e50', f: '#111111' };

const kidMap = [
  '..aa..',
  '.abca.',
  '.adda.',
  '.aeea.',
  '..ff..'
];
const kidPalette = { a: '#8bd3dd', b: '#f4d19b', c: '#1a1a1a', d: '#5d7092', e: '#6fa8dc', f: '#222' };

function drawLane(y) {
  ctx.strokeStyle = '#78839b';
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.moveTo(barX, y + 22);
  ctx.lineTo(endX, y + 22);
  ctx.stroke();
}

function drawBartender() {
  const y = laneY[bartenderLane];
  pixelSprite(barX - 34, y - 28, 8, bartenderPalette, bartenderMap);
  ctx.font = '16px monospace';
  ctx.fillText('🥛', barX + 16, y + 6);
}

function drawKid(k) {
  const y = laneY[k.lane];
  pixelSprite(k.x - 18, y - 20, 7, kidPalette, kidMap);
}

function drawMilk(m) {
  const y = laneY[m.lane];
  ctx.fillStyle = '#f0f8ff';
  ctx.fillRect(m.x - 6, y - 8, 12, 14);
  ctx.fillStyle = '#dcefff';
  ctx.fillRect(m.x - 4, y - 6, 8, 3);
}

function drawOverlay(title, line2) {
  ctx.fillStyle = 'rgba(16,19,26,0.76)';
  ctx.fillRect(0, 0, W, H);
  ctx.fillStyle = '#fff';
  ctx.textAlign = 'center';
  ctx.font = 'bold 44px system-ui';
  ctx.fillText(title, W / 2, H / 2 - 40);
  ctx.font = '22px system-ui';
  ctx.fillText(line2, W / 2, H / 2 + 6);
  ctx.font = '16px system-ui';
  ctx.fillText(`High Score: ${highScore}`, W / 2, H / 2 + 38);
  ctx.textAlign = 'left';
}

function drawScene() {
  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = '#2a313f';
  ctx.fillRect(0, 0, W, H);

  ctx.fillStyle = '#364056';
  ctx.fillRect(barX - 46, 40, 14, H - 80);

  for (const y of laneY) drawLane(y);
  drawBartender();
  kids.forEach(drawKid);
  milks.forEach(drawMilk);

  if (gameState === 'menu') drawOverlay('Milk Tapper', 'Press START / ENTER');
  if (gameState === 'gameover') drawOverlay('Game Over', 'Press START / ENTER');
}

function tick(ts) {
  if (!lastTime) lastTime = ts;
  const dt = Math.min(50, ts - lastTime);
  lastTime = ts;
  update(dt);
  drawScene();
  requestAnimationFrame(tick);
}

window.addEventListener('keydown', (e) => {
  if (e.key === 'ArrowUp') bartenderLane = Math.max(0, bartenderLane - 1);
  if (e.key === 'ArrowDown') bartenderLane = Math.min(lanes - 1, bartenderLane + 1);
  if (e.key === ' ') {
    e.preventDefault();
    serveMilk();
  }
  if (e.key === 'Enter' && gameState !== 'playing') startGame();
});

serveBtn.addEventListener('click', serveMilk);
startBtn.addEventListener('click', startGame);
muteBtn.addEventListener('click', () => {
  soundOn = !soundOn;
  muteBtn.textContent = `SOUND: ${soundOn ? 'ON' : 'OFF'}`;
});

canvas.addEventListener('click', (e) => {
  if (gameState !== 'playing') {
    startGame();
    return;
  }
  const rect = canvas.getBoundingClientRect();
  const y = (e.clientY - rect.top) * (canvas.height / rect.height);
  if (y < canvas.height / 2) bartenderLane = Math.max(0, bartenderLane - 1);
  else bartenderLane = Math.min(lanes - 1, bartenderLane + 1);
});

updateHud();
requestAnimationFrame(tick);
