const canvas = document.getElementById('game');
const ctx = canvas.getContext('2d');

const scoreEl = document.getElementById('score');
const livesEl = document.getElementById('lives');
const levelEl = document.getElementById('level');
const serveBtn = document.getElementById('serveBtn');
const startBtn = document.getElementById('startBtn');

const W = canvas.width;
const H = canvas.height;
const lanes = 4;
const laneY = Array.from({length: lanes}, (_, i) => 90 + i * 110);
const barX = 120;
const endX = W - 90;

let running = false;
let score = 0;
let lives = 3;
let level = 1;
let bartenderLane = 0;
let kids = [];
let milks = [];
let spawnTimer = 0;
let spawnEvery = 1200;
let lastTime = 0;

function reset() {
  score = 0;
  lives = 3;
  level = 1;
  bartenderLane = 0;
  kids = [];
  milks = [];
  spawnTimer = 0;
  spawnEvery = 1200;
  running = true;
  updateHud();
}

function updateHud() {
  scoreEl.textContent = `Score: ${score}`;
  livesEl.textContent = `Lives: ${lives}`;
  levelEl.textContent = `Level: ${level}`;
}

function serveMilk() {
  if (!running) return;
  milks.push({ lane: bartenderLane, x: barX + 30, speed: 300 + level * 30 });
}

function spawnKid() {
  const lane = Math.floor(Math.random() * lanes);
  kids.push({ lane, x: endX, speed: 35 + level * 9 + Math.random() * 15 });
}

function difficultyRamp() {
  level = 1 + Math.floor(score / 15);
  spawnEvery = Math.max(450, 1200 - (level - 1) * 80);
}

function loseLife() {
  lives--;
  updateHud();
  if (lives <= 0) {
    running = false;
  }
}

function update(dtMs) {
  if (!running) return;
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
      if (k.lane === m.lane && Math.abs(k.x - m.x) < 24) {
        kids.splice(j, 1);
        milks.splice(i, 1);
        score += 1;
        difficultyRamp();
        updateHud();
        break;
      }
    }
  }
}

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
  ctx.fillStyle = '#ffd166';
  ctx.fillRect(barX - 30, y - 26, 28, 52);
  ctx.fillStyle = '#2d3142';
  ctx.fillRect(barX - 8, y - 20, 14, 40);
  ctx.font = '20px system-ui';
  ctx.fillText('🥛', barX + 4, y + 8);
}

function drawKid(k) {
  const y = laneY[k.lane];
  ctx.fillStyle = '#8bd3dd';
  ctx.fillRect(k.x - 12, y - 16, 24, 34);
  ctx.font = '18px system-ui';
  ctx.fillText('🧒', k.x - 10, y + 8);
}

function drawMilk(m) {
  const y = laneY[m.lane];
  ctx.font = '18px system-ui';
  ctx.fillText('🥛', m.x - 8, y + 8);
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

  if (!running) {
    ctx.fillStyle = 'rgba(16,19,26,0.72)';
    ctx.fillRect(0,0,W,H);
    ctx.fillStyle = '#fff';
    ctx.textAlign = 'center';
    ctx.font = 'bold 44px system-ui';
    ctx.fillText(lives <= 0 ? 'Game Over' : 'Milk Tapper', W/2, H/2 - 30);
    ctx.font = '24px system-ui';
    ctx.fillText('Press START / RESTART', W/2, H/2 + 14);
    ctx.textAlign = 'left';
  }
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
  if (e.key === ' ') { e.preventDefault(); serveMilk(); }
});

serveBtn.addEventListener('click', serveMilk);
startBtn.addEventListener('click', () => { lastTime = 0; reset(); });

canvas.addEventListener('click', (e) => {
  const rect = canvas.getBoundingClientRect();
  const y = (e.clientY - rect.top) * (canvas.height / rect.height);
  if (y < canvas.height / 2) bartenderLane = Math.max(0, bartenderLane - 1);
  else bartenderLane = Math.min(lanes - 1, bartenderLane + 1);
});

requestAnimationFrame(tick);
