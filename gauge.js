function clampGaugeValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.max(0, Math.min(100, numeric));
}

function polarToCartesian(cx, cy, radius, angleDeg) {
  const angleRad = ((angleDeg - 90) * Math.PI) / 180;
  return {
    x: cx + radius * Math.cos(angleRad),
    y: cy + radius * Math.sin(angleRad)
  };
}

function createTickMarkup({ cx, cy, radius, tickCount, activeCount }) {
  const tickParts = [];
  for (let index = 0; index <= tickCount; index += 1) {
    const angle = -90 + (180 / tickCount) * index;
    const outer = polarToCartesian(cx, cy, radius, angle);
    const inner = polarToCartesian(cx, cy, radius - (index % 4 === 0 ? 18 : 13), angle);
    const isActive = index <= activeCount;
    const tickClass = isActive ? "gauge-tick gauge-tick-active" : "gauge-tick";
    tickParts.push(
      `<line class="${tickClass}" x1="${inner.x.toFixed(2)}" y1="${inner.y.toFixed(2)}" x2="${outer.x.toFixed(2)}" y2="${outer.y.toFixed(2)}"></line>`
    );
  }
  return tickParts.join("");
}

function createIndicatorMarkup({ cx, cy, radius, value }) {
  const angle = -90 + (clampGaugeValue(value) / 100) * 180;
  const angleRad = ((angle - 90) * Math.PI) / 180;
  const directionX = Math.cos(angleRad);
  const directionY = Math.sin(angleRad);
  const normalX = -directionY;
  const normalY = directionX;
  const tip = polarToCartesian(cx, cy, radius + 2, angle);
  const baseCenter = polarToCartesian(cx, cy, radius + 11, angle);
  const baseLeft = {
    x: baseCenter.x + normalX * 4,
    y: baseCenter.y + normalY * 4
  };
  const baseRight = {
    x: baseCenter.x - normalX * 4,
    y: baseCenter.y - normalY * 4
  };
  const textAnchor = tip.x < cx - 18 ? "start" : tip.x > cx + 18 ? "end" : "middle";
  const textX = textAnchor === "middle" ? tip.x : textAnchor === "start" ? tip.x + 2 : tip.x - 2;
  const textY = tip.y - 10;

  return `
    <polygon class="gauge-indicator" points="${tip.x.toFixed(2)},${tip.y.toFixed(2)} ${baseLeft.x.toFixed(2)},${baseLeft.y.toFixed(2)} ${baseRight.x.toFixed(2)},${baseRight.y.toFixed(2)}"></polygon>
    <text class="gauge-now" x="${textX.toFixed(2)}" y="${textY.toFixed(2)}" text-anchor="${textAnchor}">now</text>
  `;
}

function createGaugeMarkup({ value, label }) {
  const normalized = clampGaugeValue(value);
  const cx = 92;
  const cy = 108;
  const radius = 72;
  const activeTickCount = Math.round((normalized / 100) * 48);

  return `
    <div class="gauge-shell" role="img" aria-label="${label} ${normalized}">
      <div class="gauge-label">${label}</div>
      <svg class="gauge-svg" viewBox="0 0 185 122" aria-hidden="true">
        ${createTickMarkup({ cx, cy, radius, tickCount: 48, activeCount: activeTickCount })}
        ${createIndicatorMarkup({ cx, cy, radius, value: normalized })}
        <text class="gauge-min" x="10" y="109">0</text>
        <text class="gauge-mid" x="88" y="55">50</text>
        <text class="gauge-max" x="154" y="109">100</text>
      </svg>
      <div class="gauge-value">${Math.round(normalized)}</div>
    </div>
  `;
}

function mountGauge(element, options = {}) {
  if (!element) {
    return null;
  }
  const label = options.label ?? element.dataset.gaugeLabel ?? "Gauge";
  const value = options.value ?? element.dataset.gaugeValue ?? 0;
  element.innerHTML = createGaugeMarkup({ value, label });

  return {
    setValue(nextValue) {
      element.dataset.gaugeValue = String(clampGaugeValue(nextValue));
      element.innerHTML = createGaugeMarkup({
        value: nextValue,
        label
      });
    }
  };
}

window.BeeGauge = {
  mountGauge
};
