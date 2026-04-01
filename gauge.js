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

function describeArc(cx, cy, radius, startAngle, endAngle) {
  const start = polarToCartesian(cx, cy, radius, endAngle);
  const end = polarToCartesian(cx, cy, radius, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return [
    "M", start.x, start.y,
    "A", radius, radius, 0, largeArcFlag, 0, end.x, end.y
  ].join(" ");
}

function createTickMarkup({ cx, cy, radius, tickCount, activeCount }) {
  const tickParts = [];
  for (let index = 0; index <= tickCount; index += 1) {
    const angle = -90 + (180 / tickCount) * index;
    const outer = polarToCartesian(cx, cy, radius, angle);
    const inner = polarToCartesian(cx, cy, radius - (index % 5 === 0 ? 14 : 10), angle);
    const isActive = index <= activeCount;
    const tickClass = isActive ? "gauge-tick gauge-tick-active" : "gauge-tick";
    tickParts.push(
      `<line class="${tickClass}" x1="${inner.x.toFixed(2)}" y1="${inner.y.toFixed(2)}" x2="${outer.x.toFixed(2)}" y2="${outer.y.toFixed(2)}"></line>`
    );
  }
  return tickParts.join("");
}

function createGaugeMarkup({ value, label }) {
  const normalized = clampGaugeValue(value);
  const activeTickCount = Math.round((normalized / 100) * 34);
  const endAngle = -90 + (normalized / 100) * 180;
  const progressArc = describeArc(92, 98, 48, -90, endAngle);
  const trackArc = describeArc(92, 98, 48, -90, 90);

  return `
    <div class="gauge-shell" role="img" aria-label="${label} ${normalized}">
      <div class="gauge-label">${label}</div>
      <svg class="gauge-svg" viewBox="0 0 185 122" aria-hidden="true">
        ${createTickMarkup({ cx: 92, cy: 98, radius: 60, tickCount: 34, activeCount: activeTickCount })}
        <path class="gauge-track" d="${trackArc}"></path>
        <path class="gauge-progress" d="${progressArc}"></path>
        <text class="gauge-now" x="92" y="28" text-anchor="middle">now</text>
        <text class="gauge-min" x="42" y="101">0</text>
        <text class="gauge-mid" x="90" y="61">50</text>
        <text class="gauge-max" x="128" y="101">100</text>
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
