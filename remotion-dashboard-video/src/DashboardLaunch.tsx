import {
  AbsoluteFill,
  Easing,
  Sequence,
  interpolate,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";

const ease = Easing.bezier(0.16, 1, 0.3, 1);

const clamp = {
  extrapolateLeft: "clamp" as const,
  extrapolateRight: "clamp" as const,
  easing: ease,
};

const colors = {
  ink: "#202421",
  muted: "#69716a",
  line: "#d9dfd6",
  panel: "#fbfcf7",
  glass: "rgba(255, 255, 255, 0.78)",
  green: "#78b36c",
  honey: "#d6a646",
  blue: "#598ab8",
  coral: "#d7795f",
};

const metricData = [
  {label: "Nectar", value: 72, color: colors.honey},
  {label: "Activity", value: 58, color: colors.blue},
  {label: "Flowering", value: 41, color: colors.green},
  {label: "Mismatch", value: 63, color: colors.coral},
];

const mapPins = [
  {x: 43, y: 48, r: 80, color: colors.honey},
  {x: 54, y: 44, r: 56, color: colors.green},
  {x: 48, y: 57, r: 68, color: colors.blue},
  {x: 61, y: 54, r: 42, color: colors.coral},
];

const fade = (frame: number, start: number, end: number) =>
  interpolate(frame, [start, end], [0, 1], clamp);

const fadeOut = (frame: number, start: number, end: number) =>
  interpolate(frame, [start, end], [1, 0], clamp);

const windowed = (frame: number, enterStart: number, enterEnd: number, exitStart: number, exitEnd: number) =>
  Math.min(fade(frame, enterStart, enterEnd), fadeOut(frame, exitStart, exitEnd));

const Header = () => {
  const frame = useCurrentFrame();
  const progress = fade(frame, 18, 50);

  return (
    <div
      style={{
        position: "absolute",
        top: 34,
        right: 42,
        display: "flex",
        gap: 12,
        opacity: progress,
        transform: `translateY(${interpolate(progress, [0, 1], [-18, 0])}px)`,
      }}
    >
      {[
        ["场地", "临平归蜜农场"],
        ["日期", "2026-04-28"],
        ["时间", "11:36"],
        ["蜂群饱和度", "92%"],
        ["天气", "小雨"],
        ["温度", "14°C"],
        ["湿度", "93%"],
      ].map(([label, value]) => (
        <div
          key={label}
          style={{
            minWidth: label === "场地" ? 152 : 92,
            height: 54,
            border: `1px solid ${colors.line}`,
            borderRadius: 14,
            background: colors.glass,
            padding: "8px 14px",
            boxShadow: "0 18px 44px rgba(48, 56, 44, 0.12)",
          }}
        >
          <div style={{fontSize: 16, color: colors.muted}}>{label}</div>
          <div style={{fontSize: 21, color: colors.ink, fontWeight: 700}}>
            {value}
          </div>
        </div>
      ))}
    </div>
  );
};

const LeftNav = () => {
  const frame = useCurrentFrame();
  const progress = fade(frame, 44, 76);
  const activeIndex = frame < 150 ? 0 : frame < 275 ? 1 : frame < 360 ? 2 : 0;

  return (
    <div
      style={{
        position: "absolute",
        left: 40,
        top: 310,
        width: 96,
        border: `1px solid ${colors.line}`,
        borderRadius: 26,
        background: colors.glass,
        padding: "18px 12px",
        opacity: progress,
        transform: `translateX(${interpolate(progress, [0, 1], [-40, 0])}px)`,
        boxShadow: "0 24px 70px rgba(37, 46, 36, 0.14)",
      }}
    >
      <div
        style={{
          width: 42,
          height: 42,
          margin: "0 auto 18px",
          borderRadius: 14,
          background: colors.ink,
          color: "#fff",
          display: "grid",
          placeItems: "center",
          fontWeight: 800,
          fontSize: 18,
        }}
      >
        B
      </div>
      {["环境", "行为", "策略"].map((label, index) => {
        const active = activeIndex === index;
        return (
          <div
            key={label}
            style={{
              height: 76,
              marginTop: 10,
              borderRadius: 20,
              display: "grid",
              placeItems: "center",
              color: active ? "#fff" : colors.ink,
              background: active ? colors.ink : "rgba(255,255,255,0.48)",
              fontSize: 20,
              fontWeight: 700,
            }}
          >
            {label}
          </div>
        );
      })}
    </div>
  );
};

const MapLayer = () => {
  const frame = useCurrentFrame();
  const pulse = interpolate(frame % 90, [0, 45, 90], [0.92, 1.08, 0.92], clamp);
  const heatmapOpacity = windowed(frame, 118, 160, 338, 390);
  const flowerLayer = windowed(frame, 118, 160, 210, 238);
  const beeLayer = windowed(frame, 226, 260, 338, 390);

  return (
    <div style={{position: "absolute", inset: 0, overflow: "hidden"}}>
      <div
        style={{
          position: "absolute",
          inset: 0,
          background:
            "linear-gradient(135deg, #eef3e8 0%, #dbe8d3 36%, #f6f0df 68%, #e5edf0 100%)",
        }}
      />
      {Array.from({length: 18}).map((_, i) => (
        <div
          key={i}
          style={{
            position: "absolute",
            left: `${(i * 137) % 1920}px`,
            top: `${80 + ((i * 89) % 900)}px`,
            width: 520,
            height: 2,
            background: "rgba(104, 126, 99, 0.18)",
            transform: `rotate(${i % 2 ? -31 : 28}deg)`,
          }}
        />
      ))}
      {mapPins.map((pin, index) => (
        <div
          key={index}
          style={{
            position: "absolute",
            left: `${pin.x}%`,
            top: `${pin.y}%`,
            width: pin.r * pulse,
            height: pin.r * pulse,
            marginLeft: -pin.r / 2,
            marginTop: -pin.r / 2,
            borderRadius: "50%",
            background: pin.color,
            opacity: heatmapOpacity * (index % 2 === 0 ? flowerLayer : beeLayer) * 0.3,
            boxShadow: `0 0 ${pin.r * 1.5}px ${pin.color}`,
          }}
        />
      ))}
      <div
        style={{
          position: "absolute",
          left: "47%",
          top: "51%",
          width: 420,
          height: 420,
          border: "2px solid rgba(47, 61, 45, 0.35)",
          borderRadius: "50%",
          transform: `translate(-50%, -50%) scale(${interpolate(frame, [70, 120], [0.72, 1], clamp)})`,
        }}
      />
      <div
        style={{
          position: "absolute",
          left: "47%",
          top: "51%",
          width: 18,
          height: 18,
          borderRadius: 9,
          background: colors.ink,
          boxShadow: "0 0 0 10px rgba(32,36,33,0.12)",
        }}
      />
      <div
        style={{
          position: "absolute",
          left: "43%",
          top: "42%",
          width: 500,
          height: 280,
          borderRadius: "50%",
          background: colors.honey,
          opacity: flowerLayer * 0.16,
          filter: "blur(18px)",
          transform: `translate(-50%, -50%) scale(${interpolate(frame, [118, 185], [0.6, 1.05], clamp)})`,
        }}
      />
      <div
        style={{
          position: "absolute",
          left: "56%",
          top: "54%",
          width: 460,
          height: 320,
          borderRadius: "50%",
          background: colors.blue,
          opacity: beeLayer * 0.14,
          filter: "blur(18px)",
          transform: `translate(-50%, -50%) scale(${interpolate(frame, [226, 292], [0.62, 1.08], clamp)})`,
        }}
      />
    </div>
  );
};

const LayerToggles = () => {
  const frame = useCurrentFrame();
  const visible = windowed(frame, 92, 124, 372, 410);
  const heatmapActive = windowed(frame, 118, 144, 338, 376);
  const curveActive = windowed(frame, 250, 276, 352, 390);

  return (
    <div
      style={{
        position: "absolute",
        left: 44,
        bottom: 38,
        display: "flex",
        gap: 16,
        opacity: visible,
        transform: `translateY(${interpolate(visible, [0, 1], [42, 0])}px)`,
      }}
    >
      {[
        {active: heatmapActive, icon: "H"},
        {active: curveActive, icon: "C"},
      ].map((item) => (
        <div
          key={item.icon}
          style={{
            width: 68,
            height: 68,
            borderRadius: 22,
            display: "grid",
            placeItems: "center",
            background: item.active > 0.5 ? colors.ink : colors.glass,
            color: item.active > 0.5 ? "#fff" : colors.ink,
            fontSize: 23,
            fontWeight: 900,
            border: `1px solid ${colors.line}`,
            boxShadow: "0 18px 48px rgba(36,44,35,0.14)",
          }}
        >
          {item.icon}
        </div>
      ))}
    </div>
  );
};

const SiteCard = () => {
  const frame = useCurrentFrame();
  const progress = windowed(frame, 60, 92, 126, 154);

  return (
    <div
      style={{
        position: "absolute",
        right: 766,
        top: 104,
        width: 380,
        borderRadius: 24,
        border: `1px solid ${colors.line}`,
        background: colors.glass,
        padding: 22,
        opacity: progress,
        transform: `translateY(${interpolate(progress, [0, 1], [-22, 0])}px)`,
        boxShadow: "0 24px 68px rgba(38,48,38,0.14)",
      }}
    >
      <div style={{fontSize: 24, fontWeight: 900, color: colors.ink}}>场地信息</div>
      {[
        ["场地名称", "临平归蜜农场"],
        ["蜂种", "中华蜂"],
        ["主要数据来源", "环境传感、蜂群行为观测"],
      ].map(([label, value]) => (
        <div
          key={label}
          style={{
            display: "grid",
            gridTemplateColumns: "110px 1fr",
            gap: 14,
            marginTop: 14,
            fontSize: 18,
          }}
        >
          <div style={{color: colors.muted}}>{label}</div>
          <div style={{color: colors.ink, fontWeight: 700}}>{value}</div>
        </div>
      ))}
    </div>
  );
};

const Legend = () => {
  const frame = useCurrentFrame();
  const progress = windowed(frame, 140, 172, 336, 374);

  return (
    <div
      style={{
        position: "absolute",
        left: 176,
        bottom: 42,
        width: 272,
        borderRadius: 24,
        border: `1px solid ${colors.line}`,
        background: colors.glass,
        padding: 20,
        opacity: progress,
        transform: `translateY(${interpolate(progress, [0, 1], [44, 0])}px)`,
        boxShadow: "0 22px 62px rgba(38,48,38,0.13)",
      }}
    >
      {[
        ["商业用地", "#e5d2a1"],
        ["绿地", colors.green],
        ["当下蜜源分布", colors.honey],
        ["活动热区", colors.blue],
      ].map(([label, color]) => (
        <div
          key={label}
          style={{display: "flex", alignItems: "center", gap: 12, marginTop: 10}}
        >
          <div style={{width: 18, height: 18, borderRadius: 5, background: color}} />
          <div style={{fontSize: 18, color: colors.ink, fontWeight: 700}}>{label}</div>
        </div>
      ))}
    </div>
  );
};

const Gauge = ({label, value, color, index}: (typeof metricData)[number] & {index: number}) => {
  const frame = useCurrentFrame();
  const start = 182 + index * 10;
  const valueProgress = interpolate(frame, [start, start + 52], [0, value], clamp);
  const angle = interpolate(valueProgress, [0, 100], [-132, 132]);

  return (
    <div
      style={{
        width: 164,
        height: 150,
        borderRadius: 24,
        background: "rgba(255,255,255,0.72)",
        border: `1px solid ${colors.line}`,
        padding: 18,
      }}
    >
      <div style={{fontSize: 19, fontWeight: 800, color: colors.ink}}>{label}</div>
      <div style={{position: "relative", height: 86, marginTop: 10}}>
        <div
          style={{
            position: "absolute",
            left: 12,
            right: 12,
            bottom: 0,
            height: 68,
            borderTopLeftRadius: 100,
            borderTopRightRadius: 100,
            border: "12px solid rgba(33, 39, 33, 0.12)",
            borderBottom: 0,
          }}
        />
        <div
          style={{
            position: "absolute",
            left: 22,
            bottom: 4,
            width: 62,
            height: 5,
            borderRadius: 999,
            background: color,
            transformOrigin: "100% 50%",
            transform: `rotate(${angle}deg)`,
          }}
        />
        <div
          style={{
            position: "absolute",
            left: 45,
            bottom: -2,
            fontSize: 28,
            fontWeight: 900,
            color,
          }}
        >
          {Math.round(valueProgress)}
        </div>
      </div>
    </div>
  );
};

const DashboardPanel = () => {
  const frame = useCurrentFrame();
  const progress = windowed(frame, 170, 210, 332, 374);

  return (
    <div
      style={{
        position: "absolute",
        right: 54,
        top: 278,
        width: 406,
        borderRadius: 30,
        background: colors.glass,
        border: `1px solid ${colors.line}`,
        padding: 26,
        opacity: progress,
        transform: `translateX(${interpolate(progress, [0, 1], [64, 0])}px)`,
        boxShadow: "0 30px 90px rgba(38, 48, 38, 0.16)",
      }}
    >
      <div style={{fontSize: 28, fontWeight: 900, color: colors.ink}}>
        指数仪表盘
      </div>
      <div style={{fontSize: 17, color: colors.muted, marginTop: 4}}>
        花期、蜜源、蜂群行为与错配风险
      </div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 14,
          marginTop: 24,
        }}
      >
        {metricData.map((metric, index) => (
          <Gauge key={metric.label} {...metric} index={index} />
        ))}
      </div>
    </div>
  );
};

const CurvePanel = () => {
  const frame = useCurrentFrame();
  const progress = windowed(frame, 252, 292, 354, 406);
  const points = [38, 64, 48, 78, 57, 84, 68, 73, 91];
  const draw = interpolate(frame, [292, 368], [0, 1], clamp);

  return (
    <div
      style={{
        position: "absolute",
        left: 244,
        right: 504,
        bottom: 38,
        height: 252,
        borderRadius: 30,
        border: `1px solid ${colors.line}`,
        background: "rgba(251,252,247,0.84)",
        padding: 28,
        opacity: progress,
        transform: `translateY(${interpolate(progress, [0, 1], [72, 0])}px)`,
        boxShadow: "0 30px 84px rgba(38, 48, 38, 0.15)",
      }}
    >
      <div style={{display: "flex", justifyContent: "space-between"}}>
        <div>
          <div style={{fontSize: 28, color: colors.ink, fontWeight: 900}}>
            蜂群活动预测曲线
          </div>
          <div style={{fontSize: 17, color: colors.muted, marginTop: 3}}>
            行为模式下自动加载未来活动趋势
          </div>
        </div>
        <div style={{fontSize: 34, fontWeight: 900, color: colors.blue}}>+18%</div>
      </div>
      <svg width="100%" height="132" viewBox="0 0 1060 132" style={{marginTop: 22}}>
        {[0, 1, 2].map((line) => (
          <line
            key={line}
            x1="0"
            x2="1060"
            y1={20 + line * 44}
            y2={20 + line * 44}
            stroke="rgba(70,82,68,0.15)"
          />
        ))}
        <polyline
          points={points
            .map((p, i) => `${i * 132},${118 - p}`)
            .join(" ")}
          fill="none"
          stroke={colors.blue}
          strokeWidth="7"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeDasharray="1400"
          strokeDashoffset={1400 - 1400 * draw}
        />
      </svg>
    </div>
  );
};

export const DashboardLaunch = () => {
  const frame = useCurrentFrame();
  const {width, height} = useVideoConfig();
  const cameraScale = interpolate(frame, [0, 80, 180, 300, 430], [1.04, 1, 1.02, 0.99, 1], clamp);
  const curtain = Math.max(fadeOut(frame, 8, 36), fade(frame, 432, 470));

  return (
    <AbsoluteFill
      style={{
        width,
        height,
        backgroundColor: colors.panel,
        fontFamily:
          "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          position: "absolute",
          inset: 0,
          transform: `scale(${cameraScale})`,
        }}
      >
        <MapLayer />
        <Header />
        <SiteCard />
        <LeftNav />
        <LayerToggles />
        <Legend />
        <DashboardPanel />
        <CurvePanel />
      </div>
      <div
        style={{
          position: "absolute",
          inset: 0,
          background: `rgba(251,252,247,${curtain})`,
        }}
      />
    </AbsoluteFill>
  );
};
