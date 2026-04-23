const params = new URLSearchParams(window.location.search);
const scenarioId = params.get("scenario");
const isFreeMode = params.get("mode") === "free" || !scenarioId;

const backButton = document.getElementById("backScenarioButton");
const composer = document.getElementById("chatComposer");
const sendButton = document.querySelector(".send-placeholder");
const stage = document.querySelector(".chat-stage");
const tip = document.getElementById("composerTip");
const contextPanel = document.getElementById("chatContextPanel");
const contextKicker = document.getElementById("chatContextKicker");
const contextTitle = document.getElementById("chatContextTitle");
const contextSummary = document.getElementById("chatContextSummary");
const contextPoints = document.getElementById("chatContextPoints");
const contextClear = document.getElementById("chatContextClear");

let activeScenario = null;
let messages = [];
let isSending = false;
let shouldAutoScroll = true;

const list = document.createElement("div");
list.id = "chatMessageList";
list.className = "chat-message-list";
stage.appendChild(list);

const status = document.createElement("div");
status.id = "chatStatusBar";
status.className = "chat-status-bar";
stage.appendChild(status);

function strategyReturnUrl() {
  return scenarioId ? `./scenario_ai.html?scenario=${encodeURIComponent(scenarioId)}` : "./scenario_ai.html";
}

function goBackToStrategy() {
  window.location.href = strategyReturnUrl();
}

function statusText(text, isError = false) {
  status.textContent = text || "";
  status.classList.toggle("is-error", isError);
}

function resizeComposer() {
  composer.style.height = "auto";
  composer.style.height = `${Math.min(composer.scrollHeight, 140)}px`;
}

function trackAutoScroll() {
  const distance = document.documentElement.scrollHeight - window.innerHeight - window.scrollY;
  shouldAutoScroll = distance <= 160;
}

function scrollToBottom(force = false) {
  if (!force && !shouldAutoScroll) return;
  requestAnimationFrame(() => {
    window.scrollTo({
      top: document.documentElement.scrollHeight,
      behavior: force ? "auto" : "smooth"
    });
  });
}

function addMessage(message) {
  const item = document.createElement("article");
  item.className = `chat-message ${message.role}`;
  item.textContent = message.content;
  list.appendChild(item);
  scrollToBottom();
  return item;
}

function renderContext(scenario) {
  if (!scenario) {
    contextPanel?.classList.add("is-hidden");
    tip.textContent = "自由对话模式";
    composer.placeholder = "输入你的问题...";
    statusText("");
    return;
  }

  contextPanel?.classList.remove("is-hidden");
  contextKicker.textContent = `${scenario.shortLabel || ""} / Active Strategy Context`;
  contextTitle.textContent = scenario.title || "当前策略";
  contextSummary.textContent = scenario.summary || "";
  contextPoints.innerHTML = (scenario.detailSections || [])
    .map((section) => `<p class="chat-context-point"><strong>${section.label}</strong>：${section.value}</p>`)
    .join("");

  tip.textContent = `Context: ${scenario.title}`;
  composer.placeholder = `继续深入聊「${scenario.title}」...`;
  statusText(`已载入策略上下文：${scenario.title}`);
}

async function loadScenarioContext() {
  if (isFreeMode) {
    sessionStorage.removeItem("beeScenarioChatContext");
    activeScenario = null;
    renderContext(null);
    return;
  }

  const cachedContext = sessionStorage.getItem("beeScenarioChatContext");
  if (cachedContext) {
    try {
      const parsedContext = JSON.parse(cachedContext);
      if (parsedContext && parsedContext.id === scenarioId) {
        activeScenario = parsedContext;
        renderContext(activeScenario);
        return;
      }
    } catch {
      sessionStorage.removeItem("beeScenarioChatContext");
    }
  }

  try {
    const response = await fetch(`/api/scenarios?t=${Date.now()}`, { cache: "no-store" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `策略加载失败，HTTP ${response.status}`);
    activeScenario = (data.scenarios || []).find((item) => item.id === scenarioId) || null;
    if (!activeScenario) throw new Error("没有找到对应的策略上下文。");
    renderContext(activeScenario);
  } catch (error) {
    activeScenario = null;
    renderContext(null);
    statusText(error.message || "策略上下文加载失败，已切换为自由对话。", true);
  }
}

async function sendMessage() {
  const text = composer.value.trim();
  if (!text || isSending) return;

  isSending = true;
  shouldAutoScroll = true;
  addMessage({ role: "user", content: text });
  messages.push({ role: "user", content: text });
  composer.value = "";
  resizeComposer();

  const pending = addMessage({ role: "assistant", content: activeScenario ? "正在结合当前策略背景思考..." : "正在思考..." });
  statusText("AI 正在生成回复...");

  try {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: text,
        history: messages,
        scenarioContext: activeScenario
      })
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `发送失败，HTTP ${response.status}`);

    pending.textContent = data.reply || "AI 没有返回内容。";
    messages.push({ role: "assistant", content: pending.textContent });
    statusText("AI 回复完成。");
    scrollToBottom(true);
  } catch (error) {
    pending.textContent = error.message || "对话失败。";
    pending.classList.remove("assistant");
    pending.classList.add("system");
    statusText(error.message || "对话失败。", true);
  } finally {
    isSending = false;
  }
}

backButton?.addEventListener("click", goBackToStrategy);
contextClear?.addEventListener("click", goBackToStrategy);
window.addEventListener("scroll", trackAutoScroll, { passive: true });
composer.addEventListener("input", resizeComposer);
composer.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    sendMessage();
  }
});
sendButton.addEventListener("click", sendMessage);

loadScenarioContext().then(() => {
  composer.focus();
  scrollToBottom(true);
});
