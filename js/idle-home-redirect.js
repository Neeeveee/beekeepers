(function () {
  const IDLE_TIMEOUT_MS = 2 * 60 * 1000;
  const ACTIVITY_MESSAGE_TYPE = "bee-project-user-activity";
  const events = [
    "mousemove",
    "mousedown",
    "keydown",
    "scroll",
    "touchstart",
    "pointerdown",
    "wheel"
  ];

  const script = document.currentScript;
  const homeUrl = script?.dataset.homeUrl || "./index.html";
  const isEmbedded = window.self !== window.top;
  const excludedPages = [
    "/ml_monitor.html",
    "/nectar_carousel.html",
    "/exhibition_realtime_curve.html"
  ];
  let idleTimer = 0;

  if (excludedPages.some((page) => window.location.pathname.endsWith(page))) {
    return;
  }

  function notifyParent() {
    if (!isEmbedded) {
      return;
    }
    window.parent.postMessage({ type: ACTIVITY_MESSAGE_TYPE }, "*");
  }

  function goHome() {
    const currentUrl = new URL(window.location.href);
    const targetUrl = new URL(homeUrl, window.location.href);

    if (currentUrl.href === targetUrl.href) {
      resetIdleTimer();
      return;
    }

    window.location.href = targetUrl.href;
  }

  function resetIdleTimer() {
    window.clearTimeout(idleTimer);

    if (!isEmbedded) {
      idleTimer = window.setTimeout(goHome, IDLE_TIMEOUT_MS);
    }
  }

  function handleActivity() {
    resetIdleTimer();
    notifyParent();
  }

  events.forEach((eventName) => {
    window.addEventListener(eventName, handleActivity, { passive: true });
  });

  window.addEventListener("message", (event) => {
    if (event.data?.type === ACTIVITY_MESSAGE_TYPE) {
      resetIdleTimer();
    }
  });

  resetIdleTimer();
})();
