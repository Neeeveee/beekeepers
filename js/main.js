const header = document.querySelector("[data-header]");
const toast = document.querySelector("[data-toast]");
const contactButton = document.querySelector("[data-contact]");
const revealItems = document.querySelectorAll(".section .section-inner, .site-footer .section-inner");
const whyTimeline = document.querySelector("[data-why-timeline]");
const systemCards = document.querySelectorAll(".system-card");
const practiceStack = document.querySelector(".perspective-card-stack");
const reduceMotionQuery = window.matchMedia("(prefers-reduced-motion: reduce)");

window.addEventListener("load", () => {
  document.body.classList.remove("is-loading");
});

document.querySelectorAll('a[href^="#"]').forEach((link) => {
  link.addEventListener("click", (event) => {
    const targetId = link.getAttribute("href");
    const target = targetId && document.querySelector(targetId);

    if (!target) {
      return;
    }

    event.preventDefault();
    const headerHeight = header ? header.offsetHeight : 0;
    const top = target.getBoundingClientRect().top + window.scrollY - headerHeight + 16;

    window.scrollTo({
      top,
      behavior: "smooth"
    });
  });
});

if (contactButton && toast) {
  let toastTimer;

  contactButton.addEventListener("click", () => {
    toast.classList.add("is-visible");
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => {
      toast.classList.remove("is-visible");
    }, 2800);
  });
}

systemCards.forEach((card) => {
  const inner = card.querySelector(".system-card-inner");
  let rotation = 0;
  let isFlipped = false;

  if (!inner) {
    return;
  }

  const rotateForward = (shouldFlip) => {
    if (isFlipped === shouldFlip) {
      return;
    }

    isFlipped = shouldFlip;
    rotation += 180;
    inner.style.setProperty("--card-rotation", `${rotation}deg`);
  };

  card.addEventListener("pointerenter", () => rotateForward(true));
  card.addEventListener("pointerleave", () => rotateForward(false));
  card.addEventListener("focusin", () => rotateForward(true));
  card.addEventListener("focusout", () => rotateForward(false));
});

if (practiceStack) {
  const stackCards = Array.from(practiceStack.querySelectorAll(".stack-card"));

  const setActiveStackCard = (activeCard) => {
    stackCards.forEach((card) => {
      card.classList.toggle("is-active", card === activeCard);
    });
  };

  practiceStack.addEventListener("pointermove", (event) => {
    if (window.matchMedia("(max-width: 860px)").matches || reduceMotionQuery.matches) {
      return;
    }

    const stackRect = practiceStack.getBoundingClientRect();
    const stackCenter = stackRect.left + stackRect.width / 2;
    const cardCenters = stackCards.map((card) => {
      const x = Number.parseFloat(getComputedStyle(card).getPropertyValue("--x")) || 0;
      return stackCenter + x;
    });
    const cardHalfWidth = 145;
    const minActiveX = Math.min(...cardCenters) - cardHalfWidth;
    const maxActiveX = Math.max(...cardCenters) + cardHalfWidth;

    if (event.clientX < minActiveX || event.clientX > maxActiveX) {
      setActiveStackCard(null);
      return;
    }

    const nearestCard = stackCards.reduce((nearest, card) => {
      const x = Number.parseFloat(getComputedStyle(card).getPropertyValue("--x")) || 0;
      const centerX = stackCenter + x;
      const distance = Math.abs(event.clientX - centerX);

      if (!nearest || distance < nearest.distance) {
        return { card, distance };
      }

      return nearest;
    }, null);

    if (nearestCard) {
      setActiveStackCard(nearestCard.card);
    }
  });

  practiceStack.addEventListener("pointerleave", () => setActiveStackCard(null));
  stackCards.forEach((card) => {
    card.addEventListener("focusin", () => setActiveStackCard(card));
    card.addEventListener("focusout", () => setActiveStackCard(null));
  });
}

if ("IntersectionObserver" in window) {
  const revealObserver = new IntersectionObserver((entries, observer) => {
    entries.forEach((entry) => {
      if (!entry.isIntersecting) {
        return;
      }

      entry.target.classList.add("is-visible");
      observer.unobserve(entry.target);
    });
  }, {
    threshold: 0.18,
    rootMargin: "0px 0px -8% 0px"
  });

  revealItems.forEach((item) => revealObserver.observe(item));
} else {
  revealItems.forEach((item) => item.classList.add("is-visible"));
}

if (whyTimeline) {
  if ("IntersectionObserver" in window && !reduceMotionQuery.matches) {
    const timelineObserver = new IntersectionObserver((entries, observer) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }

        entry.target.classList.remove("is-hidden");
        entry.target.classList.add("is-active");
        observer.unobserve(entry.target);
      });
    }, {
      threshold: 0.28,
      rootMargin: "0px 0px -12% 0px"
    });

    timelineObserver.observe(whyTimeline);
} else {
    whyTimeline.classList.remove("is-hidden");
    whyTimeline.classList.add("is-active");
  }
}
