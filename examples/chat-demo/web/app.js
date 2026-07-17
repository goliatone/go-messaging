"use strict";

(() => {
  const state = {
    socket: null,
    ready: false,
    reconnectAttempt: 0,
    reconnectTimer: null,
    messages: 0,
    closedByPage: false,
  };

  const elements = {
    composer: document.querySelector("#composer"),
    sender: document.querySelector("#sender"),
    message: document.querySelector("#message"),
    send: document.querySelector("#send-button"),
    feedback: document.querySelector("#feedback"),
    feed: document.querySelector("#message-feed"),
    empty: document.querySelector("#empty-feed"),
    count: document.querySelector("#message-count"),
    connectionState: document.querySelector("#connection-state"),
    connectionDetail: document.querySelector("#connection-detail"),
    connectionDot: document.querySelector("#connection-dot"),
  };

  const storedSender = window.localStorage.getItem("chat-demo.sender");
  if (storedSender) elements.sender.value = storedSender;

  function websocketURL() {
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${scheme}//${window.location.host}/ws/chat`;
  }

  function setConnection(status, detail, tone) {
    elements.connectionState.textContent = status;
    elements.connectionDetail.textContent = detail;
    elements.connectionDot.dataset.tone = tone;
  }

  function setReady(ready) {
    state.ready = ready;
    elements.send.disabled = !ready;
  }

  function feedback(message, tone = "neutral") {
    elements.feedback.textContent = message;
    elements.feedback.dataset.tone = tone;
  }

  function connect() {
    window.clearTimeout(state.reconnectTimer);
    setReady(false);
    setConnection(
      state.reconnectAttempt ? "Reconnecting" : "Connecting",
      "Waiting for server readiness",
      "pending",
    );

    const socket = new WebSocket(websocketURL());
    state.socket = socket;

    socket.addEventListener("open", () => {
      setConnection("Connected", "Checking messaging readiness", "pending");
    });

    socket.addEventListener("message", (event) => {
      let frame;
      try {
        frame = JSON.parse(event.data);
      } catch {
        feedback("The server sent an unreadable event.", "error");
        return;
      }
      handleFrame(frame);
    });

    socket.addEventListener("close", () => {
      if (state.socket !== socket) return;
      setReady(false);
      state.socket = null;
      if (state.closedByPage) return;
      scheduleReconnect();
    });

    socket.addEventListener("error", () => {
      feedback("The live connection is unavailable. Retrying…", "error");
    });
  }

  function scheduleReconnect() {
    state.reconnectAttempt += 1;
    const delay = Math.min(10000, 500 * (2 ** Math.min(state.reconnectAttempt - 1, 5)));
    setConnection("Offline", `Retrying in ${Math.ceil(delay / 1000)}s`, "offline");
    feedback("Disconnected. Messages are not replayed while offline.", "error");
    state.reconnectTimer = window.setTimeout(connect, delay);
  }

  function handleFrame(frame) {
    if (!frame || typeof frame.type !== "string") return;
    switch (frame.type) {
      case "chat.ready":
        state.reconnectAttempt = 0;
        setReady(true);
        setConnection("Ready", "Valkey Pub/Sub is live", "ready");
        feedback("Ready to publish. Delivery appears after broker ingress.");
        break;
      case "chat.accepted":
        feedback(`Accepted for publication · ${shortID(frame.data && frame.data.id)}`, "accepted");
        break;
      case "chat.message":
        appendMessage(frame.data);
        feedback("Message delivered through Valkey.", "delivered");
        break;
      case "chat.error":
        feedback(boundedText(frame.data && frame.data.message, "Message could not be sent."), "error");
        break;
      default:
        break;
    }
  }

  function appendMessage(data) {
    if (!data || typeof data.id !== "string" || typeof data.sender !== "string" || typeof data.text !== "string") {
      feedback("A delivered event had an invalid shape.", "error");
      return;
    }
    elements.empty?.remove();

    const item = document.createElement("li");
    item.className = "message-card";
    const heading = document.createElement("div");
    heading.className = "message-meta";
    const sender = document.createElement("strong");
    sender.textContent = boundedText(data.sender, "unknown");
    const timestamp = document.createElement("time");
    const parsed = new Date(data.timestamp);
    timestamp.dateTime = Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
    timestamp.textContent = Number.isNaN(parsed.getTime())
      ? "just now"
      : new Intl.DateTimeFormat([], { hour: "numeric", minute: "2-digit", second: "2-digit" }).format(parsed);
    const text = document.createElement("p");
    text.textContent = boundedText(data.text, "");
    const id = document.createElement("span");
    id.className = "message-id";
    id.textContent = shortID(data.id);
    heading.append(sender, timestamp);
    item.append(heading, text, id);
    elements.feed.append(item);

    while (elements.feed.children.length > 200) {
      elements.feed.firstElementChild.remove();
    }
    state.messages += 1;
    elements.count.textContent = `${state.messages} ${state.messages === 1 ? "message" : "messages"}`;
    elements.feed.scrollTop = elements.feed.scrollHeight;
  }

  function boundedText(value, fallback) {
    if (typeof value !== "string" || !value.trim()) return fallback;
    return value.slice(0, 1000);
  }

  function shortID(value) {
    if (typeof value !== "string" || !value) return "pending";
    return value.slice(0, 10);
  }

  elements.composer.addEventListener("submit", (event) => {
    event.preventDefault();
    const sender = elements.sender.value.trim();
    const text = elements.message.value.trim();
    if (!state.ready || !state.socket || state.socket.readyState !== WebSocket.OPEN) {
      feedback("Wait for the live connection before sending.", "error");
      return;
    }
    if (!sender || !text) {
      feedback("Add your name and a message first.", "error");
      return;
    }
    window.localStorage.setItem("chat-demo.sender", sender);
    state.socket.send(JSON.stringify({ type: "chat.send", data: { sender, text } }));
    elements.message.value = "";
    elements.message.focus();
    feedback("Publishing…", "neutral");
  });

  window.addEventListener("beforeunload", () => {
    state.closedByPage = true;
    window.clearTimeout(state.reconnectTimer);
    state.socket?.close(1000, "page closed");
  });

  connect();
})();
