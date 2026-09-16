// Test fixture only: uses the actual HTTP/WebSocket service and browser WebRTC stack.
window.state = {};
window.start = ({ base, code, senderToken, role }) => {
  const state = window.state = { accepted: false, open: false, resets: 0, errors: [], received: null, ack: null };
  let pc, channel, candidates, parts, meta, bytes;
  const url = new URL(base.replace(/^http/, "ws") + "/ws");
  url.search = new URLSearchParams({ code, role, ...(role === "sender" ? { senderToken } : {}) });
  const ws = new WebSocket(url);
  const send = (type, payload) => ws.send(JSON.stringify({ type, payload }));
  const digest = async data => Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", data)))
    .map(x => x.toString(16).padStart(2, "0")).join("");
  function bind(dc) {
    channel = dc;
    dc.binaryType = "arraybuffer";
    dc.onopen = () => { state.open = true; };
    dc.onclose = () => { state.open = false; };
    let receiveQueue = Promise.resolve();
    dc.onmessage = event => {
      receiveQueue = receiveQueue.then(async () => {
        if (typeof event.data !== "string") {
          parts.push(new Uint8Array(event.data));
          bytes += event.data.byteLength;
          return;
        }
        const message = JSON.parse(event.data);
        if (message.type === "meta") {
          meta = message; parts = []; bytes = 0;
        } else if (message.type === "done") {
          if (bytes !== meta.size) throw new Error("File length mismatch");
          const file = new File(parts, meta.name);
          state.received = { name: file.name, size: file.size, sha256: await digest(await file.arrayBuffer()) };
          dc.send(JSON.stringify({ type: "ack", ...state.received }));
        } else if (message.type === "ack") state.ack = message;
      }).catch(error => state.errors.push(String(error)));
    };
  }
  function resetPeer() {
    if (pc) pc.close();
    state.open = false;
    candidates = []; parts = []; bytes = 0; meta = null;
    pc = new RTCPeerConnection({ iceServers: [] });
    pc.ondatachannel = event => bind(event.channel);
    pc.onicecandidate = event => { if (event.candidate) send("candidate", event.candidate.toJSON()); };
    pc.onconnectionstatechange = () => { state.connection = pc.connectionState; };
  }
  resetPeer();
  let queue = Promise.resolve();
  ws.onmessage = event => {
    queue = queue.then(async () => {
      const message = JSON.parse(event.data);
      if (message.type === "accepted") state.accepted = true;
      else if (message.type === "error") state.errors.push(message.payload.code);
      else if (message.type === "peer-ready") {
        state.initiator = message.payload.initiator;
        if (state.initiator) {
          bind(pc.createDataChannel("file"));
          await pc.setLocalDescription(await pc.createOffer());
          send("offer", pc.localDescription.toJSON());
        }
      } else if (message.type === "offer" || message.type === "answer") {
        await pc.setRemoteDescription(message.payload);
        for (const candidate of candidates.splice(0)) await pc.addIceCandidate(candidate);
        if (message.type === "offer") {
          await pc.setLocalDescription(await pc.createAnswer());
          send("answer", pc.localDescription.toJSON());
        }
      } else if (message.type === "candidate") {
        if (pc.remoteDescription) await pc.addIceCandidate(message.payload);
        else candidates.push(message.payload);
      } else if (message.type === "reset") {
        state.resets++;
        resetPeer();
      }
    }).catch(error => state.errors.push(String(error)));
  };
  ws.onclose = event => { state.closeCode = event.code; state.open = false; pc.close(); };
  ws.onerror = () => state.errors.push("WebSocket transport error");
  window.stop = () => { pc.close(); ws.close(1000); };
  window.sendFile = async size => {
    if (!channel || channel.readyState !== "open") throw new Error("DataChannel unavailable");
    state.ack = null;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 31 + 17) % 251;
    const file = new File([data], "payload.bin");
    const expected = { name: file.name, size, sha256: await digest(await file.arrayBuffer()) };
    channel.send(JSON.stringify({ type: "meta", name: file.name, size }));
    channel.bufferedAmountLowThreshold = 64 * 1024;
    for (let offset = 0; offset < size; offset += 16 * 1024) {
      if (channel.bufferedAmount > 256 * 1024) {
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => { cleanup(); reject(new Error("Backpressure timeout")); }, 10000);
          const ready = () => { cleanup(); resolve(); };
          const closed = () => { cleanup(); reject(new Error("Channel closed while sending")); };
          function cleanup() {
            clearTimeout(timer);
            channel.removeEventListener("bufferedamountlow", ready);
            channel.removeEventListener("close", closed);
          }
          channel.addEventListener("bufferedamountlow", ready);
          channel.addEventListener("close", closed);
          if (channel.bufferedAmount <= channel.bufferedAmountLowThreshold) ready();
        });
      }
      channel.send(data.subarray(offset, offset + 16 * 1024));
    }
    channel.send(JSON.stringify({ type: "done" }));
    return expected;
  };
};
