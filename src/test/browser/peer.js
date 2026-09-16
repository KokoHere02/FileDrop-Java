// Protocol v3 test fixture: one PeerConnection and independent transfer state per peer.
window.state = {};
window.start = ({ base, code, senderToken, role }) => {
  const state = window.state = { accepted: false, open: false, resets: 0, errors: [], peers: {}, received: null, ack: null, acks: {} };
  const peers = new Map();
  let selectedFile = null;
  const url = new URL(base.replace(/^http/, "ws") + "/ws");
  url.search = new URLSearchParams({ code, role, ...(role === "sender" ? { senderToken } : {}) });
  const ws = new WebSocket(url);
  const send = (peerId, type, payload) => ws.send(JSON.stringify({ type, to: peerId, payload }));
  const digest = async data => Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", data)))
    .map(x => x.toString(16).padStart(2, "0")).join("");
  function refresh() { state.open = [...peers.values()].some(p => p.channel?.readyState === "open"); }
  function remove(peerId) {
    const peer = peers.get(peerId);
    if (!peer) return;
    peers.delete(peerId);
    peer.closed = true;
    peer.pc.close();
    delete state.peers[peerId];
    delete state.acks[peerId];
    refresh();
  }
  function bind(peer, channel) {
    peer.channel = channel;
    channel.binaryType = "arraybuffer";
    channel.onopen = () => {
      state.peers[peer.id].open = true; refresh();
      // Late receivers start from byte zero using the currently selected file.
      if (role === "sender" && selectedFile) transmit(peer, selectedFile).catch(error => {
        if (!peer.closed) state.peers[peer.id].error = String(error);
      });
    };
    channel.onclose = () => { if (state.peers[peer.id]) state.peers[peer.id].open = false; refresh(); };
    let receiveQueue = Promise.resolve();
    channel.onmessage = event => {
      receiveQueue = receiveQueue.then(async () => {
        if (peer.closed) return;
        if (typeof event.data !== "string") {
          peer.parts.push(new Uint8Array(event.data)); peer.bytes += event.data.byteLength; return;
        }
        const message = JSON.parse(event.data);
        if (message.type === "meta") {
          peer.meta = message; peer.parts = []; peer.bytes = 0;
        } else if (message.type === "done") {
          if (peer.bytes !== peer.meta.size) throw new Error("File length mismatch");
          const file = new File(peer.parts, peer.meta.name);
          state.received = { name: file.name, size: file.size, sha256: await digest(await file.arrayBuffer()) };
          if (!peer.closed) channel.send(JSON.stringify({ type: "ack", ...state.received }));
        } else if (message.type === "ack") {
          state.ack = message; state.acks[peer.id] = message;
        }
      }).catch(error => { if (!peer.closed) state.errors.push(String(error)); });
    };
  }
  function createPeer(id) {
    const peer = { id, pc: new RTCPeerConnection({ iceServers: [] }), candidates: [], parts: [], bytes: 0, closed: false };
    peers.set(id, peer);
    state.peers[id] = { open: false };
    peer.pc.ondatachannel = event => bind(peer, event.channel);
    peer.pc.onicecandidate = event => { if (event.candidate && !peer.closed) send(id, "candidate", event.candidate.toJSON()); };
    return peer;
  }
  let queue = Promise.resolve();
  ws.onmessage = event => {
    queue = queue.then(async () => {
      const message = JSON.parse(event.data);
      if (message.type === "accepted") { state.accepted = true; state.clientId = message.to; return; }
      if (message.type === "error") { state.errors.push(message.payload.code); return; }
      if (message.type === "reset") { state.resets++; remove(message.from); return; }
      let peer = peers.get(message.from);
      if (message.type === "peer-ready") {
        if (peer) return;
        peer = createPeer(message.from);
        state.initiator = message.payload.initiator;
        if (message.payload.initiator) {
          bind(peer, peer.pc.createDataChannel("file"));
          await peer.pc.setLocalDescription(await peer.pc.createOffer());
          if (!peer.closed) send(peer.id, "offer", peer.pc.localDescription.toJSON());
        }
      } else if (peer && (message.type === "offer" || message.type === "answer")) {
        await peer.pc.setRemoteDescription(message.payload);
        for (const candidate of peer.candidates.splice(0)) await peer.pc.addIceCandidate(candidate);
        if (message.type === "offer") {
          await peer.pc.setLocalDescription(await peer.pc.createAnswer());
          if (!peer.closed) send(peer.id, "answer", peer.pc.localDescription.toJSON());
        }
      } else if (peer && message.type === "candidate") {
        if (peer.pc.remoteDescription) await peer.pc.addIceCandidate(message.payload);
        else peer.candidates.push(message.payload);
      }
    }).catch(error => state.errors.push(String(error)));
  };
  ws.onclose = event => { state.closeCode = event.code; for (const id of [...peers.keys()]) remove(id); };
  ws.onerror = () => state.errors.push("WebSocket transport error");
  window.stop = () => { for (const id of [...peers.keys()]) remove(id); ws.close(1000); };
  async function transmit(peer, file) {
    const channel = peer.channel;
    if (peer.closed || channel?.readyState !== "open") throw new Error("DataChannel unavailable");
    delete state.acks[peer.id];
    channel.send(JSON.stringify({ type: "meta", name: file.name, size: file.data.length }));
    channel.bufferedAmountLowThreshold = 64 * 1024;
    for (let offset = 0; offset < file.data.length; offset += 16 * 1024) {
      if (channel.bufferedAmount > 256 * 1024) {
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => { cleanup(); reject(new Error("Backpressure timeout")); }, 10000);
          const ready = () => { cleanup(); resolve(); };
          const closed = () => { cleanup(); reject(new Error("Channel closed while sending")); };
          function cleanup() {
            clearTimeout(timer);
            channel.removeEventListener("bufferedamountlow", ready); channel.removeEventListener("close", closed);
          }
          channel.addEventListener("bufferedamountlow", ready); channel.addEventListener("close", closed);
          if (channel.bufferedAmount <= channel.bufferedAmountLowThreshold) ready();
        });
      }
      channel.send(file.data.subarray(offset, offset + 16 * 1024));
    }
    channel.send(JSON.stringify({ type: "done" }));
  }
  window.sendFile = async size => {
    state.ack = null; state.acks = {};
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 31 + 17) % 251;
    selectedFile = { data, name: "payload.bin" };
    const expected = { name: selectedFile.name, size, sha256: await digest(data) };
    const ready = [...peers.values()].filter(p => p.channel?.readyState === "open");
    if (!ready.length) throw new Error("No ready receivers");
    const outcomes = await Promise.allSettled(ready.map(peer => transmit(peer, selectedFile)));
    state.sendFailures = outcomes.filter(o => o.status === "rejected").length;
    return expected;
  };
};
