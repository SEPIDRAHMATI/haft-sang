// Two-player networking: a direct peer-to-peer link (PeerJS / WebRTC) when the
// networks allow it, otherwise a relay through a public MQTT broker over WebSocket.
//
//   GameNet.host({ prefix, topic, code, onLink, onError, onStatus }) -> { stop() }
//   GameNet.join({ prefix, topic, code, forceRelay, onStatus }) -> Promise<link>
//     rejects with { type: "no-response", relays: <brokers reached>, direct: <why direct failed> }
//
// A link is { kind: "direct" | "relay", open, send(obj), close(), onData, onClose }.
(function () {
  const VERSION = 3;
  const BROKERS = [
    { url: "wss://broker.emqx.io:8084/mqtt" },
    { url: "wss://broker.hivemq.com:8884/mqtt" },
    { url: "wss://public.cloud.shiftr.io", username: "public", password: "public" }   // port 443, rarely blocked
  ];
  const connectBroker = (b, clientId, timeout) =>
    mqtt.connect(b.url, { clean: true, connectTimeout: timeout, reconnectPeriod: 3000, clientId, username: b.username, password: b.password });
  const DIRECT_WAIT = 6000, RELAY_WAIT = 10000, PING = 2000, DEAD = 9000;
  const rid = () => Math.random().toString(36).slice(2, 10);
  const parse = buf => { try { return JSON.parse(buf.toString()); } catch { return null; } };

  function wrapPeer(conn, onEnd) {
    let closed = false;
    const link = {
      kind: "direct", onData: null, onClose: null,
      get open() { return conn.open && !closed; },
      send(m) { if (conn.open) conn.send(m); },
      close() { try { conn.close(); } catch {} fire(); }
    };
    function fire() { if (closed) return; closed = true; onEnd && onEnd(); link.onClose && link.onClose(); }
    conn.on("data", m => link.onData && link.onData(m));
    conn.on("close", fire);
    conn.on("error", fire);
    return link;
  }

  function relayLink(client, outTopic, wrap, onEnd) {
    let open = true, last = Date.now();
    const pub = m => { try { client.publish(outTopic, JSON.stringify(wrap(m))); } catch {} };
    const link = {
      kind: "relay", onData: null, onClose: null,
      get open() { return open; },
      send(m) { if (open) pub(m); },
      close() { if (open) pub({ t: "__bye" }); shut(); },
      _recv(m) {
        last = Date.now();
        if (!m || typeof m !== "object" || m.t === "__ping" || m.t === "__welcome" || m.t === "__knock") return;
        if (m.t === "__bye") return shut();
        link.onData && link.onData(m);
      }
    };
    const timer = setInterval(() => { pub({ t: "__ping" }); if (Date.now() - last > DEAD) shut(); }, PING);
    function shut() { if (!open) return; open = false; clearInterval(timer); onEnd && onEnd(); link.onClose && link.onClose(); }
    return link;
  }

  function host({ prefix, topic, code, onLink, onError, onStatus }) {
    let peer = null, stopped = false, directOk = false;
    const clients = [], links = new Map(), relayUp = new Set();
    const status = () => { if (!stopped && onStatus) onStatus({ direct: directOk, relays: relayUp.size, of: BROKERS.length }); };

    if (window.Peer) {
      peer = new Peer(prefix + code);
      peer.on("open", () => { directOk = true; status(); });
      peer.on("error", e => { if (e.type === "unavailable-id" && !stopped) { stop(); onError({ type: "unavailable-id" }); } });
      peer.on("disconnected", () => { if (!stopped) try { peer.reconnect(); } catch {} });
      peer.on("connection", c => c.on("open", () => { if (!stopped) onLink(wrapPeer(c)); }));
    }

    if (window.mqtt) for (const b of BROKERS) {
      const client = connectBroker(b, "h_" + rid(), 8000);
      clients.push(client);
      const inTopic = `${topic}/${code}/h`;
      client.on("connect", () => { relayUp.add(b.url); status(); client.subscribe(inTopic); });
      client.on("close", () => { if (relayUp.delete(b.url)) status(); });
      client.on("error", () => {});
      client.on("message", (t, buf) => {
        const env = parse(buf);
        if (!env || typeof env.g !== "string" || !env.m || stopped) return;
        const out = `${topic}/${code}/g/${env.g}`;
        const welcome = () => client.publish(out, JSON.stringify({ t: "__welcome" }));
        let L = links.get(env.g);
        if (!L || !L.open) {
          if (env.m.t !== "__knock") return;
          L = relayLink(client, out, m => m, () => links.delete(env.g));
          links.set(env.g, L);
          welcome();
          onLink(L);
          return;
        }
        if (env.m.t === "__knock") welcome();
        L._recv(env.m);
      });
    }

    setTimeout(() => { if (!stopped && !directOk && !relayUp.size) onError({ type: "network" }); }, 12000);

    function stop() {
      if (stopped) return;
      stopped = true;
      links.forEach(l => l.close()); links.clear();
      try { peer && peer.destroy(); } catch {}
      setTimeout(() => clients.forEach(c => { try { c.end(true); } catch {} }), 200);
    }
    return { stop };
  }

  function join({ prefix, topic, code, forceRelay, onStatus }) {
    return new Promise((resolve, reject) => {
      let done = false, directWhy = forceRelay ? "skipped" : "timeout";

      function relay() {
        if (!window.mqtt) { done = true; return reject({ type: "no-response", relays: 0, direct: directWhy }); }
        const reached = new Set();
        const id = rid(), inTopic = `${topic}/${code}/g/${id}`, outTopic = `${topic}/${code}/h`;
        const wrap = m => ({ g: id, m });
        const clients = [], knocks = [];
        const cleanup = keep => {
          knocks.forEach(clearInterval);
          clients.forEach(c => { if (c !== keep) try { c.end(true); } catch {} });
        };
        const timeout = setTimeout(() => { if (done) return; done = true; cleanup(); reject({ type: "no-response", relays: reached.size, direct: directWhy }); }, RELAY_WAIT);
        BROKERS.forEach((b, i) => {
          const client = connectBroker(b, "g_" + id + "_" + i, 7000);
          clients.push(client);
          let L = null, knocking = false;
          client.on("error", () => {});
          client.on("connect", () => {
            reached.add(b.url);
            client.subscribe(inTopic, () => {
              if (knocking || done) return;
              knocking = true;
              const knock = () => client.publish(outTopic, JSON.stringify(wrap({ t: "__knock" })));
              knock(); knocks.push(setInterval(knock, 1000));
            });
          });
          client.on("message", (t, buf) => {
            const m = parse(buf);
            if (!m) return;
            if (L) return L._recv(m);
            if (m.t !== "__welcome" || done) return;
            done = true; clearTimeout(timeout); cleanup(client);
            L = relayLink(client, outTopic, wrap, () => setTimeout(() => { try { client.end(true); } catch {} }, 200));
            resolve(L);
          });
        });
      }

      if (forceRelay || !window.Peer) return relay();

      const peer = new Peer();
      const fallback = () => {
        if (done) return;
        clearTimeout(timer);
        try { peer.destroy(); } catch {}
        onStatus && onStatus("Trying another route…");
        relay();
      };
      const timer = setTimeout(fallback, DIRECT_WAIT);
      peer.on("error", e => {
        directWhy = e && e.type === "peer-unavailable" ? "no-host" : "blocked:" + (e && e.type);
        fallback();
      });
      peer.on("open", () => {
        const c = peer.connect(prefix + code, { reliable: true });
        c.on("open", () => {
          if (done) { try { c.close(); } catch {} return; }
          done = true; clearTimeout(timer);
          resolve(wrapPeer(c, () => { try { peer.destroy(); } catch {} }));
        });
      });
    });
  }

  window.GameNet = { host, join, VERSION };
})();
