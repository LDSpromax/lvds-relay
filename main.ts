// 吕大帅美西中继 · Deno Deploy 版（https://dash.deno.com 新建项目后粘贴本文件即可）
// WebSocket 隧道 + HTTP 透传，出口为 Deno Deploy 的真实美国 IP（Vultr 芝加哥机房，via 头 ord.vultr.prod.deno-cluster.net）
// 环境变量（Deno Deploy 项目设置里配）：RELAY_KEY
// 默认只放行 Google AI 域名；要加白名单就配 ALLOW_HOSTS（逗号分隔）

const DEFAULT_ALLOW = "gemini.google.com,generativelanguage.googleapis.com,aistudio.google.com,ai.google.dev,clients6.google.com,alkalimakersuite-pa.clients6.google.com,proactivebackend-pa.googleapis.com,autopush-gemini.sandbox.google.com,bard.google.com,notebooklm.google.com,deepmind.google,meta.com";
const RELAY_KEY = Deno.env.get("RELAY_KEY") || "";
const ALLOW = (Deno.env.get("ALLOW_HOSTS") || DEFAULT_ALLOW).split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

function hostOk(h) {
  h = (h || "").toLowerCase();
  return ALLOW.some(d => h === d || h.endsWith("." + d));
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const upgrade = req.headers.get("upgrade") || "";

  // ---- 状态页 ----
  if (upgrade.toLowerCase() !== "websocket") {
    if (url.pathname === "/") {
      return new Response(JSON.stringify({ ok: true, mode: "lvds-relay-deno", allow_hosts: ALLOW.length }), {
        headers: { "content-type": "application/json" },
      });
    }
    // ---- HTTP 透传 ----
    if (url.pathname === "/go") {
      const token = url.searchParams.get("k") || "";
      const target = url.searchParams.get("url") || "";
      if (!RELAY_KEY || token !== RELAY_KEY) return json({ e: "unauthorized" }, 401);
      if (!/^https?:\/\//.test(target)) return json({ e: "bad url" }, 400);
      const hh = new URL(target).hostname || "";
      if (!hostOk(hh)) return json({ e: "host not allowed: " + hh }, 403);
      const h = new Headers(req.headers);
      for (const k of ["host", "cookie", "connection", "keep-alive", "transfer-encoding", "accept-encoding"]) h.delete(k);
      try {
        const up = await fetch(target, { method: req.method, headers: h, body: ["GET", "HEAD"].includes(req.method) ? undefined : await req.arrayBuffer(), redirect: "manual" });
        const oh = new Headers(up.headers);
        for (const k of ["content-encoding", "content-length", "transfer-encoding"]) oh.delete(k);
        return new Response(up.body, { status: up.status, headers: oh });
      } catch (e) {
        return json({ e: String(e).slice(0, 250) }, 502);
      }
    }
    return new Response("lvds-relay online", { status: 404 });
  }

  // ---- WS 隧道 ----
  if (url.pathname !== "/relay") return new Response("not found", { status: 404 });
  const { socket: ws, response } = Deno.upgradeWebSocket(req);
  let tcp = null;

  const closeAll = () => { try { tcp && tcp.close(); } catch { } try { ws.close(); } catch { } };

  ws.onmessage = async (ev) => {
    if (!tcp) {
      // 首帧：认证 + 目标
      if (typeof ev.data !== "string") return closeAll();
      let hello;
      try { hello = JSON.parse(ev.data); } catch { return closeAll(); }
      if (!RELAY_KEY || hello.k !== RELAY_KEY || !hostOk(hello.h)) {
        ws.send(JSON.stringify({ ok: false, e: "unauthorized or host not allowed" }));
        return closeAll();
      }
      try {
        tcp = await Deno.connect({ hostname: hello.h, port: Number(hello.p) || 443 });
      } catch (e) {
        ws.send(JSON.stringify({ ok: false, e: "connect failed: " + String(e).slice(0, 150) }));
        return closeAll();
      }
      ws.send(JSON.stringify({ ok: true }));
      pumpTcpToWs(tcp, ws);
      return;
    }
    if (ev.data instanceof ArrayBuffer) {
      try { tcp.write(new Uint8Array(ev.data)); } catch { closeAll(); }
    } else if (ev.data instanceof Uint8Array) {
      try { tcp.write(ev.data); } catch { closeAll(); }
    }
  };
  ws.onerror = closeAll;
  ws.onclose = closeAll;

  async function pumpTcpToWs(tcp, ws) {
    const buf = new Uint8Array(65536);
    try {
      while (true) {
        const n = await tcp.read(buf);
        if (n === null) break;
        ws.send(buf.slice(0, n));
      }
    } catch { }
    closeAll();
  }

  return response;
});

function json(obj, status) {
  return new Response(JSON.stringify(obj), { status, headers: { "content-type": "application/json" } });
}
