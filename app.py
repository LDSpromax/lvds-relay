# 吕大帅美西中继 · aiohttp 实现（WebSocket 隧道 + HTTP 透传）
# 部署目标：任何能跑 Python 的免费/付费 PaaS（Render / Railway / 自有 VPS / HF PRO Docker）
# 监听 0.0.0.0:7860（HF Space 规范端口，其他平台任意）
#
# 协议：
#   GET  /                      -> 状态 JSON（健康检查）
#   WS   /relay                 -> 客户端首帧 TEXT: {"k":RELAY_KEY,"h":"域名","p":443}
#                                  服务端回 {"ok":true} 后，双向二进制帧=原始 TCP 字节流
#   ANY  /go?k=KEY&url=<URL>    -> HTTP 透传（服务端 fetch 目标，从本机出口出站）
#
# 环境变量：
#   RELAY_KEY   必填，访问密钥（与 Worker 端 RELAY_KEY 一致）
#   ALLOW_HOSTS 可选，逗号分隔域名后缀白名单；留空=仅限 Google AI 域名
import asyncio
import json
import os
from urllib.parse import urlparse

from aiohttp import web, WSMsgType, ClientSession, ClientTimeout

RELAY_KEY = os.environ.get("RELAY_KEY", "")
DEFAULT_ALLOW = ("gemini.google.com,generativelanguage.googleapis.com,aistudio.google.com,ai.google.dev,clients6.google.com,alkalimakersuite-pa.clients6.google.com,autopush-gemini.sandbox.google.com,bard.google.com,notebooklm.google.com,deepmind.google")
ALLOW = [d.strip().lower() for d in os.environ.get("ALLOW_HOSTS", DEFAULT_ALLOW).split(",") if d.strip()]


def host_ok(h: str) -> bool:
    h = (h or "").lower()
    return any(h == d or h.endswith("." + d) for d in ALLOW)


async def idx(_req):
    return web.json_response({
        "ok": True,
        "mode": "lvds-relay",
        "allow_hosts": len(ALLOW),
        "egress_hint": "本服务的出口 IP 即目标网站看到的 IP",
    })


async def relay_ws(request):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=8 * 1024 * 1024)
    await ws.prepare(request)

    # 1. 首帧：认证 + 目标
    try:
        msg = await ws.receive(timeout=15)
    except asyncio.TimeoutError:
        await ws.close(code=4001, message=b"handshake timeout")
        return ws
    if msg.type != WSMsgType.TEXT:
        await ws.close(code=4002, message=b"first frame must be text")
        return ws
    try:
        hello = json.loads(msg.data)
    except Exception:
        await ws.close(code=4002, message=b"bad json")
        return ws

    if not RELAY_KEY or hello.get("k") != RELAY_KEY:
        await ws.close(code=4003, message=b"unauthorized")
        return ws
    host, port = str(hello.get("h", "")).lower(), int(hello.get("p", 443) or 443)
    if not host or not host_ok(host):
        await ws.close(code=4004, message=b"host not allowed")
        return ws

    # 2. 连接目标（TLS 由两端各自处理，本服务只搬原始字节）
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=12)
    except Exception as e:
        try:
            await ws.send_text(json.dumps({"ok": False, "e": f"{type(e).__name__}: {e}"[:200]}))
        except Exception:
            pass
        await ws.close(code=4005, message=b"upstream connect failed")
        return ws

    await ws.send_text(json.dumps({"ok": True}))

    async def ws2tcp():
        async for m in ws:
            if m.type == WSMsgType.BINARY:
                writer.write(m.data)
                await writer.drain()
            elif m.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                break

    async def tcp2ws():
        while True:
            data = await reader.read(65536)
            if not data:
                break
            await ws.send_bytes(data)

    t1 = asyncio.create_task(ws2tcp())
    t2 = asyncio.create_task(tcp2ws())
    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    try:
        writer.close()
    except Exception:
        pass
    return ws


HOP_HEADERS = {"host", "cookie", "connection", "keep-alive", "transfer-encoding",
               "upgrade", "proxy-authorization", "proxy-connection", "te", "trailers"}


async def go(req):
    token = req.query.get("k", "")
    url = req.query.get("url", "")
    if not RELAY_KEY or token != RELAY_KEY:
        return web.json_response({"e": "unauthorized"}, status=401)
    if not url.startswith(("http://", "https://")):
        return web.json_response({"e": "bad url"}, status=400)
    hh = urlparse(url).hostname or ""
    if not host_ok(hh):
        return web.json_response({"e": "host not allowed: " + hh}, status=403)

    headers = {k: v for k, v in req.headers.items() if k.lower() not in HOP_HEADERS}
    headers.pop("accept-encoding", None)
    body = await req.read() if req.method not in ("GET", "HEAD") else None

    try:
        async with ClientSession(timeout=ClientTimeout(total=300)) as s:
            async with s.request(req.method, url, headers=headers, data=body,
                                 allow_redirects=False) as up:
                rh = dict(up.headers)
                for k in ("content-encoding", "content-length", "transfer-encoding"):
                    rh.pop(k, None)
                resp = web.StreamResponse(status=up.status, headers=rh)
                await resp.prepare(req)
                async for chunk in up.content.iter_chunked(65536):
                    await resp.write(chunk)
                await resp.write_eof()
                return resp
    except Exception as e:
        return web.json_response({"e": f"{type(e).__name__}: {str(e)[:250]}"}, status=502)


def make_app():
    app = web.Application(client_max_size=32 * 1024 * 1024)
    app.router.add_get("/", idx)
    app.router.add_get("/relay", relay_ws)
    app.router.add_route("*", "/go", go)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "7860"))
    web.run_app(make_app(), host="0.0.0.0", port=port)
