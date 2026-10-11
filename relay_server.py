#!/usr/bin/env python3
"""
سيرفر Relay بسيط لغرف PvP أونلاين.
بيربط اللاعبين بكود غرفة (4 حروف) وبيحوّل الرسائل بينهم. مفيش منطق لعبة هنا.

تشغيل:
    pip install websockets==12.0
    python relay_server.py            # المنفذ الافتراضي 8765 (أو متغير PORT)

الاتصال من اللعبة:  ws://IP:8765   (أو wss://دومين لو السيرفر وراء HTTPS)
"""
import asyncio
import http
import json
import os
import random

import websockets

MAX_PLAYERS = 8
CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"   # من غير حروف بتتلخبط (O/0/I/1)
rooms = {}   # code -> room dict


def make_code():
    while True:
        code = "".join(random.choice(CODE_CHARS) for _ in range(4))
        if code not in rooms:
            return code


def clean_name(value):
    name = str(value or "Player").strip()[:16]
    return name or "Player"


def plist(room):
    return [{"id": pid, "name": room["names"][pid]} for pid in room["players"]]


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass


async def broadcast(room, obj, exclude=None):
    for pid, ws in list(room["players"].items()):
        if pid != exclude:
            await send(ws, obj)


async def remove_player(room, pid):
    if pid not in room["players"]:
        return
    del room["players"][pid]
    room["names"].pop(pid, None)
    if not room["players"]:
        rooms.pop(room["code"], None)
        return
    if room["open"] and pid == room["host"]:
        # الـ Host خرج قبل بداية الماتش: نقفل الغرفة
        await broadcast(room, {"t": "closed"})
        rooms.pop(room["code"], None)
        return
    await broadcast(room, {"t": "left", "id": pid})
    await broadcast(room, {"t": "players", "players": plist(room)})


async def handler(ws):
    room = None
    pid = 0
    try:
        async for raw in ws:
            if isinstance(raw, bytes) or len(raw) > 4096:
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            t = msg.get("t")

            if t == "create" and room is None:
                code = make_code()
                settings = msg.get("settings")
                room = {
                    "code": code,
                    "players": {1: ws},
                    "names": {1: clean_name(msg.get("name"))},
                    "host": 1,
                    "next": 2,
                    "open": True,
                    "settings": settings if isinstance(settings, dict) else {},
                }
                rooms[code] = room
                pid = 1
                await send(ws, {"t": "created", "code": code, "id": 1, "players": plist(room)})

            elif t == "join" and room is None:
                code = str(msg.get("code", "")).upper().strip()
                target = rooms.get(code)
                if target is None:
                    await send(ws, {"t": "error", "msg": "Room not found"})
                elif not target["open"]:
                    await send(ws, {"t": "error", "msg": "The match already started"})
                elif len(target["players"]) >= MAX_PLAYERS:
                    await send(ws, {"t": "error", "msg": "Room is full"})
                else:
                    room = target
                    pid = room["next"]
                    room["next"] += 1
                    room["players"][pid] = ws
                    room["names"][pid] = clean_name(msg.get("name"))
                    await send(ws, {"t": "joined", "code": code, "id": pid, "host": room["host"],
                                    "players": plist(room), "settings": room["settings"]})
                    await broadcast(room, {"t": "players", "players": plist(room)}, exclude=pid)

            elif room is not None:
                if t == "relay":
                    d = msg.get("d")
                    if not isinstance(d, dict):
                        continue
                    out = {"t": "msg", "from": pid, "d": d}
                    try:
                        to = int(msg.get("to", 0))
                    except (TypeError, ValueError):
                        continue
                    if to == 0:
                        await broadcast(room, out, exclude=pid)
                    else:
                        peer = room["players"].get(to)
                        if peer is not None:
                            await send(peer, out)
                elif t == "settings" and pid == room["host"] and room["open"]:
                    s = msg.get("s")
                    if isinstance(s, dict):
                        room["settings"] = s
                        await broadcast(room, {"t": "settings", "s": s}, exclude=pid)
                elif t == "start" and pid == room["host"] and room["open"]:
                    if len(room["players"]) >= 2:
                        room["open"] = False
                        await broadcast(room, {"t": "start", "s": room["settings"]})
                    else:
                        await send(ws, {"t": "error", "msg": "Need at least 2 players"})
                elif t == "leave":
                    break
    except websockets.ConnectionClosed:
        pass
    finally:
        if room is not None:
            await remove_player(room, pid)


async def health(path, request_headers):
    # يرد على طلبات HTTP العادية (فحص الاستضافة) من غير ما يأثر على WebSocket
    if request_headers.get("Upgrade", "").lower() != "websocket":
        return http.HTTPStatus.OK, [("Content-Type", "text/plain")], b"relay ok\n"
    return None


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with websockets.serve(handler, "0.0.0.0", port, process_request=health,
                                max_size=8192, ping_interval=20, ping_timeout=20):
        print(f"Relay server listening on port {port}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
