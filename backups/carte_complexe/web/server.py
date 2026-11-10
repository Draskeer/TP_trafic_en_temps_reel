"""
Serveur de l'interface.

- sert la page (static/index.html), la ville (city.json) et la config ;
- consomme les topics Kafka `positions`, `speeds`, `late`, `invalid`, `scenario`
  et rediffuse chaque message aux navigateurs connectes via WebSocket ;
- publie les commandes des boutons (route fermee, voitures) sur le topic `control`.

Tout passe par le journal Kafka : le navigateur ne fait qu'afficher ce que la
simulation et Flink y ont ecrit.
"""

import asyncio
import json
import os
from collections import deque

from aiohttp import web, WSMsgType
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
WINDOW_MS = int(os.environ.get("WINDOW_S", "60")) * 1000
WATERMARK_DELAY_MS = int(os.environ.get("WATERMARK_DELAY_S", "30")) * 1000
SPEEDUP = float(os.environ.get("SPEEDUP", "10"))
PROBE_INTERVAL_MS = int(float(os.environ.get("PROBE_INTERVAL_S", "3")) * 1000)
MAX_CARS = int(os.environ.get("MAX_CARS", "300"))

TOPIC_TO_TYPE = {
    "positions": "position",
    "speeds": "speed",
    "late": "late",
    "invalid": "invalid",
    "scenario": "scenario",
}
# Evenements gardes en memoire pour remplir le graphe apres un rechargement.
HISTORY_SIZE = 20000


async def index(request):
    return web.FileResponse("./static/index.html")


async def city(request):
    return web.FileResponse("./city.json")


async def config(request):
    return web.json_response(
        {
            "window_ms": WINDOW_MS,
            "watermark_delay_ms": WATERMARK_DELAY_MS,
            "speedup": SPEEDUP,
            "probe_interval_ms": PROBE_INTERVAL_MS,
            "max_cars": MAX_CARS,
        }
    )


async def control(request):
    try:
        body = await request.json()
    except ValueError:
        return web.json_response({"error": "JSON invalide"}, status=400)

    action = body.get("action")
    if action in ("close", "open") and body.get("segment") in request.app["segments"]:
        command = {"action": action, "segment": body["segment"]}
    elif (
        action == "set_cars"
        and isinstance(body.get("n"), int)
        and 0 <= body["n"] <= MAX_CARS
    ):
        command = {"action": action, "n": body["n"]}
    else:
        return web.json_response({"error": "commande invalide"}, status=400)

    if "producer" not in request.app:
        return web.json_response({"error": "Kafka pas pret"}, status=503)
    await request.app["producer"].send_and_wait(
        "control", json.dumps(command).encode("utf-8")
    )
    return web.json_response(command)


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await ws.send_str(
        json.dumps({"type": "history", "data": list(request.app["history"])})
    )
    request.app["clients"].add(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        request.app["clients"].discard(ws)
    return ws


async def broadcast(app, payload):
    if not app["clients"]:
        return
    dead = []
    data = json.dumps(payload)
    for ws in app["clients"]:
        try:
            await ws.send_str(data)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        app["clients"].discard(ws)


async def kafka_loop(app):
    consumer = AIOKafkaConsumer(
        *TOPIC_TO_TYPE.keys(),
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    for client in (consumer, producer):
        while True:
            try:
                await client.start()
                break
            except Exception as e:  # noqa: BLE001
                print(f"[web] Kafka pas pret ({e}), nouvelle tentative...", flush=True)
                await asyncio.sleep(2)
    app["producer"] = producer
    print(f"[web] connecte a Kafka ({BOOTSTRAP})", flush=True)
    try:
        async for msg in consumer:
            payload = {"type": TOPIC_TO_TYPE[msg.topic], "data": msg.value}
            if msg.topic != "positions":
                app["history"].append(payload)
            await broadcast(app, payload)
    finally:
        await consumer.stop()
        await producer.stop()


async def on_startup(app):
    app["kafka_task"] = asyncio.create_task(kafka_loop(app))


async def on_cleanup(app):
    app["kafka_task"].cancel()


def make_app():
    app = web.Application()
    app["clients"] = set()
    app["history"] = deque(maxlen=HISTORY_SIZE)
    with open("./city.json", encoding="utf-8") as f:
        app["segments"] = {s["id"] for s in json.load(f)["segments"]}
    app.router.add_get("/", index)
    app.router.add_get("/city.json", city)
    app.router.add_get("/config", config)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_post("/control", control)
    app.router.add_static("/static", "./static")
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=8080)
