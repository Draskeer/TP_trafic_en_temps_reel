"""
Serveur de l'interface.

- sert la page (static/index.html), la ville (city.json) et la config ;
- consomme les topics Kafka `positions`, `speeds`, `late`, `invalid`, `scenario`
  et rediffuse chaque message aux navigateurs connectes via WebSocket ;
- publie les commandes des boutons (route fermee, voitures) sur le topic `control` ;
- simule une panne : tue le worker Flink, puis le relance apres OUTAGE_S secondes.

Tout passe par le journal Kafka : le navigateur ne fait qu'afficher ce que la
simulation et Flink y ont ecrit.
"""

import asyncio
import json
import os
import time
from collections import deque

from aiohttp import ClientError, ClientSession, UnixConnector, web, WSMsgType
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
DOCKER_SOCKET = "/var/run/docker.sock"
FLINK_WORKER = "taskmanager"
FLINK_REST = "http://jobmanager:8081"
OUTAGE_S = 15
RECOVERY_TIMEOUT_S = 120
OUTAGE_BUSY_PHASES = ("down", "restoring")

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
    elif action in ("pause", "resume"):
        command = {"action": action}
    else:
        return web.json_response({"error": "commande invalide"}, status=400)

    if "producer" not in request.app:
        return web.json_response({"error": "Kafka pas pret"}, status=503)
    await request.app["producer"].send_and_wait(
        "control", json.dumps(command).encode("utf-8")
    )
    return web.json_response(command)


async def docker_post(path):
    """Appelle l'API Docker sur le worker Flink (kill ou start, rien d'autre)."""
    async with ClientSession(connector=UnixConnector(path=DOCKER_SOCKET)) as session:
        url = f"http://docker/containers/{FLINK_WORKER}/{path}"
        async with session.post(url) as response:
            if response.status not in (204, 304):
                raise ClientError(f"Docker a repondu {response.status}")


async def set_outage(app, phase, **details):
    app["outage"] = {"phase": phase, "at": int(time.time() * 1000), **details}
    await broadcast(app, {"type": "outage", "data": app["outage"]})


async def publish_action(app, action):
    """L'action apparait sur le graphe, a l'heure de la simulation."""
    event = {"action": action, "event_time": app["event_time"]}
    await app["producer"].send_and_wait("scenario", json.dumps(event).encode("utf-8"))


async def wait_for_restored_checkpoint():
    """Attend que le job tourne a nouveau ; renvoie le checkpoint restaure, ou None."""
    deadline = time.monotonic() + RECOVERY_TIMEOUT_S
    async with ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                async with session.get(f"{FLINK_REST}/jobs/overview") as response:
                    jobs = (await response.json())["jobs"]
                for job in jobs:
                    tasks = job["tasks"]
                    if job["state"] != "RUNNING" or tasks["running"] != tasks["total"]:
                        continue
                    url = f"{FLINK_REST}/jobs/{job['jid']}/checkpoints"
                    async with session.get(url) as response:
                        restored = (await response.json())["latest"].get("restored")
                    if restored:
                        return restored["id"]
            except (ClientError, KeyError) as e:
                print(f"[web] Flink pas encore pret ({e!r})", flush=True)
            await asyncio.sleep(1)
    return None


async def recover_after_outage(app):
    await asyncio.sleep(OUTAGE_S)
    try:
        await docker_post("start")
        await publish_action(app, "restart")
        await set_outage(app, "restoring")
        checkpoint = await wait_for_restored_checkpoint()
    except ClientError as e:
        print(f"[web] relance impossible : {e!r}", flush=True)
        checkpoint = None
    if checkpoint is None:
        await set_outage(app, "failed")
    else:
        await set_outage(app, "recovered", checkpoint=checkpoint)


async def outage(request):
    app = request.app
    if "producer" not in app:
        return web.json_response({"error": "Kafka pas pret"}, status=503)
    if app["outage"]["phase"] in OUTAGE_BUSY_PHASES:
        return web.json_response({"error": "panne deja en cours"}, status=409)
    try:
        await docker_post("kill")
    except ClientError as e:
        print(f"[web] panne impossible : {e!r}", flush=True)
        return web.json_response({"error": "Docker injoignable"}, status=500)

    await set_outage(app, "down", until_ms=int((time.time() + OUTAGE_S) * 1000))
    await publish_action(app, "outage")
    app["outage_task"] = asyncio.create_task(recover_after_outage(app))
    return web.json_response(app["outage"])


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await ws.send_str(
        json.dumps({"type": "history", "data": list(request.app["history"])})
    )
    await ws.send_str(json.dumps({"type": "outage", "data": request.app["outage"]}))
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
            if msg.topic == "positions":
                app["event_time"] = msg.value["event_time"]
            else:
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
    app["outage"] = {"phase": "idle"}
    app["event_time"] = 0
    with open("./city.json", encoding="utf-8") as f:
        app["segments"] = {s["id"] for s in json.load(f)["segments"]}
    app.router.add_get("/", index)
    app.router.add_get("/city.json", city)
    app.router.add_get("/config", config)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_post("/control", control)
    app.router.add_post("/outage", outage)
    app.router.add_static("/static", "./static")
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=8080)
