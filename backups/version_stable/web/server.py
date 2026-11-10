"""
Serveur de l'interface.

- sert la page (static/index.html), la ville (city.json) et la config ;
- consomme les topics Kafka `positions`, `speeds`, `late`, `invalid`, `scenario`
  et rediffuse chaque message aux navigateurs connectes via WebSocket ;
- publie les commandes des boutons (route fermee, voitures) sur le topic `control` ;
- simule une panne : tue le worker Flink d'une rive, puis le relance apres OUTAGE_S
  secondes ;
- relance toute la demo a zero (jobs Flink, topics Kafka, simulateur).

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
from aiokafka.admin import AIOKafkaAdminClient, RecordsToDelete
from aiokafka.structs import TopicPartition

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
FLINK_WORKERS = ("taskmanager-1", "taskmanager-2")
SIMULATOR = "simulator"
JOB_SUBMITTER = "job-submitter"  # avec les workers, les seuls conteneurs pilotes
SIDES = ("w", "e")
ALL_TOPICS = ("positions", "probes", "speeds", "late", "invalid", "control", "scenario")
ENDED_JOB_STATES = ("CANCELED", "FAILED", "FINISHED")
JOBS_TIMEOUT_S = 120
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


async def docker_post(container, action):
    """Appelle l'API Docker sur un conteneur de la demo (kill, start ou stop)."""
    async with ClientSession(connector=UnixConnector(path=DOCKER_SOCKET)) as session:
        url = f"http://docker/containers/{container}/{action}"
        async with session.post(url) as response:
            if response.status not in (204, 304):
                raise ClientError(f"Docker a repondu {response.status}")


async def set_outage(app, side, phase, **details):
    state = {"side": side, "phase": phase, "at": int(time.time() * 1000), **details}
    app["outage"][side] = state
    await broadcast(app, {"type": "outage", "data": state})


async def publish_action(app, action, side):
    """L'action apparait sur le graphe, a l'heure de la simulation."""
    event = {"action": action, "side": side, "event_time": app["event_time"]}
    await app["producer"].send_and_wait("scenario", json.dumps(event).encode("utf-8"))


async def active_jobs(session):
    async with session.get(f"{FLINK_REST}/jobs/overview") as response:
        jobs = (await response.json())["jobs"]
    return [j for j in jobs if j["state"] not in ENDED_JOB_STATES]


async def flink_job(session, side):
    jobs = await active_jobs(session)
    return next((j for j in jobs if j["name"] == f"trafic-{side}"), None)


async def find_worker(side):
    """Le conteneur qui fait tourner le job de cette rive, ou None."""
    async with ClientSession() as session:
        job = await flink_job(session, side)
        if job is None or job["state"] != "RUNNING":
            return None
        base = f"{FLINK_REST}/jobs/{job['jid']}"
        async with session.get(base) as response:
            vertex = (await response.json())["vertices"][0]["id"]
        async with session.get(f"{base}/vertices/{vertex}") as response:
            host = (await response.json())["subtasks"][0]["host"].split(":")[0]
    return host if host in FLINK_WORKERS else None


async def wait_for_restored_checkpoint(side):
    """Attend que le job tourne a nouveau ; renvoie le checkpoint restaure, ou None."""
    deadline = time.monotonic() + RECOVERY_TIMEOUT_S
    async with ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                job = await flink_job(session, side)
                tasks = job["tasks"] if job else {}
                if job and job["state"] == "RUNNING" and tasks["running"] == tasks["total"]:
                    url = f"{FLINK_REST}/jobs/{job['jid']}/checkpoints"
                    async with session.get(url) as response:
                        restored = (await response.json())["latest"].get("restored")
                    if restored:
                        return restored["id"]
            except (ClientError, KeyError) as e:
                print(f"[web] Flink pas encore pret ({e!r})", flush=True)
            await asyncio.sleep(1)
    return None


async def recover_after_outage(app, side, worker):
    await asyncio.sleep(OUTAGE_S)
    try:
        await docker_post(worker, "start")
        await publish_action(app, "restart", side)
        await set_outage(app, side, "restoring")
        checkpoint = await wait_for_restored_checkpoint(side)
    except ClientError as e:
        print(f"[web] relance impossible : {e!r}", flush=True)
        checkpoint = None
    if checkpoint is None:
        await set_outage(app, side, "failed")
    else:
        await set_outage(app, side, "recovered", checkpoint=checkpoint)


async def outage(request):
    app = request.app
    try:
        side = (await request.json()).get("side")
    except ValueError:
        return web.json_response({"error": "JSON invalide"}, status=400)
    if side not in SIDES:
        return web.json_response({"error": "rive inconnue"}, status=400)
    if app["reset_phase"] == "running":
        return web.json_response({"error": "relance en cours"}, status=409)
    if "producer" not in app:
        return web.json_response({"error": "Kafka pas pret"}, status=503)
    if app["outage"][side]["phase"] in OUTAGE_BUSY_PHASES:
        return web.json_response({"error": "panne deja en cours"}, status=409)
    try:
        worker = await find_worker(side)
        if worker is None:
            return web.json_response({"error": "job pas pret"}, status=409)
        await docker_post(worker, "kill")
    except (ClientError, KeyError, IndexError) as e:
        print(f"[web] panne impossible : {e!r}", flush=True)
        return web.json_response({"error": "Docker ou Flink injoignable"}, status=500)

    await set_outage(app, side, "down", worker=worker, until_ms=int((time.time() + OUTAGE_S) * 1000))
    await publish_action(app, "outage", side)
    app["outage_tasks"][side] = asyncio.create_task(recover_after_outage(app, side, worker))
    return web.json_response(app["outage"][side])


async def wait_for_submitter():
    """Juste apres le demarrage, le soumetteur peut encore etre en train de lancer les jobs."""
    deadline = time.monotonic() + JOBS_TIMEOUT_S
    async with ClientSession(connector=UnixConnector(path=DOCKER_SOCKET)) as session:
        while time.monotonic() < deadline:
            async with session.get(f"http://docker/containers/{JOB_SUBMITTER}/json") as response:
                if not (await response.json())["State"]["Running"]:
                    return
            await asyncio.sleep(0.5)
    raise ClientError("le soumetteur de jobs ne se termine pas")


async def cancel_jobs():
    deadline = time.monotonic() + JOBS_TIMEOUT_S
    async with ClientSession() as session:
        for job in await active_jobs(session):
            async with session.patch(f"{FLINK_REST}/jobs/{job['jid']}?mode=cancel"):
                pass
        while await active_jobs(session):
            if time.monotonic() > deadline:
                raise ClientError("les jobs Flink ne s'arretent pas")
            await asyncio.sleep(0.5)


async def wait_for_jobs():
    expected = {f"trafic-{side}" for side in SIDES}
    deadline = time.monotonic() + JOBS_TIMEOUT_S
    async with ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                running = {
                    j["name"]
                    for j in await active_jobs(session)
                    if j["state"] == "RUNNING" and j["tasks"]["running"] == j["tasks"]["total"]
                }
                if expected <= running:
                    return
            except ClientError as e:
                print(f"[web] Flink pas encore pret ({e!r})", flush=True)
            await asyncio.sleep(1)
    raise ClientError("les jobs Flink ne demarrent pas")


async def empty_topics():
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        # -1 : tout supprimer jusqu'au dernier message ecrit.
        await admin.delete_records(
            {TopicPartition(t, 0): RecordsToDelete(before_offset=-1) for t in ALL_TOPICS}
        )
    finally:
        await admin.close()


async def restart_simulation(app):
    """Tout repart de zero, jobs d'abord : aucun point n'est emis avant qu'ils soient prets."""
    try:
        for task in app["outage_tasks"].values():
            task.cancel()
        await wait_for_submitter()
        await cancel_jobs()
        await docker_post(SIMULATOR, "stop?t=0")
        for worker in FLINK_WORKERS:
            await docker_post(worker, "start")
        await empty_topics()
        app["history"].clear()
        app["event_time"] = 0
        app["outage"] = idle_outages()
        await docker_post(JOB_SUBMITTER, "start")
        await wait_for_jobs()
        await docker_post(SIMULATOR, "start")
    except Exception as e:  # noqa: BLE001
        print(f"[web] relance a zero impossible : {e!r}", flush=True)
        app["reset_phase"] = "failed"
        await broadcast(app, {"type": "reset", "data": {"phase": "failed"}})
        return
    app["reset_phase"] = "idle"
    await broadcast(app, {"type": "reset", "data": {"phase": "done"}})


async def reset(request):
    app = request.app
    if app["reset_phase"] == "running":
        return web.json_response({"error": "relance deja en cours"}, status=409)
    app["reset_phase"] = "running"
    await broadcast(app, {"type": "reset", "data": {"phase": "running"}})
    app["reset_task"] = asyncio.create_task(restart_simulation(app))
    return web.json_response({"phase": "running"})


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await ws.send_str(
        json.dumps({"type": "history", "data": list(request.app["history"])})
    )
    for state in request.app["outage"].values():
        await ws.send_str(json.dumps({"type": "outage", "data": state}))
    await ws.send_str(
        json.dumps({"type": "reset", "data": {"phase": request.app["reset_phase"]}})
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


def idle_outages():
    return {side: {"side": side, "phase": "idle"} for side in SIDES}


def make_app():
    app = web.Application()
    app["clients"] = set()
    app["history"] = deque(maxlen=HISTORY_SIZE)
    app["outage"] = idle_outages()
    app["outage_tasks"] = {}
    app["reset_phase"] = "idle"
    app["event_time"] = 0
    with open("./city.json", encoding="utf-8") as f:
        app["segments"] = {s["id"] for s in json.load(f)["segments"]}
    app.router.add_get("/", index)
    app.router.add_get("/city.json", city)
    app.router.add_get("/config", config)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_post("/control", control)
    app.router.add_post("/outage", outage)
    app.router.add_post("/reset", reset)
    app.router.add_static("/static", "./static")
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=8080)
