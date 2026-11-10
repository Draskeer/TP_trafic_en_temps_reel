"""
Simulateur de ville — LA SOURCE DU FLUX.

Les voitures roulent selon le modele de circulation (traffic.py), dans un repere
10000 x 10000. Le simulateur envoie EN LIVE :

  - topic `positions` : l'etat reel de toutes les voitures a chaque tick
    -> sert uniquement a l'affichage ;
  - topic `probes` : le point GPS du telephone (car_id, x, y, event_time) toutes
    les PROBE_INTERVAL_S -> c'est ce que Flink traite ;
  - topic `scenario` : les actions appliquees (route fermee, voitures ajoutees).

Il ecoute le topic `control`, alimente par les boutons de l'interface.

Le "temps" est accelere : 1 s reelle = SPEEDUP s d'event-time.

Dans le TUNNEL :
  - le telephone n'a plus de reseau : il garde ses points en file (FIFO) et les
    envoie a la reconnexion, apres la sortie + un delai aleatoire. Leur event_time
    reste celui de la mesure -> certains arrivent a temps, d'autres trop tard ;
  - le GPS y est mauvais : une partie des points est aberrante (saut hors route,
    hors carte, ou teleportation sur une autre rue).
"""

import json
import math
import os
import random
import time
from collections import deque

from kafka import KafkaConsumer, KafkaProducer

from traffic import TrafficModel

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SPEEDUP = float(os.environ.get("SPEEDUP", "10"))
TICK_REAL = float(os.environ.get("TICK_REAL", "0.2"))
PROBE_INTERVAL_S = float(os.environ.get("PROBE_INTERVAL_S", "3"))
TUNNEL_RECONNECT_MAX_S = float(os.environ.get("TUNNEL_RECONNECT_MAX_S", "60"))
GPS_ERROR_TUNNEL = float(os.environ.get("GPS_ERROR_TUNNEL", "0.25"))
GPS_ERROR_CITY = float(os.environ.get("GPS_ERROR_CITY", "0.02"))
N_CARS = int(os.environ.get("N_CARS", "50"))
MAX_CARS = int(os.environ.get("MAX_CARS", "300"))

SUBSTEPS = 4  # pas de calcul du modele de circulation par tick
GPS_NOISE = 15.0  # bruit normal d'un GPS, en unites de la carte
OFF_ROAD_DIST = 280.0  # un point "hors route" est au moins a cette distance des rues
TELEPORT_DIST = 3500.0  # une teleportation saute au moins de cette distance


def load_city(path="/app/city.json"):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


class Phone:
    """Le telephone d'une voiture : quand emettre, et le reseau."""

    def __init__(self, vt):
        self.next_probe = vt + random.uniform(0, PROBE_INTERVAL_S)
        self.queue = deque()
        self.offline = False
        self.reconnect_at = None

    def update_network(self, vt, in_tunnel):
        """Coupe le reseau dans le tunnel ; le retablit apres un delai aleatoire."""
        if in_tunnel:
            self.offline, self.reconnect_at = True, None
        elif self.offline and self.reconnect_at is None:
            self.reconnect_at = vt + random.uniform(0, TUNNEL_RECONNECT_MAX_S)


def road_distance(x, y, model):
    best = math.inf
    for s in model.segments.values():
        ax, ay = model.nodes[s["a"]]
        bx, by = model.nodes[s["b"]]
        dx, dy = bx - ax, by - ay
        t = max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy)))
        best = min(best, math.hypot(x - ax - t * dx, y - ay - t * dy))
    return best


def corrupt(x, y, model, bounds):
    """Fabrique un point GPS aberrant, de l'un des trois types."""
    kind = random.choice(["hors_route", "hors_carte", "teleportation"])
    if kind == "hors_carte":
        return bounds["w"] + random.uniform(500, 3000), y
    for _ in range(50):
        if kind == "hors_route":
            p = (random.uniform(0, bounds["w"]), random.uniform(0, bounds["h"]))
            if road_distance(p[0], p[1], model) > OFF_ROAD_DIST:
                return p
        else:
            # Teleportation : un point SUR une route, mais trop loin pour etre possible.
            s = random.choice(list(model.segments.values()))
            a, b, t = model.nodes[s["a"]], model.nodes[s["b"]], random.random()
            p = (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)
            if math.dist(p, (x, y)) > TELEPORT_DIST:
                return p
    return x, y


def connect(factory, name):
    while True:
        try:
            client = factory()
            print(f"[sim] {name} connecte a Kafka ({BOOTSTRAP})", flush=True)
            return client
        except Exception as e:  # noqa: BLE001
            print(f"[sim] Kafka pas pret ({e}), nouvelle tentative...", flush=True)
            time.sleep(2)


def apply_commands(consumer, model, producer, now_ms):
    for records in consumer.poll(timeout_ms=0).values():
        for record in records:
            cmd = record.value
            action = cmd.get("action")
            if action == "close":
                model.close(cmd["segment"])
            elif action == "open":
                model.open(cmd["segment"])
            elif action == "set_cars":
                cmd["n"] = max(0, min(int(cmd["n"]), MAX_CARS))
                model.set_cars(cmd["n"])
            else:
                continue
            print(f"[sim] commande : {cmd}", flush=True)
            producer.send("scenario", {**cmd, "event_time": now_ms})


def main():
    city = load_city()
    model = TrafficModel(city, N_CARS)
    producer = connect(
        lambda: KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=20,
        ),
        "producteur",
    )
    consumer = connect(
        lambda: KafkaConsumer(
            "control",
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        ),
        "consommateur",
    )

    vt = 0.0  # temps de l'evenement (s), accelere
    phones = {}
    tick_virtual = TICK_REAL * SPEEDUP

    print(
        f"[sim] {N_CARS} voitures | speedup x{SPEEDUP} | reconnexion tunnel "
        f"0-{TUNNEL_RECONNECT_MAX_S}s | erreurs GPS tunnel {GPS_ERROR_TUNNEL:.0%}",
        flush=True,
    )

    while True:
        apply_commands(consumer, model, producer, int(vt * 1000))
        for i in range(SUBSTEPS):
            model.step(tick_virtual / SUBSTEPS, vt + tick_virtual * (i + 1) / SUBSTEPS)
        vt += tick_virtual
        now_ms = int(vt * 1000)

        snapshot = []
        for car in model.cars:
            x, y = car.xy
            phone = phones.setdefault(car.id, Phone(vt))
            in_tunnel = car.road.tunnel
            phone.update_network(vt, in_tunnel)
            snapshot.append(
                [
                    car.id,
                    round(x, 1),
                    round(y, 1),
                    round(model.kmh(car), 1),
                    car.road.segment,
                    1 if car.road.forward else -1,
                    1 if phone.offline else 0,
                    car.lane.index,
                ]
            )

            # Point GPS du telephone — periodique, et parfois faux.
            if vt >= phone.next_probe:
                phone.next_probe += PROBE_INTERVAL_S
                gx = x + random.gauss(0, GPS_NOISE)
                gy = y + random.gauss(0, GPS_NOISE)
                if random.random() < (GPS_ERROR_TUNNEL if in_tunnel else GPS_ERROR_CITY):
                    gx, gy = corrupt(x, y, model, city["bounds"])
                probe = {
                    "car_id": car.id,
                    "x": round(gx, 1),
                    "y": round(gy, 1),
                    "event_time": now_ms,
                }
                # Hors reseau : on garde le point. L'event_time reste celui de la mesure.
                if phone.offline:
                    phone.queue.append(probe)
                else:
                    producer.send("probes", probe)

            # Reconnexion : tout part d'un coup, dans l'ordre de la file.
            if phone.reconnect_at is not None and vt >= phone.reconnect_at:
                while phone.queue:
                    producer.send("probes", phone.queue.popleft())
                phone.offline, phone.reconnect_at = False, None

        # Etat reel de la ville pour l'affichage — toujours a l'heure.
        producer.send(
            "positions",
            {
                "event_time": now_ms,
                "n_cars": len(model.cars),
                "closed": sorted(model.closed),
                "cars": snapshot,
            },
        )

        if len(phones) > len(model.cars):
            alive = {car.id for car in model.cars}
            phones = {k: v for k, v in phones.items() if k in alive}

        time.sleep(TICK_REAL)


if __name__ == "__main__":
    main()
