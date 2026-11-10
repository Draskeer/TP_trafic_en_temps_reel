"""
Modele de circulation : les bouchons EMERGENT, rien n'est scripte.

  - chaque voiture va vers une destination, souvent sur l'autre rive ;
  - sur une voie, une voiture ne peut pas depasser celle de devant et garde ses
    distances : c'est ce qui forme les files d'attente ;
  - un carrefour ne laisse entrer qu'une voiture a la fois, et seulement s'il y a
    de la place : une file peut donc remonter jusqu'aux rues precedentes ;
  - les ponts ont un debit limite (`exit_headway_s`) : c'est le goulot d'etranglement ;
  - les voitures choisissent le chemin le plus rapide selon le trafic du moment.
"""

import math
import random

ACCELERATION = 60.0  # unites/s^2
REACTION_TIME = 1.0  # s
ENTRY_HEADWAY = 1.5  # s entre deux entrees sur une meme voie
BLOCK_TIMEOUT = 60.0  # s bloque avant de forcer le passage (evite un blocage total)
ROUTE_PERIOD = 10.0  # s entre deux recalculs des itineraires
CROSS_PROBABILITY = 0.6  # part des trajets vers l'autre rive
MIN_LANE_SPEED_RATIO = 0.05  # evite un temps de parcours infini dans le calcul d'itineraire


class Lane:
    """Un sens de circulation d'un tronceon."""

    def __init__(self, segment, frm, to, nodes, units_per_kmh):
        self.segment = segment["id"]
        self.tunnel = bool(segment.get("tunnel"))
        self.forward = frm == segment["a"]
        self.frm, self.to = frm, to
        self.start, self.end = nodes[frm], nodes[to]
        self.length = math.dist(self.start, self.end)
        self.vmax = segment.get("speed", 50) * units_per_kmh
        self.exit_headway = segment.get("exit_headway_s", 0.0)
        self.cars = []  # de la tete de file vers la queue
        self.next_entry = 0.0
        self.next_exit = 0.0
        self.mean_speed = self.vmax


class Car:
    def __init__(self, car_id, lane, pos):
        self.id = car_id
        self.lane = lane
        self.pos = pos
        self.v = 0.0
        self.desire = random.uniform(0.9, 1.1)  # conducteur plus ou moins rapide
        self.dest = None
        self.blocked_since = None

    @property
    def xy(self):
        a, b, t = self.lane.start, self.lane.end, self.pos / self.lane.length
        return a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t


class TrafficModel:
    def __init__(self, city, n_cars):
        self.units_per_kmh = city["units_per_kmh"]
        self.spacing = city["car_spacing"]
        self.nodes = {k: tuple(v) for k, v in city["nodes"].items()}
        self.segments = {s["id"]: s for s in city["segments"]}
        self.closed = {s["id"] for s in city["segments"] if s.get("closed")}
        self.lanes = {}
        for s in city["segments"]:
            self.lanes[(s["a"], s["b"])] = Lane(s, s["a"], s["b"], self.nodes, self.units_per_kmh)
            self.lanes[(s["b"], s["a"])] = Lane(s, s["b"], s["a"], self.nodes, self.units_per_kmh)
        self.lane_list = list(self.lanes.values())
        self.next_hop = {}
        self.next_route_update = 0.0
        self.created = 0
        self.cars = []
        self._update_routes()
        self.set_cars(n_cars)

    # --- commandes -----------------------------------------------------------

    def close(self, segment):
        self.closed.add(segment)
        self._update_routes()

    def open(self, segment):
        self.closed.discard(segment)
        self._update_routes()

    def set_cars(self, n):
        while len(self.cars) > n:
            car = self.cars.pop(random.randrange(len(self.cars)))
            car.lane.cars.remove(car)
        attempts = 0
        while len(self.cars) < n and attempts < 20 * n:
            attempts += 1
            self._try_spawn()

    def _try_spawn(self):
        lane = random.choice(self.lane_list)
        if lane.segment in self.closed:
            return
        pos = random.uniform(0, lane.length)
        if any(abs(c.pos - pos) < self.spacing for c in lane.cars):
            return
        car = Car(f"c{self.created:03d}", lane, pos)
        self.created += 1
        car.dest = self._new_destination(lane.to)
        lane.cars.append(car)
        lane.cars.sort(key=lambda c: -c.pos)
        self.cars.append(car)

    # --- itineraires ---------------------------------------------------------

    def _new_destination(self, node):
        reachable = list(self.next_hop.get(node, {}))
        if not reachable:
            return None
        other = [n for n in reachable if n[0] != node[0]]
        same = [n for n in reachable if n[0] == node[0]]
        pool = other if other and (random.random() < CROSS_PROBABILITY or not same) else same
        return random.choice(pool)

    def _update_routes(self):
        """Floyd-Warshall sur le temps de parcours actuel de chaque voie."""
        names = list(self.nodes)
        dist = {u: {v: math.inf for v in names} for u in names}
        hop = {u: {} for u in names}
        for lane in self.lane_list:
            if lane.segment in self.closed:
                continue
            speed = max(lane.mean_speed, lane.vmax * MIN_LANE_SPEED_RATIO)
            dist[lane.frm][lane.to] = lane.length / speed
            hop[lane.frm][lane.to] = (lane.frm, lane.to)
        for k in names:
            dk = dist[k]
            for i in names:
                di = dist[i]
                if di[k] == math.inf:
                    continue
                for j in names:
                    if i != j and di[k] + dk[j] < di[j]:
                        di[j] = di[k] + dk[j]
                        hop[i][j] = hop[i][k]
        self.next_hop = hop

    def _measure_lanes(self):
        for lane in self.lane_list:
            current = (
                sum(c.v for c in lane.cars) / len(lane.cars) if lane.cars else lane.vmax
            )
            lane.mean_speed = 0.5 * lane.mean_speed + 0.5 * current

    def _next_lane(self, car):
        node = car.lane.to
        if car.dest == node or car.dest not in self.next_hop[node]:
            car.dest = self._new_destination(node)
        key = self.next_hop[node].get(car.dest)
        return self.lanes[key] if key else None

    # --- dynamique -----------------------------------------------------------

    def _can_enter(self, car, lane, target, now):
        if target is None or target.segment in self.closed:
            return False
        if now < lane.next_exit or now < target.next_entry:
            return False
        if target.cars and target.cars[-1].pos < self.spacing:
            waited = now - (car.blocked_since if car.blocked_since is not None else now)
            return waited > BLOCK_TIMEOUT
        return True

    def step(self, dt, now):
        if now >= self.next_route_update:
            self.next_route_update = now + ROUTE_PERIOD
            self._measure_lanes()
            self._update_routes()

        random.shuffle(self.lane_list)  # aucun carrefour n'est prioritaire
        for lane in self.lane_list:
            cars = lane.cars
            target = None
            free = False
            for i, car in enumerate(cars):
                v = min(car.desire * lane.vmax, car.v + ACCELERATION * dt)
                if i > 0:
                    gap = cars[i - 1].pos - car.pos - self.spacing
                else:
                    target = self._next_lane(car)
                    free = self._can_enter(car, lane, target, now)
                    gap = math.inf if free else lane.length - car.pos
                    if not free and car.blocked_since is None:
                        car.blocked_since = now
                car.v = min(v, max(gap, 0.0) / REACTION_TIME)
                car.pos += car.v * dt

            if cars and free and cars[0].pos >= lane.length:
                car = cars.pop(0)
                overflow = car.pos - lane.length
                car.lane, car.blocked_since = target, None
                car.pos = min(overflow, target.length * 0.5)
                target.cars.append(car)
                target.next_entry = now + ENTRY_HEADWAY
                lane.next_exit = now + lane.exit_headway

    def kmh(self, car):
        return car.v / self.units_per_kmh
