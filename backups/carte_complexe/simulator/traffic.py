"""
Modele de circulation : les bouchons EMERGENT, rien n'est scripte.

  - chaque voiture va vers une destination, souvent sur l'autre rive ;
  - dans une voie, une voiture ne peut pas depasser celle de devant et garde ses
    distances : c'est ce qui forme les files d'attente ;
  - une route peut avoir plusieurs voies : la voiture prend celle qui a le plus de
    place en y entrant, et la capacite est multipliee d'autant ;
  - un carrefour ne laisse entrer une voiture que s'il y a de la place : une file
    peut donc remonter jusqu'aux rues precedentes ;
  - dans un rond-point, l'anneau est prioritaire sur les voitures qui entrent ;
  - les ponts ont un debit limite (`exit_headway_s`) : c'est le goulot d'etranglement ;
  - les voitures choisissent le chemin le plus rapide selon le trafic du moment.
"""

import heapq
import math
import random

ACCELERATION = 60.0  # unites/s^2
REACTION_TIME = 1.0  # s
ENTRY_HEADWAY = 1.5  # s entre deux entrees sur une meme voie
RING_YIELD_DISTANCE = 90.0  # une voiture si proche sur l'anneau a la priorite
GIVE_UP_AFTER = 60.0  # s coince avant de renoncer et de changer de destination
ROUTE_PERIOD = 10.0  # s entre deux recalculs des itineraires
CROSS_PROBABILITY = 0.6  # part des trajets vers l'autre rive
MIN_ROAD_SPEED_RATIO = 0.05  # evite un temps de parcours infini dans le calcul d'itineraire


class Lane:
    """Une voie : une file de voitures, de la tete vers la queue."""

    def __init__(self, road, index):
        self.road = road
        self.index = index
        self.cars = []
        self.next_entry = 0.0
        self.next_exit = 0.0


class Road:
    """Un sens de circulation d'un tronceon, avec une ou plusieurs voies."""

    def __init__(self, segment, frm, to, nodes, units_per_kmh):
        self.segment = segment["id"]
        self.tunnel = bool(segment.get("tunnel"))
        self.roundabout = bool(segment.get("roundabout"))
        self.forward = frm == segment["a"]
        self.frm, self.to = frm, to
        self.start, self.end = nodes[frm], nodes[to]
        self.length = math.dist(self.start, self.end)
        self.vmax = segment.get("speed", 50) * units_per_kmh
        self.exit_headway = segment.get("exit_headway_s", 0.0)
        self.lanes = [Lane(self, i) for i in range(segment.get("lanes", 1))]
        self.mean_speed = self.vmax
        self.upstream_ring = None  # le morceau d'anneau qui arrive ici

    def cars(self):
        return [car for lane in self.lanes for car in lane.cars]


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
    def road(self):
        return self.lane.road

    @property
    def xy(self):
        road = self.road
        a, b, t = road.start, road.end, self.pos / road.length
        return a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t


class TrafficModel:
    def __init__(self, city, n_cars):
        self.units_per_kmh = city["units_per_kmh"]
        self.spacing = city["car_spacing"]
        self.nodes = {k: tuple(v) for k, v in city["nodes"].items()}
        self.segments = {s["id"]: s for s in city["segments"]}
        self.closed = {s["id"] for s in city["segments"] if s.get("closed")}

        self.roads = []
        for s in city["segments"]:
            self.roads.append(Road(s, s["a"], s["b"], self.nodes, self.units_per_kmh))
            if not s.get("oneway"):
                self.roads.append(Road(s, s["b"], s["a"], self.nodes, self.units_per_kmh))
        self.lanes = [lane for road in self.roads for lane in road.lanes]

        ring_nodes = set()
        rings_by_end = {}
        for road in self.roads:
            if road.roundabout:
                ring_nodes.update((road.frm, road.to))
                rings_by_end[road.to] = road
        for road in self.roads:
            if road.roundabout:
                road.upstream_ring = rings_by_end.get(road.frm)
        self.destinations = [n for n in self.nodes if n not in ring_nodes]

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
        lane = random.choice(self.lanes)
        road = lane.road
        if road.segment in self.closed or road.roundabout:
            return
        pos = random.uniform(0, road.length)
        if any(abs(c.pos - pos) < self.spacing for c in lane.cars):
            return
        car = Car(f"c{self.created:03d}", lane, pos)
        self.created += 1
        car.dest = self._new_destination(road.to)
        lane.cars.append(car)
        lane.cars.sort(key=lambda c: -c.pos)
        self.cars.append(car)

    # --- itineraires ---------------------------------------------------------

    def _new_destination(self, node):
        reachable = [n for n in self.next_hop[node] if n != node]
        if not reachable:
            return None
        other = [n for n in reachable if n[0] != node[0]]
        same = [n for n in reachable if n[0] == node[0]]
        pool = other if other and (random.random() < CROSS_PROBABILITY or not same) else same
        return random.choice(pool)

    def _update_routes(self):
        """Dijkstra vers chaque destination, sur le temps de parcours actuel."""
        incoming = {n: [] for n in self.nodes}
        for road in self.roads:
            if road.segment not in self.closed:
                speed = max(road.mean_speed, road.vmax * MIN_ROAD_SPEED_RATIO)
                incoming[road.to].append((road.length / speed, road))

        hop = {n: {} for n in self.nodes}
        for dest in self.destinations:
            best = {dest: 0.0}
            heap = [(0.0, dest)]
            settled = set()
            while heap:
                time_to_dest, node = heapq.heappop(heap)
                if node in settled:
                    continue
                settled.add(node)
                for weight, road in incoming[node]:
                    candidate = time_to_dest + weight
                    if candidate < best.get(road.frm, math.inf):
                        best[road.frm] = candidate
                        hop[road.frm][dest] = road if node == dest else hop[node][dest]
                        heapq.heappush(heap, (candidate, road.frm))
        self.next_hop = hop

    def _measure_roads(self):
        for road in self.roads:
            cars = road.cars()
            current = sum(c.v for c in cars) / len(cars) if cars else road.vmax
            road.mean_speed = 0.5 * road.mean_speed + 0.5 * current

    def _next_road(self, car, now):
        node = car.road.to
        if car.blocked_since is not None and now - car.blocked_since > GIVE_UP_AFTER:
            car.dest = self._new_destination(node)
            car.blocked_since = now
        if car.dest == node or car.dest not in self.next_hop[node]:
            car.dest = self._new_destination(node)
        return self.next_hop[node].get(car.dest)

    # --- dynamique -----------------------------------------------------------

    def _ring_is_busy(self, target):
        upstream = target.upstream_ring
        if upstream is None:
            return False
        return any(
            lane.cars and lane.cars[0].pos > upstream.length - RING_YIELD_DISTANCE
            for lane in upstream.lanes
        )

    def _entry_lane(self, car, lane, target, now):
        """La voie ou cette voiture peut entrer maintenant, ou None."""
        if target is None or target.segment in self.closed or now < lane.next_exit:
            return None
        if target.roundabout and not lane.road.roundabout and self._ring_is_busy(target):
            return None

        best, best_room = None, -1.0
        for candidate in target.lanes:
            if now < candidate.next_entry:
                continue
            room = candidate.cars[-1].pos if candidate.cars else math.inf
            if room > best_room:
                best, best_room = candidate, room
        return best if best is not None and best_room >= self.spacing else None

    def step(self, dt, now):
        if now >= self.next_route_update:
            self.next_route_update = now + ROUTE_PERIOD
            self._measure_roads()
            self._update_routes()

        random.shuffle(self.lanes)  # aucun carrefour n'est prioritaire
        for lane in self.lanes:
            cars = lane.cars
            if not cars:
                continue
            road = lane.road
            entry = None
            for i, car in enumerate(cars):
                v = min(car.desire * road.vmax, car.v + ACCELERATION * dt)
                if i > 0:
                    gap = cars[i - 1].pos - car.pos - self.spacing
                else:
                    entry = self._entry_lane(car, lane, self._next_road(car, now), now)
                    gap = math.inf if entry else road.length - car.pos
                    if entry is None and car.blocked_since is None:
                        car.blocked_since = now
                car.v = min(v, max(gap, 0.0) / REACTION_TIME)
                car.pos += car.v * dt

            if entry is not None and cars[0].pos >= road.length:
                car = cars.pop(0)
                overflow = car.pos - road.length
                car.lane, car.blocked_since = entry, None
                car.pos = min(overflow, entry.road.length * 0.5)
                entry.cars.append(car)
                entry.next_entry = now + ENTRY_HEADWAY
                lane.next_exit = now + road.exit_headway

    def kmh(self, car):
        return car.v / self.units_per_kmh
