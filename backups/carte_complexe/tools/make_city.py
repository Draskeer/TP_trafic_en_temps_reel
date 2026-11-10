"""
Genere city.json : deux rives, reliees par deux ponts.

    python3 tools/make_city.py

Hierarchie des routes : rocades 2x3 voies, axes 2x2 voies, petites rues a une voie
(dont des sens uniques). Certains carrefours sont transformes en ronds-points : un
anneau de petits tronceons a sens unique, parcouru dans le sens anti-horaire.
"""

import json
import math
import random
from pathlib import Path

OUTPUT = Path(__file__).resolve().parent.parent / "city.json"

COLUMNS = {"w": [600, 1500, 2400, 3300, 4200], "e": [5800, 6700, 7600, 8500, 9400]}
ROWS = [9200, 7800, 6400, 4800, 3200, 1400]
JITTER = 110
RING_RADIUS = 180

ROCADE = {"lanes": 3, "speed": 80}
AXE = {"lanes": 2, "speed": 50}
AVENUE = {"lanes": 1, "speed": 40}
RUELLE = {"lanes": 1, "speed": 30}
RING = {"lanes": 1, "speed": 25}

OUTER_COLUMN = {"w": 0, "e": 4}
QUAI_COLUMN = {"w": 4, "e": 0}
HILL_NODE = "e23"  # la colline : le tunnel passe dessous

ROCADE_NAME = {"w": "Rocade Ouest", "e": "Rocade Est"}
QUAI_NAME = {"w": "Quai Ouest", "e": "Quai Est"}
AVENUE_NAME = {"w": "Avenue Victor-Hugo", "e": "Avenue Pasteur"}
RUE_NAME = {"w": "Rue des Écoles", "e": "Rue du Marché"}
RUELLE_NAME = {("w", 1): "Rue de la Mairie", ("w", 3): "Rue des Lilas",
               ("e", 1): "Rue du Lycée", ("e", 3): "Rue des Tilleuls"}

# Quelques petites rues en moins, pour casser le quadrillage.
REMOVED = {
    ("h", "w", 3, 1), ("h", "w", 1, 3), ("v", "w", 1, 0), ("v", "w", 3, 1), ("v", "w", 3, 4),
    ("h", "e", 1, 0), ("h", "e", 3, 0), ("v", "e", 1, 4), ("v", "e", 3, 0),
}
ONE_WAY = {("h", "w", 3, 0), ("v", "w", 1, 3), ("h", "e", 3, 3), ("v", "e", 3, 2)}
ROUNDABOUTS = ["w22", "w24", "w11", "e32", "e14", "e41"]


def node_id(side, col, row):
    return f"{side}{col}{row}"


def horizontal(side, row):
    if row in (0, 5):
        return ROCADE_NAME[side], ROCADE
    if row == 2:
        return "Rue Principale", AXE
    if row == 4:
        return "Boulevard Sud", AXE
    if row == 1:
        return AVENUE_NAME[side], AVENUE
    return RUE_NAME[side], RUELLE


def vertical(side, col):
    if col == OUTER_COLUMN[side]:
        return ROCADE_NAME[side], ROCADE
    if col == QUAI_COLUMN[side]:
        return QUAI_NAME[side], AVENUE
    if col == 2:
        return "Boulevard Central", AXE
    return RUELLE_NAME[(side, col)], RUELLE


def segment(a, b, name, kind, **extra):
    return {"id": f"{a}-{b}", "a": a, "b": b, "name": name, **kind, **extra}


def build_nodes(rng):
    nodes = {}
    for side, columns in COLUMNS.items():
        for c, x in enumerate(columns):
            for r, y in enumerate(ROWS):
                nodes[node_id(side, c, r)] = [
                    x + rng.randint(-JITTER, JITTER),
                    y + rng.randint(-JITTER, JITTER),
                ]
    del nodes[HILL_NODE]
    return nodes


def build_segments(nodes):
    segments = []
    for side in COLUMNS:
        for r in range(len(ROWS)):
            for c in range(4):
                a, b = node_id(side, c, r), node_id(side, c + 1, r)
                key = ("h", side, r, c)
                if key in REMOVED or a not in nodes or b not in nodes:
                    continue
                name, kind = horizontal(side, r)
                extra = {"oneway": True} if key in ONE_WAY else {}
                segments.append(segment(a, b, name, kind, **extra))
        for c in range(5):
            for r in range(len(ROWS) - 1):
                a, b = node_id(side, c, r), node_id(side, c, r + 1)
                key = ("v", side, c, r)
                if key in REMOVED or a not in nodes or b not in nodes:
                    continue
                name, kind = vertical(side, c)
                extra = {"oneway": True} if key in ONE_WAY else {}
                segments.append(segment(a, b, name, kind, **extra))

    segments.append(segment("e22", "e24", "Tunnel de la Colline", {"lanes": 2, "speed": 60}, tunnel=True))
    # La Rue Principale passe de 2 voies a 1 sur le pont : l'entonnoir.
    bridge = {"lanes": 1, "speed": 50, "crossing": True, "exit_headway_s": 6}
    segments.append(segment("w42", "e02", "Rue Principale", bridge))
    segments.append(segment("w44", "e04", "Pont Sud", bridge, closed=True, works=True))
    return segments


def add_roundabout(center, nodes, segments):
    cx, cy = nodes.pop(center)
    arms = []
    for s in segments:
        for end, other in (("a", "b"), ("b", "a")):
            if s[end] == center:
                ox, oy = nodes[s[other]] if s[other] in nodes else (cx, cy)
                arms.append((math.atan2(oy - cy, ox - cx), s, end))
    if len(arms) < 3:
        raise ValueError(f"{center} : un rond-point demande au moins 3 branches")
    arms.sort(key=lambda arm: arm[0])

    ring_nodes = []
    for i, (angle, s, end) in enumerate(arms):
        ring = f"{center}r{i}"
        nodes[ring] = [round(cx + RING_RADIUS * math.cos(angle)), round(cy + RING_RADIUS * math.sin(angle))]
        s[end] = ring
        ring_nodes.append(ring)

    ring_ids = []
    for i, a in enumerate(ring_nodes):
        b = ring_nodes[(i + 1) % len(ring_nodes)]
        ring_ids.append(f"{center}-ring{i}")
        segments.append({"id": ring_ids[-1], "a": a, "b": b, "name": "Rond-point", **RING,
                         "oneway": True, "roundabout": True})
    return {"x": cx, "y": cy, "r": RING_RADIUS, "segments": ring_ids}


def check_connected(nodes, segments):
    """Toutes les destinations doivent rester accessibles, travaux compris."""
    next_nodes = {n: [] for n in nodes}
    previous = {n: [] for n in nodes}
    for s in segments:
        if s.get("closed"):
            continue
        next_nodes[s["a"]].append(s["b"])
        previous[s["b"]].append(s["a"])
        if not s.get("oneway"):
            next_nodes[s["b"]].append(s["a"])
            previous[s["a"]].append(s["b"])

    for graph in (next_nodes, previous):
        start = next(iter(nodes))
        seen, stack = {start}, [start]
        while stack:
            for n in graph[stack.pop()]:
                if n not in seen:
                    seen.add(n)
                    stack.append(n)
        if seen != set(nodes):
            raise ValueError(f"ville non connexe : {sorted(set(nodes) - seen)}")


def main():
    rng = random.Random(11)
    nodes = build_nodes(rng)
    segments = build_segments(nodes)
    roundabouts = [add_roundabout(n, nodes, segments) for n in ROUNDABOUTS]
    check_connected(nodes, segments)

    city = {
        "bounds": {"w": 10000, "h": 10000},
        "units_per_kmh": 3.0,
        "car_spacing": 110,
        "river": {"width": 700, "points": [[5150, 10000], [4900, 8000], [5100, 6400],
                                           [4900, 4000], [5100, 2000], [4900, 0]]},
        "hill": {"x": 7600, "y": 4800, "rx": 750, "ry": 1250},
        "parks": [[700, 3300, 1400, 4700], [3400, 1500, 4100, 3100],
                  [6800, 1500, 7500, 3100], [8600, 7900, 9300, 9100]],
        "labels": [
            {"text": "Rive Ouest", "x": 1950, "y": 8500, "bold": True},
            {"text": "Rive Est", "x": 8000, "y": 8500, "bold": True},
            {"text": "Rue Principale", "x": 2900, "y": 6750},
            {"text": "Rocade Ouest", "x": 2000, "y": 9650},
            {"text": "Rocade Est", "x": 8000, "y": 9650},
            {"text": "Tunnel", "x": 7950, "y": 4800},
        ],
        "roundabouts": roundabouts,
        "nodes": nodes,
        "segments": segments,
    }
    OUTPUT.write_text(json.dumps(city, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{len(nodes)} noeuds, {len(segments)} tronceons, {len(roundabouts)} ronds-points")


if __name__ == "__main__":
    main()
