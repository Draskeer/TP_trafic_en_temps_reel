"""
Job Flink (PyFlink) — le coeur de la demo.

Il lit les positions GPS brutes (topic Kafka `probes` : car_id, x, y, event_time)
et publie la VITESSE MOYENNE par tronceon sur des FENETRES D'1 MINUTE en TEMPS DE
L'EVENEMENT. Deux etapes, chacune avec son etat :

  1. keyBy(voiture) -> NettoyageSonde
       rattache le point a un tronceon (map-matching), deduit la vitesse depuis le
       dernier point valide, et REJETTE les points aberrants (side output `invalid`).
  2. keyBy(tronceon) -> FenetreVitesse
       accumule par fenetre, et c'est le WATERMARK qui declenche la publication.
       Une sonde dont la fenetre est deja fermee part en side output `late`.

Watermark = max(event_time) - WATERMARK_DELAY : une ESTIMATION, donc faillible.

CHECKPOINT : Flink sauvegarde regulierement l'etat et ses positions de lecture Kafka.
Apres la perte d'un worker, il restaure le dernier checkpoint et relit Kafka depuis
la. Une fenetre peut alors etre publiee deux fois : le lecteur garde la derniere.

UN JOB PAR RIVE (SIDE = w ou e), chacun sur son worker : la panne d'un job n'arrete
pas l'autre. Les deux lisent tout le flux et suivent toutes les voitures, car une
voiture change de rive, mais chacun ne publie que les tronceons de sa rive.

La fenetre est ecrite a la main (etat + timer) : cela montre la mecanique
event-time / watermark / retard, et evite un bug de pickling de PyFlink 1.18 sur
`side_output_late_data()`.
"""

import json
import math
import os

from pyflink.common import Duration, Row
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SIDE = os.environ.get("SIDE", "w")
CITY_PATH = os.environ.get("CITY_PATH", "/opt/city.json")
WINDOW_MS = int(os.environ.get("WINDOW_S", "60")) * 1000
WATERMARK_DELAY_MS = int(os.environ.get("WATERMARK_DELAY_S", "30")) * 1000
MAX_SPEED_KMH = float(os.environ.get("MAX_SPEED_KMH", "130"))
MAX_ROAD_DIST = float(os.environ.get("MAX_ROAD_DIST", "200"))
CHECKPOINT_INTERVAL_MS = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "5000"))
RESTART_ATTEMPTS = 100
RESTART_DELAY_MS = 1000


class ProbeTimestampAssigner(TimestampAssigner):
    """On dit a Flink ou lire le temps de l'evenement dans chaque enregistrement."""

    def extract_timestamp(self, value, record_timestamp):
        return value[3]  # champ event_time (ms)


def parse_probe(raw: str) -> Row:
    d = json.loads(raw)
    return Row(str(d["car_id"]), float(d["x"]), float(d["y"]), int(d["event_time"]))


def window_start(ts):
    return (ts // WINDOW_MS) * WINDOW_MS


def nearest_segment(x, y, segments):
    """Map-matching simplifie : le tronceon le plus proche du point."""
    best_id, best_d = None, float("inf")
    for seg_id, ax, ay, bx, by in segments:
        dx, dy = bx - ax, by - ay
        t = max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy)))
        d = math.hypot(x - (ax + t * dx), y - (ay + t * dy))
        if d < best_d:
            best_id, best_d = seg_id, d
    return best_id, best_d


class NettoyageSonde(KeyedProcessFunction):
    """Par voiture : point GPS -> (tronceon, vitesse), ou rejet s'il est aberrant."""

    def __init__(self, side, invalid_tag, segments, width, height, units_per_kmh):
        self.side = side
        self.invalid_tag = invalid_tag
        self.segments = segments
        self.width = width
        self.height = height
        self.units_per_kmh = units_per_kmh

    def open(self, ctx):
        # Etat : dernier point VALIDE de la voiture (x, y, ts, tronceon).
        self.last = ctx.get_state(
            ValueStateDescriptor("last", Types.PICKLED_BYTE_ARRAY())
        )

    def _rejet(self, value, segment, reason, speed):
        return self.invalid_tag, json.dumps(
            {
                "segment": segment,
                "car_id": value[0],
                "x": value[1],
                "y": value[2],
                "event_time": value[3],
                "window_start": window_start(value[3]),
                "reason": reason,
                "speed": speed,
            }
        )

    def process_element(self, value, ctx):
        x, y, ts = value[1], value[2], value[3]
        last = self.last.value()
        seg, road_dist = nearest_segment(x, y, self.segments)

        speed = None
        if last is not None and ts > last[2]:
            dist = math.hypot(x - last[0], y - last[1])
            speed = round(dist / ((ts - last[2]) / 1000) / self.units_per_kmh, 1)

        # Un point rejete est compte sur le dernier tronceon SUR de la voiture, et
        # ne remplace jamais le dernier point valide (sinon le point suivant, bon,
        # paraitrait aberrant a son tour).
        known_seg = last[3] if last is not None else seg
        reason = None
        if not (0 <= x <= self.width and 0 <= y <= self.height):
            reason = "hors_carte"
        elif road_dist > MAX_ROAD_DIST:
            reason = "hors_route"
        elif last is None:
            self.last.update((x, y, ts, seg))
            return
        elif speed is None:
            reason = "horodatage"
        elif speed > MAX_SPEED_KMH:
            reason = "vitesse_impossible"

        # L'etat suit toutes les voitures, mais on ne publie que pour sa rive.
        if reason is not None:
            if known_seg[0] == self.side:
                yield self._rejet(value, known_seg, reason, speed)
            return
        self.last.update((x, y, ts, seg))
        if seg[0] == self.side:
            yield Row(seg, speed, ts, value[0])


class FenetreVitesse(KeyedProcessFunction):
    """Fenetres fixes d'1 min par tronceon, pilotees par le watermark."""

    def __init__(self, late_tag):
        self.late_tag = late_tag

    def open(self, ctx):
        # Etat : debut de fenetre -> (somme des vitesses, nb sondes, nb en desordre).
        self.agg = ctx.get_map_state(
            MapStateDescriptor("agg", Types.LONG(), Types.PICKLED_BYTE_ARRAY())
        )
        # Les checkpoints ne sauvegardent pas le watermark : apres une reprise il
        # repart de zero. On retient donc jusqu'ou les fenetres sont deja fermees.
        self.closed_until = ctx.get_state(
            ValueStateDescriptor("closed_until", Types.LONG())
        )

    def process_element(self, value, ctx):
        ts = value[2]
        wm = ctx.timer_service().current_watermark()
        w_start = window_start(ts)
        w_end = w_start + WINDOW_MS

        # Si la fenetre est DEJA FERMEE par le watermark -> retardataire.
        if w_end <= max(wm, self.closed_until.value() or 0):
            yield self.late_tag, json.dumps(
                {
                    "segment": value[0],
                    "speed": value[1],
                    "event_time": ts,
                    "car_id": value[3],
                    "window_start": w_start,
                    "watermark": wm,
                }
            )
            return

        total, n, rescued = (
            self.agg.get(w_start) if self.agg.contains(w_start) else (0.0, 0, 0)
        )
        # ts < watermark : en retard, mais la fenetre est encore ouverte -> sauvee.
        self.agg.put(w_start, (total + value[1], n + 1, rescued + (ts < wm)))
        # Le timer se declenchera quand le watermark depassera w_end.
        ctx.timer_service().register_event_time_timer(w_end)

    def on_timer(self, timestamp, ctx):
        w_start = timestamp - WINDOW_MS
        if not self.agg.contains(w_start):
            return
        total, n, rescued = self.agg.get(w_start)
        self.agg.remove(w_start)
        self.closed_until.update(max(timestamp, self.closed_until.value() or 0))
        yield json.dumps(
            {
                "segment": ctx.get_current_key(),
                "window_start": w_start,
                "window_end": timestamp,
                "avg_speed": round(total / n, 1),
                "count": n,
                "out_of_order": rescued,
                "watermark": ctx.timer_service().current_watermark(),
            }
        )


def load_segments():
    with open(CITY_PATH, encoding="utf-8") as f:
        city = json.load(f)
    nodes = city["nodes"]
    segments = [
        (s["id"], *nodes[s["a"]], *nodes[s["b"]]) for s in city["segments"]
    ]
    return segments, city["bounds"]["w"], city["bounds"]["h"], city["units_per_kmh"]


def make_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)  # rien de perdu a la reprise
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def main():
    # IMPORTANT : aucun objet Types/OutputTag au niveau module, tout se cree ici.
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # demo deterministe
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(RESTART_ATTEMPTS, RESTART_DELAY_MS)
    )

    raw_type = Types.ROW_NAMED(
        ["car_id", "x", "y", "event_time"],
        [Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()],
    )
    probe_type = Types.ROW_NAMED(
        ["segment", "speed", "event_time", "car_id"],
        [Types.STRING(), Types.DOUBLE(), Types.LONG(), Types.STRING()],
    )
    # Deux "robinets" lateraux : donnees aberrantes et retardataires.
    invalid_tag = OutputTag("rejetees", Types.STRING())
    late_tag = OutputTag("retardataires", Types.STRING())

    # earliest : le journal Kafka permet de rejouer tout le flux depuis le debut.
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("probes")
        .set_group_id(f"flink-trafic-{SIDE}")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "probes-source")

    # Watermark : max(event_time) - WATERMARK_DELAY.
    wms = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_millis(WATERMARK_DELAY_MS)
    ).with_timestamp_assigner(ProbeTimestampAssigner())

    cleaned = (
        raw.map(parse_probe, output_type=raw_type)
        .assign_timestamps_and_watermarks(wms)
        .key_by(lambda r: r[0], key_type=Types.STRING())  # une voiture -> un worker
        .process(
            NettoyageSonde(SIDE, invalid_tag, *load_segments()), output_type=probe_type
        )
    )

    result = cleaned.key_by(
        lambda r: r[0], key_type=Types.STRING()  # un tronceon -> un worker
    ).process(FenetreVitesse(late_tag), output_type=Types.STRING())

    result.sink_to(make_sink("speeds"))
    result.get_side_output(late_tag).sink_to(make_sink("late"))
    cleaned.get_side_output(invalid_tag).sink_to(make_sink("invalid"))

    env.execute(f"trafic-{SIDE}")


if __name__ == "__main__":
    main()
