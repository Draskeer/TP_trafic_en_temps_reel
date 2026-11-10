# Comment Google Maps connaît-il les bouchons en temps réel ?

Démo : vitesse moyenne par tronçon sur des fenêtres d'une minute, en temps de
l'évènement, avec des données en retard et des données fausses (le tunnel).

Flink illustre le principe : Google ne publie pas le détail de son pipeline.

## Lancer

    docker compose up -d --build

- Interface : http://localhost:8080
- Flink : http://localhost:8081

Le premier build télécharge PyFlink (compter ~2 Go d'espace disque).

## Repartir de zéro

Le temps de la simulation repart à 00:00, il faut donc aussi vider Kafka et l'état Flink :

    docker compose down -v && docker compose up -d --build

## Ce qui circule

    simulateur -> probes (x, y, t) -> Flink -> speeds   (moyennes publiées)
                                           -> late     (fenêtre déjà fermée)
                                           -> invalid  (points aberrants)

Le topic `positions` sert uniquement à dessiner les voitures. Les boutons de
l'interface passent par le topic `control`, et le simulateur publie les actions
appliquées sur `scenario`.

## Piloter la ville

Dans l'interface : le curseur change le nombre de voitures, et chaque tronçon
peut être fermé ou rouvert. La barre d'espace met la simulation en pause. Chaque rive a son job Flink et son
worker : les boutons « Panne worker » tuent celui d'une rive, le relancent 15 s plus
tard, et il reprend depuis son dernier checkpoint pendant que l'autre rive continue
(le serveur web a accès au socket Docker pour ça). Les ponts sont gérés par le
worker Ouest. Le bouton « Relancer à zéro » remet toute la démo à zéro. Les bouchons ne sont pas scriptés (`simulator/traffic.py`) :
ils viennent du nombre de voitures et du débit limité des ponts (`exit_headway_s`
dans `city.json`).

## Le réglage à montrer

Dans `.env`, changer `WATERMARK_DELAY_S` puis repartir de zéro : un délai court
publie plus vite mais écarte plus de retards, un délai long fait l'inverse.
