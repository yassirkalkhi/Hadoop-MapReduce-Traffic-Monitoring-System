#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Reducer — Détection de congestion.
Logique : avg_speed < 20 ET vehicle_count > 500 → route congestionnée.
Entrée  : road_id\tspeed,1  (trié par Hadoop)
Sortie  : road_id\tavg_speed\tvehicle_count\tcongested
"""

import sys

SPEED_THRESHOLD = 20.0    # km/h : en dessous = lent
COUNT_THRESHOLD = 500     # véhicules : au-dessus = dense

current_road  = None
total_speed   = 0.0
total_count   = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue   # Ligne malformée, ignorée

    road_id, value = parts
    try:
        speed_str, count_str = value.split(",")
        speed = float(speed_str)
        count = int(count_str)
    except (ValueError, AttributeError):
        continue   # Format invalide, ignoré

    if road_id == current_road:
        total_speed += speed
        total_count += count
    else:
        if current_road is not None and total_count > 0:
            avg_speed  = round(total_speed / total_count, 2)
            congested  = (avg_speed < SPEED_THRESHOLD) and (total_count > COUNT_THRESHOLD)
            print("{0}\t{1}\t{2}\t{3}".format(current_road, avg_speed, total_count, int(congested)))
        current_road = road_id
        total_speed  = speed
        total_count  = count

if current_road is not None and total_count > 0:
    avg_speed = round(total_speed / total_count, 2)
    congested = (avg_speed < SPEED_THRESHOLD) and (total_count > COUNT_THRESHOLD)
    print("{0}\t{1}\t{2}\t{3}".format(current_road, avg_speed, total_count, int(congested)))
