#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Reducer — Vitesse moyenne par route.
Entrée  : road_id\tspeed  (trié par Hadoop)
Sortie  : road_id\tavg_speed
"""

import sys

current_road  = None
total_speed   = 0.0
count         = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue   # Ligne malformée, ignorée

    road_id, speed_str = parts
    try:
        speed = float(speed_str)
    except ValueError:
        continue   # Valeur non numérique, ignorée

    if road_id == current_road:
        total_speed += speed
        count       += 1
    else:
        # Émettre la moyenne pour la route précédente
        if current_road is not None and count > 0:
            avg = round(total_speed / count, 2)
            print("{0}\t{1}".format(current_road, avg))
        current_road = road_id
        total_speed  = speed
        count        = 1

# Émettre le dernier groupe
if current_road is not None and count > 0:
    avg = round(total_speed / count, 2)
    print("{0}\t{1}".format(current_road, avg))
