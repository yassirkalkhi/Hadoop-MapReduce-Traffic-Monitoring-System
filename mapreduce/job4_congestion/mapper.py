#!/usr/bin/env python
from __future__ import print_function
"""
Mapper — Détection de congestion.
Émet vitesse et 1 par route pour permettre le calcul dans le reducer.
Entrée  : road_id,timestamp,vehicle_id,speed,lat,lon
Sortie  : road_id\tspeed,1
"""

import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("row_key"):
        continue

    parts = line.split(",")
    if len(parts) < 7:
        continue   # Ligne malformée, ignorée

    road_id = parts[1]
    speed   = parts[4]

    try:
        float(speed)   # Validation numérique
    except ValueError:
        continue       # Vitesse invalide, ignorée

    print("{0}\t{1},1".format(road_id, speed))
