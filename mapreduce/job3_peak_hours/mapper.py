#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Mapper — Trafic par heure (heures de pointe).
Entrée  : road_id,timestamp,vehicle_id,speed,lat,lon
Sortie  : hour\t1
"""

import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("row_key"):
        continue

    parts = line.split(",")
    if len(parts) < 7:
        continue   # Ligne malformée, ignorée

    timestamp = parts[2]   # Format : YYYY-MM-DD HH:MM:SS
    try:
        hour = timestamp.split(" ")[1].split(":")[0]
        int(hour)           # Validation : doit être numérique
    except (IndexError, ValueError):
        continue            # Format d'horodatage invalide, ignoré

    print("{0}\t1".format(hour))
