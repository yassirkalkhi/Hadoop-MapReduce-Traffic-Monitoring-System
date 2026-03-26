#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Mapper — Comptage de véhicules par route.
Entrée  : road_id,timestamp,vehicle_id,speed,lat,lon
Sortie  : road_id\t1
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
    print("{0}\t1".format(road_id))
