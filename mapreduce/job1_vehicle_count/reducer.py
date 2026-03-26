from __future__ import print_function
"""
Reducer — Comptage de véhicules par route.
Entrée  : road_id\t1  (trié par Hadoop)
Sortie  : road_id\tcount
"""

import sys

current_road  = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue   # Ligne malformée, ignorée

    road_id, count_str = parts
    try:
        count = int(count_str)
    except ValueError:
        continue   # Valeur non entière, ignorée

    if road_id == current_road:
        current_count += count
    else:
        if current_road is not None:
            print("{0}\t{1}".format(current_road, current_count))
        current_road  = road_id
        current_count = count
if current_road is not None:
    print("{0}\t{1}".format(current_road, current_count))
