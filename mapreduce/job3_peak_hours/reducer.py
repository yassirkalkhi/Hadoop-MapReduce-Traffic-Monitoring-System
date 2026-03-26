#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
"""
Reducer — Trafic par heure (heures de pointe).
Entrée  : hour\t1  (trié par Hadoop)
Sortie  : hour\tcount
"""

import sys

current_hour  = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue   # Ligne malformée, ignorée

    hour, count_str = parts
    try:
        count = int(count_str)
    except ValueError:
        continue   # Valeur non entière, ignorée

    if hour == current_hour:
        current_count += count
    else:
        if current_hour is not None:
            print("{0}\t{1}".format(current_hour, current_count))
        current_hour  = hour
        current_count = count

if current_hour is not None:
    print("{0}\t{1}".format(current_hour, current_count))
