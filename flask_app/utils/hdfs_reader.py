# -*- coding: utf-8 -*-

"""
Utilitaires partagés — lecture des résultats MapReduce depuis HDFS.
Toutes les opérations HDFS passent par 'docker exec namenode'.
"""

import logging
import sys
import os

log = logging.getLogger(__name__)

LOG_FILE = os.path.join(os.path.expanduser("~"), "stms_flask.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

from utils.docker_exec import hdfs_cat, hdfs_path_exists

HDFS_OUTPUT_BASE = "/traffic/output"

JOB_PATHS = {
    "vehicle_count": f"{HDFS_OUTPUT_BASE}/vehicle_count",
    "avg_speed":     f"{HDFS_OUTPUT_BASE}/avg_speed",
    "peak_hours":    f"{HDFS_OUTPUT_BASE}/peak_hours",
    "congestion":    f"{HDFS_OUTPUT_BASE}/congestion",
}


def read_hdfs_output(job_name: str) -> list:
    """
    Lit la sortie MapReduce depuis HDFS via 'docker exec namenode hdfs dfs -cat'.
    Retourne les lignes brutes (chaînes de caractères).

    :param job_name: Nom du travail (ex : 'vehicle_count').
    :returns:        Liste de lignes non vides.
    :raises ValueError:    Si le nom de travail est inconnu.
    :raises RuntimeError:  Si la lecture HDFS échoue.
    """
    if job_name not in JOB_PATHS:
        raise ValueError(f"Nom de travail inconnu : '{job_name}'")

    hdfs_glob = f"{JOB_PATHS[job_name]}/part-*"
    log.info("Lecture HDFS : %s", hdfs_glob)

    lines = hdfs_cat(hdfs_glob, timeout=60)
    log.info("%d lignes lues depuis HDFS pour '%s'.", len(lines), job_name)
    return lines


def check_hdfs_output_exists(job_name: str) -> bool:
    """
    Vérifie si le répertoire de sortie HDFS du travail existe
    via 'docker exec namenode hdfs dfs -test -e'.

    :param job_name: Nom du travail.
    :returns:        True si le répertoire existe, False sinon.
    """
    if job_name not in JOB_PATHS:
        return False
    path   = JOB_PATHS[job_name]
    exists = hdfs_path_exists(path)
    log.debug("HDFS existe (%s) : %s", path, exists)
    return exists
