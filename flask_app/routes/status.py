"""
Routes — Vérification du statut des résultats disponibles.
GET /status  : retourne la disponibilité de chaque travail.
"""

import logging
from flask import Blueprint, jsonify
from utils.hdfs_reader import check_hdfs_output_exists, JOB_PATHS

log       = logging.getLogger(__name__)
status_bp = Blueprint("status", __name__)


@status_bp.route("", methods=["GET"])
def get_status():
    """
    Vérifie l'état de disponibilité de chaque résultat MapReduce dans HDFS.
    Retourne un dictionnaire {job_name: bool} pour chaque travail connu.
    """
    availability = {}
    for job_name in JOB_PATHS:
        availability[job_name] = check_hdfs_output_exists(job_name)

    all_ready = all(availability.values())
    log.info(f"Vérification du statut : {availability}")

    return jsonify({
        "success":     True,
        "all_ready":   all_ready,
        "jobs":        availability,
    })
