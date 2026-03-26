"""
Routes — Lecture et exposition des résultats MapReduce.
GET /data/<job>  : retourne les données du travail en JSON.
"""

import logging
from flask import Blueprint, jsonify
from utils.hdfs_reader import read_hdfs_output, check_hdfs_output_exists

log     = logging.getLogger(__name__)
data_bp = Blueprint("data", __name__)


def parse_vehicle_count(lines: list[str]) -> list[dict]:
    """Analyse les résultats du travail 1 : comptage de véhicules par route."""
    results = []
    for line in lines:
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        try:
            results.append({
                "road_id": parts[0],
                "value":   int(parts[1]),
            })
        except ValueError:
            log.warning(f"Ligne malformée ignorée (vehicle_count) : {line!r}")
    # Tri décroissant par valeur
    return sorted(results, key=lambda x: x["value"], reverse=True)


def parse_avg_speed(lines: list[str]) -> list[dict]:
    """Analyse les résultats du travail 2 : vitesse moyenne par route."""
    results = []
    for line in lines:
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        try:
            results.append({
                "road_id": parts[0],
                "value":   float(parts[1]),
            })
        except ValueError:
            log.warning(f"Ligne malformée ignorée (avg_speed) : {line!r}")
    # Tri croissant : les routes les plus lentes en premier
    return sorted(results, key=lambda x: x["value"])


def parse_peak_hours(lines: list[str]) -> list[dict]:
    """Analyse les résultats du travail 3 : trafic par heure."""
    results = []
    for line in lines:
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        try:
            results.append({
                "hour":  int(parts[0]),
                "value": int(parts[1]),
            })
        except ValueError:
            log.warning(f"Ligne malformée ignorée (peak_hours) : {line!r}")
    # Tri par heure pour l'affichage chronologique
    return sorted(results, key=lambda x: x["hour"])


def parse_congestion(lines: list[str]) -> list[dict]:
    """Analyse les résultats du travail 4 : détection de congestion."""
    results = []
    for line in lines:
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        try:
            results.append({
                "road_id":       parts[0],
                "avg_speed":     float(parts[1]),
                "vehicle_count": int(parts[2]),
                "congested":     bool(int(parts[3])),
            })
        except ValueError:
            log.warning(f"Ligne malformée ignorée (congestion) : {line!r}")
    # Routes congestionnées en premier, puis par vitesse croissante
    return sorted(results, key=lambda x: (not x["congested"], x["avg_speed"]))


# Correspondance nom de travail → parseur
PARSERS = {
    "vehicle_count": parse_vehicle_count,
    "avg_speed":     parse_avg_speed,
    "peak_hours":    parse_peak_hours,
    "congestion":    parse_congestion,
}


@data_bp.route("/<job>", methods=["GET"])
def get_data(job: str):
    """
    Lit et retourne les résultats MapReduce pour le travail demandé.
    """
    if job not in PARSERS:
        return jsonify({
            "success": False,
            "error":   f"Travail inconnu : '{job}'.",
        }), 400

    if not check_hdfs_output_exists(job):
        return jsonify({
            "success": False,
            "error":   f"Aucun résultat trouvé pour '{job}'. "
                       "Veuillez d'abord exécuter le travail MapReduce.",
        }), 404

    try:
        lines   = read_hdfs_output(job)
        parser  = PARSERS[job]
        results = parser(lines)

        log.info(f"Données '{job}' retournées : {len(results)} entrées.")
        return jsonify({
            "success": True,
            "job":     job,
            "count":   len(results),
            "data":    results,
        })

    except RuntimeError as exc:
        return jsonify({
            "success": False,
            "error":   str(exc),
        }), 500
    except Exception as exc:
        log.error(f"Erreur inattendue lors de la lecture de '{job}' : {exc}")
        return jsonify({
            "success": False,
            "error":   "Erreur interne du serveur.",
        }), 500
