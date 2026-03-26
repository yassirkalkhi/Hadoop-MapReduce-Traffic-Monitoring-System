# -*- coding: utf-8 -*-

"""
Routes — Déclenchement des travaux MapReduce.
POST /run/<job>  : copie les scripts dans le conteneur namenode et exécute
                   le travail via 'docker exec namenode hadoop streaming'.
"""

import subprocess
import os
import logging
from flask import Blueprint, jsonify
from utils.docker_exec import (
    docker_exec, hdfs_exec, hdfs_rm,
    CONTAINER_NAMENODE, STREAMING_JAR
)

log     = logging.getLogger(__name__)
jobs_bp = Blueprint("jobs", __name__)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
MR_DIR       = os.path.join(PROJECT_ROOT, "mapreduce")

HDFS_INPUT  = "/traffic/raw/data.csv"
HDFS_OUTPUT = "/traffic/output"

JOBS = {
    "vehicle_count": {
        "mapper":  os.path.join(MR_DIR, "job1_vehicle_count", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job1_vehicle_count", "reducer.py"),
        "output":  f"{HDFS_OUTPUT}/vehicle_count",
        "label":   "Comptage de véhicules par route",
    },
    "avg_speed": {
        "mapper":  os.path.join(MR_DIR, "job2_avg_speed", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job2_avg_speed", "reducer.py"),
        "output":  f"{HDFS_OUTPUT}/avg_speed",
        "label":   "Vitesse moyenne par route",
    },
    "peak_hours": {
        "mapper":  os.path.join(MR_DIR, "job3_peak_hours", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job3_peak_hours", "reducer.py"),
        "output":  f"{HDFS_OUTPUT}/peak_hours",
        "label":   "Trafic par heure (heures de pointe)",
    },
    "congestion": {
        "mapper":  os.path.join(MR_DIR, "job4_congestion", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job4_congestion", "reducer.py"),
        "output":  f"{HDFS_OUTPUT}/congestion",
        "label":   "Détection de congestion",
    },
}

CONTAINER_MR_DIR = "/tmp/stms_mapreduce"


def _copy_scripts_to_container(job_key: str) -> tuple:
    """
    Copie les scripts mapper/reducer dans le conteneur namenode via 'docker cp'.
    Retourne les chemins des scripts dans le conteneur sous forme de tuple.

    :param job_key: Clé du travail (ex : 'vehicle_count').
    :returns:       (chemin_mapper, chemin_reducer) dans le conteneur.
    :raises RuntimeError: Si la copie échoue.
    """
    job      = JOBS[job_key]
    dest_dir = f"{CONTAINER_MR_DIR}/{job_key}"

    mk = subprocess.run(
        ["docker", "exec", CONTAINER_NAMENODE, "mkdir", "-p", dest_dir],
        capture_output=True, text=True, timeout=15,
    )
    if mk.returncode != 0:
        raise RuntimeError(f"Impossible de créer {dest_dir} : {mk.stderr}")

    for label, local_path in [("mapper", job["mapper"]), ("reducer", job["reducer"])]:
        r = subprocess.run(
            ["docker", "cp", local_path, f"{CONTAINER_NAMENODE}:{dest_dir}/"],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode != 0:
            raise RuntimeError(
                f"Impossible de copier le {label} ({local_path}) : {r.stderr}"
            )
        log.info("Script copié → conteneur : %s → %s/", local_path, dest_dir)

    mapper_name  = os.path.basename(job["mapper"])
    reducer_name = os.path.basename(job["reducer"])
    return (
        f"{dest_dir}/{mapper_name}",
        f"{dest_dir}/{reducer_name}",
    )


def _run_single_job(job_key: str) -> dict:
    """
    Lance un seul travail MapReduce dans le conteneur namenode.

    :param job_key: Clé du travail.
    :returns:       Dictionnaire {success, output, error}.
    """
    job = JOBS[job_key]
    log.info("Lancement : %s", job["label"])

    hdfs_rm(job["output"])

    mapper_path, reducer_path = _copy_scripts_to_container(job_key)

    docker_exec(CONTAINER_NAMENODE,
                ["chmod", "+x", mapper_path, reducer_path], timeout=10)

    streaming_cmd = [
        "hadoop", "jar", STREAMING_JAR,
        "-files", f"{mapper_path},{reducer_path}",
        "-mapper",  f"python {os.path.basename(mapper_path)}",
        "-reducer", f"python {os.path.basename(reducer_path)}",
        "-input",   HDFS_INPUT,
        "-output",  job["output"],
    ]

    result = docker_exec(CONTAINER_NAMENODE, streaming_cmd, timeout=600)

    success = result.returncode == 0
    log.info(
        "Travail '%s' %s (code : %d).",
        job_key,
        "réussi" if success else "échoué",
        result.returncode,
    )
    return {
        "success": success,
        "output":  (result.stdout + result.stderr)[-3000:],
        "error":   result.stderr[-1000:] if not success else None,
    }


@jobs_bp.route("/<job>", methods=["POST"])
def run_job(job: str):
    """
    Déclenche un ou tous les travaux MapReduce.
    Valeurs valides : 'vehicle_count', 'avg_speed', 'peak_hours', 'congestion', 'all'.
    """
    valid = list(JOBS.keys()) + ["all"]
    if job not in valid:
        return jsonify({
            "success": False,
            "error":   f"Travail inconnu : '{job}'. Valides : {valid}",
        }), 400

    jobs_to_run = list(JOBS.keys()) if job == "all" else [job]

    results   = {}
    all_ok    = True
    combined  = []

    for j in jobs_to_run:
        try:
            r = _run_single_job(j)
            results[j] = r
            combined.append(f"[{j}] {'OK' if r['success'] else 'ERREUR'}")
            if not r["success"]:
                all_ok = False
        except Exception as exc:
            log.error("Erreur inattendue pour '%s' : %s", j, exc)
            results[j] = {"success": False, "error": str(exc)}
            combined.append(f"[{j}] ERREUR : {exc}")
            all_ok = False

    return jsonify({
        "success": all_ok,
        "job":     job,
        "summary": "\n".join(combined),
        "results": results,
    }), 200 if all_ok else 500
