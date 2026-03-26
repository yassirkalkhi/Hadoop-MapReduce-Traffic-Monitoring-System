#!/usr/bin/env python
"""
Lance les travaux MapReduce via 'docker exec namenode'.
Remplace run_jobs.sh pour compatibilité Windows / Linux.

Usage:
    python run_jobs.py all
    python run_jobs.py job1
    python run_jobs.py job2 job3
"""

from __future__ import print_function
import sys
import os
import subprocess
import time

# ─── Configuration ───────────────────────────────────────────────────────────
CONTAINER_NN  = "namenode"
CONTAINER_MR  = "/tmp/stms_mapreduce"
HDFS_INPUT    = "/traffic/raw/data.csv"
HDFS_OUTPUT   = "/traffic/output"

STREAMING_JAR = (
    "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
)

MR_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapreduce")

JOBS = {
    "job1": {
        "key":     "vehicle_count",
        "label":   "Comptage de véhicules par route",
        "mapper":  os.path.join(MR_DIR, "job1_vehicle_count", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job1_vehicle_count", "reducer.py"),
        "output":  HDFS_OUTPUT + "/vehicle_count",
    },
    "job2": {
        "key":     "avg_speed",
        "label":   "Vitesse moyenne par route",
        "mapper":  os.path.join(MR_DIR, "job2_avg_speed", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job2_avg_speed", "reducer.py"),
        "output":  HDFS_OUTPUT + "/avg_speed",
    },
    "job3": {
        "key":     "peak_hours",
        "label":   "Trafic par heure (heures de pointe)",
        "mapper":  os.path.join(MR_DIR, "job3_peak_hours", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job3_peak_hours", "reducer.py"),
        "output":  HDFS_OUTPUT + "/peak_hours",
    },
    "job4": {
        "key":     "congestion",
        "label":   "Détection de congestion",
        "mapper":  os.path.join(MR_DIR, "job4_congestion", "mapper.py"),
        "reducer": os.path.join(MR_DIR, "job4_congestion", "reducer.py"),
        "output":  HDFS_OUTPUT + "/congestion",
    },
}
# ─────────────────────────────────────────────────────────────────────────────


def log(msg):
    """Affiche un message horodaté."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print("[{0}] {1}".format(ts, msg))
    sys.stdout.flush()


def run_cmd(cmd, timeout=60):
    """
    Exécute une commande et retourne (returncode, stdout+stderr).

    :param cmd:     Liste de tokens de la commande.
    :param timeout: Délai maximal en secondes.
    :returns:       Tuple (returncode, output_str).
    """
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.returncode, (result.stdout + result.stderr)


def copy_script(local_path, job_key):
    """
    Copie un script Python dans le conteneur namenode via 'docker cp'.

    :param local_path: Chemin local du script.
    :param job_key:    Clé du travail (utilisée pour le sous-répertoire).
    :raises SystemExit: Si la copie échoue.
    """
    dest_dir = "{0}/{1}".format(CONTAINER_MR, job_key)

    code, out = run_cmd(
        ["docker", "exec", CONTAINER_NN, "mkdir", "-p", dest_dir],
        timeout=15,
    )
    if code != 0:
        log("[ERREUR] Impossible de créer {0} : {1}".format(dest_dir, out[:200]))
        sys.exit(1)

    code, out = run_cmd(
        ["docker", "cp", local_path,
         "{0}:{1}/".format(CONTAINER_NN, dest_dir)],
        timeout=15,
    )
    if code != 0:
        log("[ERREUR] docker cp échoué pour {0} : {1}".format(local_path, out[:200]))
        sys.exit(1)

    return "{0}/{1}".format(dest_dir, os.path.basename(local_path))


def run_job(job_id):
    """
    Exécute un seul travail MapReduce.

    :param job_id: Identifiant du travail ('job1'..'job4').
    :returns:      True si succès, False sinon.
    """
    if job_id not in JOBS:
        log("[ERREUR] Travail inconnu : {0}".format(job_id))
        return False

    job = JOBS[job_id]
    print("")
    print("─" * 60)
    log("Travail : {0}".format(job["label"]))
    print("─" * 60)

    log("Suppression de la sortie précédente : {0}".format(job["output"]))
    run_cmd(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-rm", "-r", "-f", job["output"]],
        timeout=30,
    )

    # ── Copie des scripts dans le conteneur ──────────────────────────────
    log("Copie des scripts dans le conteneur...")
    mapper_path  = copy_script(job["mapper"],  job["key"])
    reducer_path = copy_script(job["reducer"], job["key"])

    # Rendre les scripts exécutables
    run_cmd(
        ["docker", "exec", CONTAINER_NN, "chmod", "+x",
         mapper_path, reducer_path],
        timeout=10,
    )

    # ── Lancement de Hadoop Streaming ────────────────────────────────────
    log("Lancement de Hadoop Streaming...")
    t0 = time.time()

    streaming_cmd = [
        "docker", "exec", CONTAINER_NN,
        "hadoop", "jar", STREAMING_JAR,
        "-files", "{0},{1}".format(mapper_path, reducer_path),
        "-mapper",  "python {0}".format(os.path.basename(mapper_path)),
        "-reducer", "python {0}".format(os.path.basename(reducer_path)),
        "-input",   HDFS_INPUT,
        "-output",  job["output"],
    ]

    code, output = run_cmd(streaming_cmd, timeout=600)
    elapsed = time.time() - t0

    if code == 0:
        log("[OK] {0} terminé en {1:.1f}s → {2}".format(
            job["label"], elapsed, job["output"]))
        return True
    else:
        log("[ERREUR] {0} a échoué (code {1}) après {2:.1f}s".format(
            job["label"], code, elapsed))
        print(output[-500:])
        return False


def main():
    print("=" * 60)
    print("  STMS — Lancement des travaux MapReduce")
    print("  " + time.strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 60)

    # Sélection des travaux depuis les arguments de la ligne de commande
    args = sys.argv[1:]
    if not args:
        print("Usage : python run_jobs.py all | job1 | job2 | job3 | job4")
        sys.exit(1)

    if "all" in args:
        selected = list(JOBS.keys())
    else:
        selected = [a for a in args if a in JOBS]
        invalid  = [a for a in args if a not in JOBS and a != "all"]
        if invalid:
            log("[ERREUR] Travaux inconnus : {0}".format(", ".join(invalid)))
            sys.exit(1)

    # Exécution des travaux sélectionnés
    results = {}
    for job_id in selected:
        results[job_id] = run_job(job_id)

    # Résumé final
    print("")
    print("=" * 60)
    print("  RÉSUMÉ")
    print("=" * 60)
    all_ok = True
    for job_id, success in results.items():
        status = "[OK]     " if success else "[ERREUR]"
        print("  {0} {1} — {2}".format(
            status, job_id, JOBS[job_id]["label"]))
        if not success:
            all_ok = False
    print("=" * 60)
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
