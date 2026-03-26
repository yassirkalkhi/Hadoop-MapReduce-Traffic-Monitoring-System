# -*- coding: utf-8 -*-
"""
Extracteur HBase → HDFS.
Note : le CSV d'entrée est déjà présent dans HDFS après l'étape de génération.
Ce script effectue une vérification de disponibilité et peut forcer une
re-exportation depuis HBase si nécessaire.

Stratégie d'exportation HBase (si re-génération demandée) :
  1. HBase Shell scan → sortie texte capturée depuis 'docker exec -i hbase-master'
  2. Parse et reformatage en CSV
  3. 'docker cp' du CSV vers le conteneur namenode
  4. 'docker exec namenode hdfs dfs -put' vers /traffic/raw/data.csv
"""

import os
import sys
import subprocess
import tempfile
import time
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

CONTAINER_HBASE  = "hbase-master"
CONTAINER_NN     = "namenode"
HBASE_TABLE      = "traffic_data"
HDFS_RAW_PATH    = "/traffic/raw/data.csv"
CONTAINER_TMP    = "/tmp/stms_extract.csv"
CHUNK_SIZE       = 500    # Lignes de scan HBase par lot (LIMIT dans le shell)


def check_hdfs_input_exists() -> bool:
    """
    Vérifie si le fichier d'entrée HDFS (/traffic/raw/data.csv) existe déjà.
    Si oui, aucune re-extraction n'est nécessaire.

    :returns: True si le fichier HDFS existe.
    """
    r = subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-test", "-e", HDFS_RAW_PATH],
        capture_output=True, text=True, timeout=20,
    )
    return r.returncode == 0


def get_row_count() -> int:
    """
    Retourne le nombre approximatif de lignes dans la table HBase
    via un 'count' dans le shell HBase.
    Note : count HBase est lent pour de grands datasets, utilisé à titre indicatif.

    :returns: Nombre de lignes ou -1 si indéterminé.
    """
    shell_cmd = "count '{table}', INTERVAL => 50000\nexit\n".format(
        table=HBASE_TABLE)
    r = subprocess.run(
        ["docker", "exec", "-i", CONTAINER_HBASE, "hbase", "shell"],
        input=shell_cmd, capture_output=True, text=True, timeout=300,
    )
    match = re.search(r"(\d+)\s+row\(s\)", r.stdout)
    if match:
        return int(match.group(1))
    return -1


def export_hbase_to_hdfs() -> None:
    """
    Re-exporte la table HBase vers HDFS en streaming par lots.
    Utilise 'hbase shell' avec des scans paginés (STARTROW + LIMIT).
    Chaque lot est parsé et ajouté au fichier CSV temporaire local.

    :raises RuntimeError: Si le scan ou l'upload HDFS échoue.
    """
    log.info("Début de l'export HBase vers HDFS...")
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix="stms_export_")
    os.close(tmp_fd)

    try:
        start_row   = ""       # Clé de début pour la pagination
        total_rows  = 0
        total_skip  = 0
        t0          = time.time()
        is_first    = True

        with open(tmp_path, "w", encoding="utf-8") as out_f:
            out_f.write("road_id,timestamp,vehicle_id,speed,lat,lon\n")

            while True:
                if start_row:
                    scan_script = (
                        "scan '{table}', {{STARTROW => '{sr}', LIMIT => {lim}}}\nexit\n"
                    ).format(table=HBASE_TABLE, sr=start_row, lim=CHUNK_SIZE + 1)
                else:
                    scan_script = (
                        "scan '{table}', {{LIMIT => {lim}}}\nexit\n"
                    ).format(table=HBASE_TABLE, lim=CHUNK_SIZE + 1)

                r = subprocess.run(
                    ["docker", "exec", "-i", CONTAINER_HBASE, "hbase", "shell"],
                    input=scan_script,
                    capture_output=True, text=True, timeout=120,
                )

                if r.returncode != 0:
                    log.error("Scan HBase échoué : %s", r.stderr[:300])
                    raise RuntimeError("Scan HBase a échoué.")

                rows       = _parse_hbase_output(r.stdout)
                batch_size = len(rows)

                if batch_size == 0:
                    break   # Plus de données à lire

                write_rows = rows[1:] if not is_first and batch_size > 1 else rows
                for row_csv in write_rows:
                    out_f.write(row_csv + "\n")
                    total_rows += 1

                is_first = False

                last_key = _extract_last_key(r.stdout)
                if not last_key or batch_size <= CHUNK_SIZE:
                    break   # Dernier lot atteint
                start_row = last_key

                if total_rows % 100_000 < CHUNK_SIZE:
                    elapsed = time.time() - t0
                    log.info("  %s lignes exportées (%s lignes/s)...",
                             f"{total_rows:,}",
                             f"{total_rows / max(elapsed, 1):,.0f}")

        elapsed = time.time() - t0
        log.info("[OK] Export HBase terminé : %s lignes en %.1fs.",
                 f"{total_rows:,}", elapsed)

        _upload_csv_to_hdfs(tmp_path)

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _parse_hbase_output(shell_output: str) -> list:
    """
    Parse la sortie texte du shell HBase pour construire des lignes CSV.
    Format de la sortie HBase Shell :
        ROW                COLUMN+CELL
        road#ts#vehicle    column=cf:road_id, value=A1
        ...

    :param shell_output: Sortie brute du shell HBase.
    :returns:            Liste de lignes CSV 'road_id,timestamp,vehicle_id,speed,lat,lon'.
    """
    results = []
    current = {}

    for line in shell_output.splitlines():
        line = line.strip()
        if "\t" in line and "column=" in line:
            parts  = line.split("\t", 1)
            col_part = parts[1] if len(parts) > 1 else ""

            col_match = re.search(r"column=cf:(\w+).*?value=(.*)", col_part)
            if col_match:
                col_name  = col_match.group(1)
                col_value = col_match.group(2).strip()
                current[col_name] = col_value
        elif line.startswith("ROW") or line.startswith("---") or not line:
            if current:
                csv_line = _build_csv_line(current)
                if csv_line:
                    results.append(csv_line)
                current = {}

    if current:
        csv_line = _build_csv_line(current)
        if csv_line:
            results.append(csv_line)

    return results


def _build_csv_line(row: dict) -> str | None:
    """
    Construit une ligne CSV à partir d'un dictionnaire de colonnes HBase.

    :param row: Dictionnaire {nom_colonne: valeur}.
    :returns:   Ligne CSV ou None si données manquantes.
    """
    needed = ["road_id", "timestamp", "vehicle_id", "speed", "lat", "lon"]
    if not all(k in row for k in needed):
        return None
    return ",".join([row[k] for k in needed])


def _extract_last_key(shell_output: str) -> str:
    """
    Extrait la dernière clé de ligne du scan HBase pour la pagination.

    :param shell_output: Sortie brute du shell HBase.
    :returns:            Dernière clé de ligne ou chaîne vide.
    """
    last_key = ""
    for line in shell_output.splitlines():
        if "\t" in line and "column=" in line:
            last_key = line.split("\t")[0].strip()
    return last_key


def _upload_csv_to_hdfs(local_path: str) -> None:
    """
    Copie un fichier CSV local dans le namenode et l'upload vers HDFS.

    :param local_path: Chemin du fichier CSV local.
    :raises RuntimeError: Si la copie ou l'upload échoue.
    """
    r = subprocess.run(
        ["docker", "cp", local_path,
         "{0}:{1}".format(CONTAINER_NN, CONTAINER_TMP)],
        capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        raise RuntimeError("docker cp échoué : {0}".format(r.stderr))

    subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-rm", "-f", HDFS_RAW_PATH],
        capture_output=True, text=True, timeout=30,
    )

    subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-mkdir", "-p", "/traffic/raw"],
        capture_output=True, text=True, timeout=30,
    )

    r = subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-put", CONTAINER_TMP, HDFS_RAW_PATH],
        capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        raise RuntimeError("hdfs -put échoué : {0}".format(r.stderr))
    log.info("[OK] CSV uploadé vers HDFS : %s", HDFS_RAW_PATH)



def main() -> None:
    print("=" * 60)
    print("  STMS — Extracteur HBase → HDFS")
    print("=" * 60)

    if check_hdfs_input_exists():
        log.info(
            "[INFO] Le fichier HDFS '%s' existe déjà.\n"
            "       Re-exportation non nécessaire (utiliser --force pour forcer).",
            HDFS_RAW_PATH,
        )
        if "--force" not in sys.argv:
            log.info("Extraction ignorée. Utilisez : python extract_to_hdfs.py --force")
            return

    log.info("Lancement de l'extraction HBase → HDFS...")
    try:
        export_hbase_to_hdfs()
        print("")
        print("=" * 60)
        print("  [SUCCESS] Extraction terminée !")
        print("  HDFS : {0}".format(HDFS_RAW_PATH))
        print("=" * 60)
    except Exception as exc:
        log.error("ERREUR fatale : %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
