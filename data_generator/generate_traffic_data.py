"""
Générateur de données de trafic (exécution unique).
Stratégie :
  1. Génère le dataset CSV localement.
  2. Copie le CSV dans le conteneur namenode via 'docker cp'.
  3. Upload le CSV vers HDFS via 'docker exec namenode hdfs dfs -put'.
  4. Crée la table HBase via 'docker exec hbase-master hbase shell'.
  5. Charge les données HBase via 'docker exec hbase-master hbase ImportTsv'.
"""

import os
import sys
import csv
import time
import random
import tempfile
import subprocess
import logging
from datetime import datetime, timedelta

# ─── Chemin du module docker_exec (ajout du répertoire flask_app au path) ─────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flask_app"))

# ─── Journalisation ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────
TOTAL_ROWS       = 500_000
CONTAINER_HBASE  = "hbase-master"
CONTAINER_NN     = "namenode"
HBASE_TABLE      = "traffic_data"
HDFS_RAW_PATH    = "/traffic/raw/data.csv"
CONTAINER_TMP    = "/tmp/stms_data.csv"   # Chemin temporaire dans le conteneur
# ─────────────────────────────────────────────────────────────────────────────

# Identifiants de routes disponibles
ROAD_IDS = [
    "A1", "A2", "A3", "A4", "A5",
    "B1", "B2", "B3", "B4", "B5",
    "C1", "C2", "C3", "C4", "C5",
    "D1", "D2", "D3",
]

# Centre géographique de référence (Paris, France)
BASE_LAT = 48.8566
BASE_LON = 2.3522


def reverse_timestamp(ts: float) -> str:
    """
    Inverse l'horodatage pour obtenir un tri HBase du plus récent au plus ancien.
    Permet d'accéder rapidement aux données les plus récentes par scan.

    :param ts: Horodatage epoch (float).
    :returns:  Chaîne de l'horodatage inversé (9_999_999_999_999 − ts_ms).
    """
    max_ts = 9_999_999_999_999
    return str(max_ts - int(ts * 1000))


def make_row_key(road_id: str, ts: float, vehicle_id: str) -> str:
    """
    Construit la clé de ligne HBase au format :
    <road_id>#<reverse_timestamp>#<vehicle_id>

    :param road_id:    Identifiant de route.
    :param ts:         Horodatage epoch.
    :param vehicle_id: Identifiant du véhicule.
    :returns:          Clé de ligne HBase.
    """
    return "{road}#{rev}#{vid}".format(
        road=road_id,
        rev=reverse_timestamp(ts),
        vid=vehicle_id,
    )


def random_timestamp(start: datetime, end: datetime) -> datetime:
    """
    Génère un horodatage aléatoire dans l'intervalle [start, end].

    :param start: Date de début.
    :param end:   Date de fin.
    :returns:     Datetime aléatoire.
    """
    delta   = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


# ─── Étape 1 : Génération du CSV local ───────────────────────────────────────

def generate_csv(output_path: str) -> None:
    """
    Génère le dataset de trafic et l'écrit dans un fichier CSV local.
    Format : row_key,road_id,timestamp,vehicle_id,speed,lat,lon

    :param output_path: Chemin du fichier CSV à créer.
    """
    end_time   = datetime.now()
    start_time = end_time - timedelta(days=30)

    log.info("Génération de %s lignes de données de trafic...", f"{TOTAL_ROWS:,}")
    t0 = time.time()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # En-tête compatible ImportTsv : row_key en première colonne
        writer.writerow(["row_key", "road_id", "timestamp",
                         "vehicle_id", "speed", "lat", "lon"])

        for i in range(TOTAL_ROWS):
            vehicle_id = "VH{:05d}".format(random.randint(1, 50000))
            road_id    = random.choice(ROAD_IDS)
            speed      = round(random.uniform(0, 140), 2)
            lat        = round(BASE_LAT + random.uniform(-0.5, 0.5), 6)
            lon        = round(BASE_LON + random.uniform(-0.5, 0.5), 6)
            ts         = random_timestamp(start_time, end_time)
            ts_epoch   = ts.timestamp()
            ts_str     = ts.strftime("%Y-%m-%d %H:%M:%S")
            row_key    = make_row_key(road_id, ts_epoch, vehicle_id)

            writer.writerow([row_key, road_id, ts_str,
                             vehicle_id, speed, lat, lon])

            # Rapport de progression toutes les 50 000 lignes
            if (i + 1) % 50_000 == 0:
                elapsed = time.time() - t0
                rate    = (i + 1) / elapsed
                log.info("  %s / %s lignes générées (%s lignes/s)...",
                         f"{i+1:,}", f"{TOTAL_ROWS:,}", f"{rate:,.0f}")

    elapsed = time.time() - t0
    log.info("[OK] CSV généré : %s lignes en %.1fs.", f"{TOTAL_ROWS:,}", elapsed)


# ─── Étape 2 : Copie dans le conteneur namenode ───────────────────────────────

def copy_to_container(local_path: str) -> None:
    """
    Copie le fichier CSV local dans le conteneur namenode via 'docker cp'.

    :param local_path: Chemin du fichier CSV local.
    :raises RuntimeError: Si la copie échoue.
    """
    log.info("Copie du CSV dans le conteneur '%s'...", CONTAINER_NN)
    r = subprocess.run(
        ["docker", "cp", local_path, "{0}:{1}".format(CONTAINER_NN, CONTAINER_TMP)],
        capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        raise RuntimeError("docker cp a échoué : {0}".format(r.stderr))
    log.info("[OK] CSV copié → %s:%s", CONTAINER_NN, CONTAINER_TMP)


# ─── Étape 3 : Upload vers HDFS ──────────────────────────────────────────────

def upload_to_hdfs() -> None:
    """
    Crée le répertoire HDFS cible et uploads le CSV via
    'docker exec namenode hdfs dfs -put'.

    :raises RuntimeError: Si l'une des commandes HDFS échoue.
    """
    log.info("Création du répertoire HDFS /traffic/raw/...")
    # Création du répertoire parent
    r = subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-mkdir", "-p", "/traffic/raw"],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        raise RuntimeError("Impossible de créer /traffic/raw : {0}".format(r.stderr))

    # Suppression de l'ancien fichier si présent
    subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-rm", "-f", HDFS_RAW_PATH],
        capture_output=True, text=True, timeout=30,
    )

    log.info("Upload du CSV vers HDFS : %s ...", HDFS_RAW_PATH)
    r = subprocess.run(
        ["docker", "exec", CONTAINER_NN,
         "hdfs", "dfs", "-put", CONTAINER_TMP, HDFS_RAW_PATH],
        capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        raise RuntimeError("hdfs -put a échoué : {0}".format(r.stderr))
    log.info("[OK] CSV disponible dans HDFS : %s", HDFS_RAW_PATH)


# ─── Étape 4 : Création de la table HBase ────────────────────────────────────

HBASE_SHELL_CREATE = """\
disable '{table}'
drop '{table}'
create '{table}', 'cf'
exit
""".format(table=HBASE_TABLE)


def create_hbase_table() -> None:
    """
    Crée (ou recrée) la table HBase via 'docker exec hbase-master hbase shell'.
    Les commandes Ruby HBase Shell sont passées sur stdin.

    :raises RuntimeError: Si le shell HBase retourne une erreur.
    """
    log.info("Création de la table HBase '%s'...", HBASE_TABLE)
    r = subprocess.run(
        ["docker", "exec", "-i", CONTAINER_HBASE, "hbase", "shell"],
        input=HBASE_SHELL_CREATE,
        capture_output=True, text=True, timeout=120,
    )
    # Le shell HBase retourne toujours 0, on vérifie la sortie manuellement
    if "ERROR" in r.stdout and "rescue nil" not in r.stdout:
        log.warning("Sortie HBase Shell : %s", r.stdout[-300:])
    else:
        log.info("[OK] Table HBase '%s' prête.", HBASE_TABLE)


# ─── Étape 5 : Chargement HBase via ImportTsv ────────────────────────────────

def import_to_hbase() -> None:
    """
    Charge les données depuis HDFS vers HBase en utilisant
    'docker exec hbase-master hbase org.apache.hadoop.hbase.mapreduce.ImportTsv'.
    Cette approche évite 500k appels 'put' individuels.

    Colonnes ImportTsv :
        HBASE_ROW_KEY, cf:road_id, cf:timestamp, cf:vehicle_id, cf:speed, cf:lat, cf:lon

    :raises RuntimeError: Si le job MapReduce ImportTsv échoue.
    """
    log.info("Chargement des données dans HBase via ImportTsv (job MR)...")

    import_cmd = [
        "hbase",
        "org.apache.hadoop.hbase.mapreduce.ImportTsv",
        "-Dimporttsv.separator=,",
        "-Dimporttsv.skip.bad.lines=true",
        (
            "-Dimporttsv.columns="
            "HBASE_ROW_KEY,"
            "cf:road_id,"
            "cf:timestamp,"
            "cf:vehicle_id,"
            "cf:speed,"
            "cf:lat,"
            "cf:lon"
        ),
        HBASE_TABLE,
        "hdfs://namenode:9000" + HDFS_RAW_PATH,
    ]

    r = subprocess.run(
        ["docker", "exec", CONTAINER_HBASE] + import_cmd,
        capture_output=True, text=True, timeout=900,  # 15 min max pour 500k lignes
    )

    # ImportTsv retourne 0 même en cas d'avertissements, vérifier stderr
    if r.returncode != 0:
        log.error("ImportTsv stdout : %s", r.stdout[-5000:])
        log.error("ImportTsv stderr : %s", r.stderr[-5000:])
        raise RuntimeError(
            "ImportTsv a échoué (code {0}).".format(r.returncode)
        )

    log.info("[OK] Données chargées dans HBase (table : %s).", HBASE_TABLE)


# ─── Point d'entrée ──────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  STMS — Générateur de données de trafic")
    print("=" * 60)

    # Fichier CSV temporaire local
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix="stms_traffic_")
    os.close(tmp_fd)

    try:
        # 1. Génération du CSV
        generate_csv(tmp_path)

        # 2. Copie dans le conteneur namenode
        copy_to_container(tmp_path)

        # 3. Upload vers HDFS
        upload_to_hdfs()

        # 4. Création de la table HBase
        create_hbase_table()

        # 5. Chargement des données via ImportTsv
        import_to_hbase()

        print("")
        print("=" * 60)
        print("  [SUCCESS] Génération et chargement terminés !")
        print("  HDFS  : {0}".format(HDFS_RAW_PATH))
        print("  HBase : table '{0}'".format(HBASE_TABLE))
        print("=" * 60)

    except Exception as exc:
        log.error("ERREUR fatale : %s", exc)
        sys.exit(1)
    finally:
        # Suppression du fichier temporaire local
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            log.info("Fichier temporaire local supprimé : %s", tmp_path)


if __name__ == "__main__":
    main()
