"""
Utilitaires Docker — exécution de commandes dans les conteneurs.
Toutes les interactions HDFS / HBase passent par ce module.
"""

import subprocess
import logging
import sys

# ─── Journalisation ──────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ─── Noms des conteneurs (doivent correspondre au docker-compose) ─────────────
CONTAINER_NAMENODE   = "namenode"
CONTAINER_HBASE      = "hbase-master"

# ─── Chemins dans les conteneurs ─────────────────────────────────────────────
# Chemin vers le JAR Hadoop Streaming dans le namenode (Hadoop 3.2.1)
STREAMING_JAR = (
    "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
)


def docker_exec(container: str, cmd: list, stdin_data: str = None,
                timeout: int = 120) -> subprocess.CompletedProcess:
    """
    Exécute une commande dans un conteneur Docker via 'docker exec'.

    :param container:   Nom du conteneur cible.
    :param cmd:         Commande à exécuter (liste de tokens).
    :param stdin_data:  Données à passer sur stdin (optionnel).
    :param timeout:     Délai maximal en secondes.
    :returns:           Résultat de subprocess.run.
    """
    full_cmd = ["docker", "exec"]
    if stdin_data is not None:
        full_cmd.append("-i")   # Mode interactif pour lire stdin
    full_cmd += [container] + cmd

    log.debug("docker exec : %s", " ".join(full_cmd[:6]))  # Journal tronqué

    result = subprocess.run(
        full_cmd,
        input=stdin_data,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result


def hdfs_exec(cmd: list, timeout: int = 120) -> subprocess.CompletedProcess:
    """
    Raccourci : exécute une commande 'hdfs dfs' dans le conteneur namenode.

    :param cmd:     Arguments après 'hdfs dfs' (ex : ['-ls', '/traffic']).
    :param timeout: Délai maximal en secondes.
    :returns:       Résultat subprocess.
    """
    return docker_exec(CONTAINER_NAMENODE, ["hdfs", "dfs"] + cmd,
                       timeout=timeout)


def hdfs_path_exists(hdfs_path: str) -> bool:
    """
    Vérifie si un chemin HDFS (fichier ou répertoire) existe.

    :param hdfs_path: Chemin HDFS absolu (ex : '/traffic/output/vehicle_count').
    :returns:         True si le chemin existe, False sinon.
    """
    result = hdfs_exec(["-test", "-e", hdfs_path], timeout=30)
    return result.returncode == 0


def hdfs_cat(hdfs_glob: str, timeout: int = 60) -> list:
    """
    Lit le contenu d'un ou plusieurs fichiers HDFS via 'hdfs dfs -cat'.

    :param hdfs_glob: Chemin ou glob HDFS (ex : '/traffic/output/job/part-*').
    :param timeout:   Délai maximal en secondes.
    :returns:         Liste de lignes non vides.
    :raises RuntimeError: Si la commande échoue ou dépasse le délai.
    """
    try:
        result = hdfs_exec(["-cat", hdfs_glob], timeout=timeout)
        if result.returncode != 0:
            log.error("hdfs -cat a échoué : %s", result.stderr[:500])
            raise RuntimeError(
                f"Impossible de lire '{hdfs_glob}' depuis HDFS : {result.stderr[:200]}"
            )
        return [l.strip() for l in result.stdout.splitlines() if l.strip()]
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Délai dépassé lors de la lecture HDFS : {hdfs_glob}")


def hdfs_rm(hdfs_path: str) -> None:
    """
    Supprime un chemin HDFS (silencieux si inexistant).

    :param hdfs_path: Chemin HDFS à supprimer.
    """
    hdfs_exec(["-rm", "-r", "-f", hdfs_path], timeout=30)
    log.debug("HDFS supprimé : %s", hdfs_path)


def hdfs_mkdir(hdfs_path: str) -> None:
    """
    Crée un répertoire HDFS (et ses parents) si nécessaire.

    :param hdfs_path: Chemin HDFS à créer.
    """
    hdfs_exec(["-mkdir", "-p", hdfs_path], timeout=30)
    log.debug("HDFS répertoire créé : %s", hdfs_path)
