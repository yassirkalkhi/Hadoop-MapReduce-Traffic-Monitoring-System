#!/usr/bin/env bash
# =============================================================================
# Lance les 4 travaux MapReduce via Hadoop Streaming (Python).
# Usage : bash run_jobs.sh [job1|job2|job3|job4|all]
# =============================================================================

# ─── Variables de configuration ───────────────────────────────────────────────
HADOOP_STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar"
INPUT_PATH="/traffic/raw/data.csv"
OUTPUT_BASE="/traffic/output"
MR_DIR="$(cd "$(dirname "$0")/mapreduce" && pwd)"
LOG_FILE="/tmp/stms_mapreduce.log"
# ─────────────────────────────────────────────────────────────────────────────

# Redirige la sortie vers le journal et la console
exec > >(tee -a "$LOG_FILE") 2>&1

echo "============================================================"
echo "  STMS — Lancement des travaux MapReduce"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# ─── Fonction utilitaire : exécution d'un travail ─────────────────────────────
run_job() {
    local JOB_NAME="$1"
    local MAPPER="$2"
    local REDUCER="$3"
    local OUTPUT="$4"

    echo ""
    echo "──────────────────────────────────────────────────────"
    echo "  Travail : $JOB_NAME"
    echo "──────────────────────────────────────────────────────"

    # Suppression du répertoire de sortie s'il existe déjà
    hdfs dfs -rm -r -f "$OUTPUT" 2>/dev/null

    hadoop jar $HADOOP_STREAMING_JAR \
        -files "$MAPPER","$REDUCER" \
        -mapper "python3 $(basename "$MAPPER")" \
        -reducer "python3 $(basename "$REDUCER")" \
        -input  "$INPUT_PATH" \
        -output "$OUTPUT"

    local EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[OK] $JOB_NAME terminé avec succès → $OUTPUT"
    else
        echo "[ERREUR] $JOB_NAME a échoué (code : $EXIT_CODE)"
        return $EXIT_CODE
    fi
}
# ─────────────────────────────────────────────────────────────────────────────

# ─── Sélection des travaux à exécuter ────────────────────────────────────────
JOB="${1:-all}"

if [[ "$JOB" == "job1" || "$JOB" == "all" ]]; then
    run_job "Comptage de véhicules par route" \
        "$MR_DIR/job1_vehicle_count/mapper.py" \
        "$MR_DIR/job1_vehicle_count/reducer.py" \
        "$OUTPUT_BASE/vehicle_count"
fi

if [[ "$JOB" == "job2" || "$JOB" == "all" ]]; then
    run_job "Vitesse moyenne par route" \
        "$MR_DIR/job2_avg_speed/mapper.py" \
        "$MR_DIR/job2_avg_speed/reducer.py" \
        "$OUTPUT_BASE/avg_speed"
fi

if [[ "$JOB" == "job3" || "$JOB" == "all" ]]; then
    run_job "Trafic par heure (heures de pointe)" \
        "$MR_DIR/job3_peak_hours/mapper.py" \
        "$MR_DIR/job3_peak_hours/reducer.py" \
        "$OUTPUT_BASE/peak_hours"
fi

if [[ "$JOB" == "job4" || "$JOB" == "all" ]]; then
    run_job "Détection de congestion" \
        "$MR_DIR/job4_congestion/mapper.py" \
        "$MR_DIR/job4_congestion/reducer.py" \
        "$OUTPUT_BASE/congestion"
fi

echo ""
echo "============================================================"
echo "  Tous les travaux demandés sont terminés."
echo "  Journal : $LOG_FILE"
echo "============================================================"
