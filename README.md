# STMS (Smart Traffic Monitoring System)

## 🚦 À propos du projet
Le système **STMS** est une architecture intelligente d'analyse du trafic routier, conçue pour stocker, traiter et visualiser de très gros volumes de données.
L'infrastructure s'appuie entièrement sur des conteneurs **Docker** pour le Big Data (Hadoop, HDFS, HBase, Zookeeper), couplée à un backend Python local et un frontend moderne mis à jour.

## 🔧 Architecture & Technologies
- **Génération de données :** Scripts Python pour simuler et injecter un flux massif de véhicules, contenant vitesse, localisation, timestamps et identifiants.
- **Stockage (HDFS & HBase) :** Les fichiers CSV de trafic sont centralisés sur HDFS et intégrés de façon performante dans des tables **HBase** via l'outil natif Hadoop `ImportTsv`.
- **Traitement (MapReduce) :** Algorithmes d'agrégation écrits en Python et exécutés sur le cluster par le **Hadoop Streaming API**.
  - **Job 1** : Tally global volumétrique (véhicules par routes).
  - **Job 2** : Calcul des moyennes de vitesse par axes.
  - **Job 3** : Distribution du trafic sur 24H (heures de pointe).
  - **Job 4** : Détection des points de crise et de la congestion selon des seuils de densité et de lenteur.
- **Visualisation :** Application **Flask** servant un Dashboard analytique (Chart.js, Boxicons) qui lit directement les résultats consolidés (`/traffic/output/...`) depuis la couche HDFS pour des temps de réponse instantanés.

---

## 🛠 Prérequis
- **Docker** et **Docker Compose** opérationnels.
- **Python 3.x** installé sur la machine hôte.
- Dépendances du projet :
  ```bash
  pip install -r requirements.txt
  ```

---

## 🚀 Comment lancer le projet

### 1. Démarrer l'infrastructure Hadoop/HBase
Lancez les conteneurs définis dans le `docker-compose.yml`. Tous les services (Namenode, Datanode, HBase, Zookeeper) seront instanciés en arrière-plan.
```bash
docker-compose up -d
```
*(Attendez 1 à 2 minutes lors de la toute première exécution le temps que HBase et HDFS s'initialisent correctement et quittent le Safe Mode).*

### 2. Générer et importer le trafic massifié
Exécutez le script principal de bout en bout. Il générera 500 000 lignes localement, les poussera vers le `namenode`, puis invoquera le chargement `ImportTsv`.
```bash
python data_generator\generate_traffic_data.py
```

### 3. Traiter la donnée (Jobs MapReduce)
Lancez le script d'orchestration qui transfère virtuellement l'ordre d'analyse MapReduce à Hadoop (vous pouvez relancer ce script plus tard quand vous le souhaitez pour rafraîchir l'analyse).
```bash
python run_jobs.py all
```

### 4. Démarrer le Tableau de bord
Une fois les résultats de sortie agrégés sur HDFS (visibles dans les logs du script précédent), démarrez l'API Flask :
```bash
python flask_app\app.py
```
👉 **Interface accessible sur :** [http://127.0.0.1:5000](http://127.0.0.1:5000)

---

