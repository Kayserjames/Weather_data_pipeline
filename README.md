# 🌤️ WeatherPipeline — Pipeline ETL météo automatisé (Cameroun)

Pipeline de données end-to-end qui extrait, transforme, contrôle et charge quotidiennement les données météo de 11 villes camerounaises, orchestré avec Apache Airflow et visualisé dans un dashboard Power BI.

Ce projet est une évolution d'un premier pipeline plus simple réalisé dans le cadre d'un stage chez **AnalystLab Africa**, retravaillé ici avec une approche orchestration, monitoring et bonnes pratiques de production.

---

## 📐 Architecture

```
OpenWeatherMap API
        │
        ▼
   ┌─────────┐      ┌───────────┐      ┌────────────────┐      ┌──────────┐
   │ Extract │ ───▶ │ Transform │ ───▶ │ Quality Check   │ ───▶ │  Load    │
   └─────────┘      └───────────┘      └────────────────┘      └──────────┘
        │                                                             │
        │                                                             ▼
   Orchestré par Apache Airflow (Docker Compose)              PostgreSQL
                                                                       │
                                                                       ▼
                                                                  Power BI
                                                              (dashboard 7 jours)
```

---

## ⚙️ Fonctionnalités

- **Extraction** des données météo (température, humidité, vent, conditions) pour 11 villes du Cameroun via l'API OpenWeatherMap
- **Transformation** : nettoyage et mise en forme des données
- **Contrôle qualité** avant chargement :
  - blocage si le jeu de données est vide ou si une colonne essentielle manque
  - blocage si moins de 50 % des villes attendues sont récupérées
  - avertissements (non bloquants) pour valeurs manquantes, doublons, valeurs hors plage plausible
- **Retry automatique** de l'extraction si moins de 80 % des villes attendues sont récupérées (tolère les pannes réseau ponctuelles de l'API)
- **Chargement** en base PostgreSQL avec gestion des doublons (`ON CONFLICT ... DO NOTHING`)
- **Orchestration** quotidienne via un DAG Airflow (Docker Compose)
- **Dashboard Power BI** avec indicateurs glissants sur 7 jours (température moyenne, humidité, vent)

---

## 🛠️ Stack technique

| Composant | Technologie |
|---|---|
| Langage | Python 3.13 |
| Extraction | `requests`, API OpenWeatherMap |
| Transformation | `pandas` |
| Base de données | PostgreSQL |
| ORM / connexion DB | `SQLAlchemy`, `psycopg2` |
| Orchestration | Apache Airflow (TaskFlow API) |
| Conteneurisation | Docker / Docker Compose |
| Visualisation | Power BI |

---

## 📁 Structure du projet

```
WeatherPipeline/
├── dags/
│   └── weather_ETL_dag.py       # DAG Airflow (extract → transform → quality_check → load)
├── Weather_pipeline_utils.py    # Fonctions extract(), transform(), quality_check(), load()
├── docker-compose.yaml          # Environnement Airflow conteneurisé
├── .env                         # Variables sensibles (non versionné)
├── .gitignore
└── README.md
```

---

## 🚀 Installation et exécution

### Prérequis
- Docker et Docker Compose
- Une clé API [OpenWeatherMap](https://openweathermap.org/api)
- Une base PostgreSQL accessible (locale ou distante)

### 1. Cloner le dépôt
```bash
git clone <url-du-repo>
cd WeatherPipeline
```

### 2. Configurer les variables d'environnement
Crée un fichier `.env` à la racine du projet :
```
API_KEY=ta_clé_openweathermap
Weather_DB_USER=postgres
Weather_DB_PASSWORD=ton_mot_de_passe
Weather_DB_HOST=localhost
Weather_DB_PORT=5432
Weather_DB_NAME=weather_data
```
⚠️ Ce fichier est exclu du versionnement via `.gitignore` — ne jamais le committer.

### 3. Lancer l'environnement Airflow
```bash
docker compose up -d
```
L'interface Airflow est ensuite accessible sur [http://localhost:8080](http://localhost:8080).

### 4. Activer le DAG
Dans l'interface Airflow, active le DAG `Weatherdata_etl`. Il est planifié pour s'exécuter automatiquement chaque jour à 6h (`schedule="0 14 * * *"`), avec possibilité de le déclencher manuellement.

---

## ✅ Contrôle qualité des données

Chaque exécution passe par une étape `quality_check_task` avant le chargement en base, qui vérifie :

| Contrôle | Seuil | Comportement |
|---|---|---|
| DataFrame vide | — | Bloque le chargement |
| Colonne essentielle manquante | — | Bloque le chargement |
| Villes récupérées | < 50 % des villes attendues | Bloque le chargement |
| Villes récupérées | < 100 % des villes attendues | Avertissement (log) |
| Valeurs manquantes / doublons / valeurs hors plage | — | Avertissement (log) |

En complément, la tâche d'extraction retente automatiquement (jusqu'à 2 fois) si moins de 80 % des villes attendues sont récupérées, avant même d'atteindre l'étape de contrôle qualité.

---

## 📊 Dashboard

Le dashboard Power BI présente, sur une fenêtre glissante de 7 jours :
- Température, humidité et vitesse du vent moyennes
- Comparaison entre villes
- Répartition des conditions météo
- Un indicateur de monitoring : nombre de villes chargées par jour, pour repérer visuellement les extractions incomplètes

