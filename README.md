# Projet 12

CheckItAI conçoit des solutions d'intelligence artificielle pour lutter contre les fake news. Pour enrichir son moteur d'analyse, l'entreprise souhaite disposer d'un pipeline d'acquisition de données multimodales (texte + image).

Ce projet met en place un système d'extraction automatisée qui récupère des publications (articles ou posts) contenant à la fois du texte et des images, depuis plusieurs sources accessibles (API, sites web). Les extractions sont orchestrées par Airflow et suivies dans un tableau de bord Streamlit.

## Installation

```bash
uv sync
```

## Airflow

Les secrets ne sont pas versionnés : copier `.env.example` en `.env` (clé API Google) et `airflow/.env.example` en `airflow/.env`, puis renseigner chaque valeur (les commandes de génération sont dans le fichier). `docker compose` refuse de démarrer si un secret manque.

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

Interface : http://localhost:8080 (identifiants : `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` définis dans `airflow/.env`)

Arrêt :

```bash
docker compose down
```

## Streamlit

```bash
uv run streamlit run streamlit_dashboard.py
```

Interface : http://localhost:8501

## Documentation

- [Plan de monitoring](docs/plan_monitoring.md)
- [Rapport d'exploration des sources](docs/rapport_exploration_sources.md)
- [Schéma conceptuel](docs/schema_conceptuel.md)
