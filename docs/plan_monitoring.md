# Plan de monitoring du pipeline ETL

Ce document décrit comment surveiller le pipeline `extraction → transformation → chargement` (voir [schema_conceptuel.md](schema_conceptuel.md) et [rapport_exploration_sources.md](rapport_exploration_sources.md)), à quelle fréquence, avec quels seuils d'alerte, et comment réagir aux erreurs connues.

## 1. Objectif

Détecter trois types de dérive avant qu'ils ne dégradent le dataset final :
1. **Une source qui casse silencieusement** (le site change de structure, un lien de scraping ne répond plus, une API renvoie un format différent) — le pipeline continue de tourner mais produit moins (ou plus) de données valides qu'avant.
2. **Une source qui devient trop lente ou trop coûteuse** — signe d'un problème réseau, d'un site qui throttle, ou d'une fuite mémoire.
3. **Une erreur bloquante** (dépendance manquante, contrainte de base de données violée, fichier introuvable) qui arrête complètement une exécution.

## 2. Ce qui existe déjà (sources de signal)

Le pipeline journalise à trois niveaux, déjà en place :

| Niveau | Emplacement | Contenu | Rétention |
| --- | --- | --- | --- |
| **Logs d'exécution** | `logs/pipeline_<horodatage>.log` (+ console) | Chaque étape (lecture, adaptation, nettoyage, enrichissement, export), avec compteurs de lignes rejetées et raison | Un fichier par run, non purgé automatiquement |
| **Manifeste par dataset** | `data/processed/<source>.manifest.json` | Nombre de lignes, distribution des labels, anomalies de schéma détectées | Un fichier par source, écrasé à chaque run |
| **Métriques par exécution** | `data/metrics/etl_runs.jsonl` (`pipeline.metrics`) | Précision (% valide, détail des rejets), rapidité (durée totale + par étape), coût (mémoire pic) | Append-only, historique complet, alimente le [dashboard Streamlit](../streamlit_dashboard.py) |

Un enregistrement couvre les **trois phases** (extraction, transformation, chargement), pas seulement la transformation : chaque tâche Airflow (`extract_*`, `transform_*`, `load_*`) mesure sa propre phase indépendamment — nécessaire puisqu'elles peuvent s'exécuter sur des workers différents et ne peuvent donc pas partager un objet Python en mémoire — et la dernière tâche (`load_*`) fusionne les trois mesures en un seul enregistrement final (`pipeline.metrics.merge_phase_metrics`, orchestré dans `dags/_common.py`). En usage autonome (CLI/notebook, hors DAG), une seule phase existe et s'auto-enregistre.

Le [tableau de bord Streamlit](../streamlit_dashboard.py) lit directement `etl_runs.jsonl` — c'est aujourd'hui l'outil de consultation manuelle privilégié pour ces trois métriques. Les DAGs Airflow (`dags/*.py`) ajoutent en plus leurs propres logs de tâche (visibles dans l'UI Airflow) et un `retries: 1` par défaut sur chaque tâche.

## 3. Fréquence de vérification

| Contrôle | Fréquence | Qui / comment |
| --- | --- | --- |
| Statut de chaque tâche Airflow (succès/échec) | À chaque exécution du DAG | Airflow (UI + `retries`) |
| Lecture du dashboard Streamlit (qualité/vitesse/coût) | Après chaque run manuel, et au moins 1×/semaine si les DAGs tournent en automatique | Manuel, pour l'instant |
| Revue des `schema_issues` dans les manifestes | À chaque run | Actuellement loggé en `WARNING` mais non bloquant — à surveiller manuellement tant qu'aucune alerte automatique n'existe (§6) |
| Revue du taux de liens morts (FakeNewsNet, Bluesky) | Hebdomadaire | Comparer `rows_dropped_*` dans `etl_runs.jsonl` entre deux semaines |
| Purge/rotation des logs (`logs/`) | Mensuelle | Pas d'automatisation actuelle — répertoire non borné, à surveiller pour l'espace disque |

**Cadence des DAGs eux-mêmes** : `factcheck_etl` et `isot_etl` sont en `schedule=None` (déclenchement manuel), ce qui est cohérent avec des sources qui ne varient pas d'un jour à l'autre. **`bluesky_afp_etl` est actuellement lui aussi en `schedule=None`**, alors que le flux RSS n'expose que les ~30 derniers posts — sans exécution régulière, des posts sortent de la fenêtre sans jamais être collectés. C'est une incohérence à corriger avant mise en production (repasser à `@hourly` ou équivalent) plutôt qu'un point à surveiller : la déduplication par `guid` (`extraction/bluesky_afp.py`) rend les exécutions fréquentes sûres (aucun doublon), donc rien ne justifie de garder `None` ici.

## 4. Seuils d'alerte

Les mêmes seuils que le dashboard Streamlit (`streamlit_dashboard.py`), pour rester cohérent entre consultation manuelle et alerting automatique futur :

| KPI | 🟢 Bon | 🟠 À surveiller | 🔴 Critique | Justification |
| --- | --- | --- | --- | --- |
| **Précision** (% lignes valides) | ≥ 80 % | 50 – 80 % | < 50 % | Google Fact Check et ISOT tournent aujourd'hui à ~85-100% ; en dessous de 50%, la moitié des données collectées est perdue — signe quasi certain d'un changement de structure côté source |
| **Rapidité** (durée totale d'un run) | < 2× la durée moyenne historique de la source | 2-5× | > 5× | Pas encore assez d'historique pour un seuil absolu fiable (voir §7) — seuil **relatif** à la propre baseline de chaque source, à calculer sur `etl_runs.jsonl` une fois quelques semaines de données accumulées |
| **Coût** (mémoire pic) | < 500 Mo | 500 Mo – 1 Go | > 1 Go | Les runs observés à ce jour restent sous 150 Mo (sources actuelles, échantillonnées) ; ce seuil est large et devra être resserré si des sources non échantillonnées (ISOT complet, FakeNewsNet sans `max_rows`) deviennent la norme |
| **Erreur bloquante** | 0 | — | ≥ 1 | Toute exception non gérée (voir §5) est par nature critique : pas de palier intermédiaire |

Ces seuils sont des **points de départ**, à ajuster une fois plusieurs semaines d'historique disponibles dans `etl_runs.jsonl` (voir le notebook [`kpi_exploration.ipynb`](../notebooks/kpi_exploration.ipynb), section 4, pour visualiser la tendance dans le temps).

## 5. Canaux d'alerte

**Ce qui est en place aujourd'hui** :
- Échec de tâche Airflow → visible dans l'UI Airflow (page DAG), avec un retry automatique (`retries: 1`, `dags/_common.py` / DAG files)
- `schema_issues` → logué en `WARNING` dans les logs de run, mais **non remonté ailleurs** — nécessite une lecture manuelle

**Ce qui n'est pas encore en place** (proposition, non implémentée) :
- Notification push (email/Slack) sur échec de tâche Airflow via `on_failure_callback` dans `default_args` de chaque DAG — la mécanique existe côté Airflow, juste pas câblée aujourd'hui
- Alerte automatique si `pct_valid` d'un run tombe dans la zone 🟠/🔴 (§4) — nécessiterait une tâche Airflow supplémentaire type `check_metrics_task`, exécutée après `load_task`, qui lit la dernière ligne de `etl_runs.jsonl` pour ce `source` et déclenche une alerte si le seuil est franchi
- Agrégation/visualisation centralisée au-delà du dashboard local (ex. Grafana + export des métriques) — hors scope pour le volume actuel du projet

## 6. Gestion des erreurs connues

Catalogue des modes de défaillance déjà rencontrés pendant le développement, et la réponse appropriée :

| Erreur | Cause | Détection | Action |
| --- | --- | --- | --- |
| `ModuleNotFoundError` (ex. `curl_cffi`) dans une tâche Airflow | Dépendance absente de l'image Airflow (`_PIP_ADDITIONAL_REQUIREMENTS` dans `airflow/.env` non à jour) | Échec immédiat de la tâche, message explicite dans les logs Airflow | Ajouter le paquet manquant à `_PIP_ADDITIONAL_REQUIREMENTS` ; pour une solution durable (pas de réinstallation à chaque démarrage de conteneur), construire une image Airflow custom |
| `HTTP 403` répété sur une source scrapée (AFP/Bluesky) | Blocage WAF (empreinte TLS ou IP) | `http_status` dans le CSV brut, ou taux de `rows_exported` proche de 0 | Vérifier que `curl_cffi` est bien utilisé (pas `requests`) ; si le blocage persiste malgré `impersonate="chrome124"`, envisager une version Chrome plus récente ou un espacement des requêtes plus long |
| Taux de liens morts (404/410) élevé et croissant | Dataset vieillissant (FakeNewsNet) | `rows_dropped_*` ou logs `"dropped N/M dead link(s)"` | Comportement normal et attendu, déjà atténué par le nettoyage progressif de la source (`extraction/fakenewsnet.py`) ; pas d'action needed sauf si le taux dépasse ~50% d'un coup (signe d'un problème plus large, ex. le domaine source entier a disparu) |
| `IntegrityError: CHECK constraint failed` (SQLite) | Valeur de `label` hors `{fake, true}`, ou colonne mal typée à la lecture d'un CSV (ex. bug historique : une colonne 100% "true" auto-convertie en booléen par pandas) | Échec de la tâche `load_*` | Vérifier `dtype={"label": str}` sur tous les points de lecture CSV (`pipeline/run.py`, `pipeline/load.py`, `dags/_common.py`) ; ne jamais laisser pandas inférer le type d'une colonne d'énumération texte |
| `schema_issues` non vide dans le manifeste | Colonne requise manquante ou vide après transformation (ex. un adaptateur mal écrit) | `pipeline.schema.validate_schema()`, loggé en `WARNING`, visible dans `data/processed/<source>.manifest.json` | Ne bloque pas l'export (comportement volontaire, pour ne pas perdre un run entier sur un souci partiel) — mais toute occurrence doit être investiguée manuellement tant qu'aucune alerte automatique n'existe |
| Déséquilibre extrême des labels (ex. quasi 0% de `true`) | Biais structurel de la source (les fact-checkers vérifient surtout des affirmations fausses) | `label_distribution` dans le manifeste | Ne pas tenter de "corriger" en sous-échantillonnant la classe majoritaire à l'aveugle (perte de données) — plutôt élargir la collecte (plus de sites, `max_results` plus grand) avant d'envisager un rééquilibrage |

## 7. Limites actuelles de ce plan

- Les seuils de rapidité et de coût (§4) sont provisoires : peu d'historique disponible au moment de la rédaction. À revoir après quelques semaines de runs réels.
- Aucune alerte n'est **automatiquement poussée** (email/Slack) aujourd'hui — la détection reste manuelle via le dashboard ou la lecture des logs/manifestes. C'est le principal écart entre ce plan et un monitoring de production complet.
- `bluesky_afp` n'a pas de label exploitable (voir [rapport_exploration_sources.md](rapport_exploration_sources.md#4-flux-rss-bluesky-afp-factuel)) : son `pct_valid` restera à 0% après l'étape de mapping de label jusqu'à ce qu'une heuristique de détection de verdict soit implémentée — ne pas interpréter ce 0% comme une panne du pipeline.
