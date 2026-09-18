# Rapport d'exploration des sources de données

Ce document recense les sources de données explorées pour le projet de détection de fake news, leurs caractéristiques, et la méthode d'extraction retenue (ou proposée) pour chacune. Quatre sources sont aujourd'hui implémentées (`src/extraction/`) et alimentent le [schéma canonique commun](schema_conceptuel.md) via `src/pipeline/`.

## Vue d'ensemble

| Source | Format brut | Langue | Modalités | Label | Volume approx. | Conditions d'accès | Statut |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [Google Fact Check Tools](#1-google-fact-check-tools-api) | API REST + scraping HTML | Multilingue (16 fact-checkers) | Texte | Binaire, normalisé depuis une évaluation libre | ~700-1000 lignes/run | Clé API obligatoire, quota non publié | ✅ Implémentée |
| [FakeNewsNet](#2-fakenewsnet) | CSV local + scraping HTML | Anglais | Texte | Binaire, un fichier = un label | ~23 000 lignes (4 fichiers) | Pas de licence formelle, citation requise, droits d'auteur des éditeurs applicables aux articles scrapés | ✅ Implémentée |
| [ISOT Fake News](#3-isot-fake-news-dataset) | CSV local (Kaggle) | Anglais | Texte | Binaire, un fichier = un label | ~45 000 lignes (2 fichiers) | Licence MIT, aucune restriction | ✅ Implémentée |
| [Bluesky RSS (AFP Factuel)](#4-flux-rss-bluesky-afp-factuel) | RSS XML + scraping HTML | Français | Texte + Image | Aucun label structuré (verdict implicite non extrait) | 30 posts/appel (fenêtre glissante) | Non vérifiées (source mise de côté) | 🔲 Implémentée (Aucun label "vrai" identifié) |
| [Fakeddit](#5-fakeddit-identifiée-non-implémentée) | TSV + téléchargement d'images | Anglais | Texte + Image | Multi-classes (2/3/6 voies) | ~1M lignes | Pas de licence formelle ; images Reddit soumises au Reddit User Agreement (non traité par Fakeddit) | 🔲 Identifiée, non implémentée |

---

## 1. Google Fact Check Tools API

**Lien** : https://toolbox.google.com/factcheck/explorer

**Conditions d'accès** : authentification par **clé API obligatoire** — confirmé en interrogeant l'endpoint sans clé : `403 PERMISSION_DENIED — "Method doesn't allow unregistered callers... Please use API Key or other form of API consumer identity"`. Soumis aux [Google APIs Terms of Service](https://developers.google.com/terms) ; les [conditions spécifiques](https://developers.google.com/fact-check/tools/api/terms) de la Fact Check Tools API n'imposent d'obligation d'attribution ("contribuer à Data Commons") que pour l'API ClaimReview *Read/Write* — non utilisée ici, seule l'API de lecture publique `claims:search` l'est. Aucun quota journalier précis n'est publié dans la documentation consultée pour cet endpoint ; à surveiller empiriquement (erreurs `429`).

**Format** : API REST (JSON) — endpoint `claims:search`. Le corps complet de l'article n'est pas fourni par l'API ; il est récupéré par scraping HTML de la page cible.

**Langue** : multilingue. La collecte interroge 16 organismes de fact-checking connus via `reviewPublisherSiteFilter` (politifact.com, snopes.com, factcheck.org, fullfact.org, leadstories.com, factcheck.afp.com, reuters.com, apnews.com, africacheck.org, boomlive.in, altnews.in, correctiv.org, maldita.es, newtral.es, rappler.com, verafiles.org) — anglais, français, espagnol notamment.

**Labels** : `textual_rating`, texte libre choisi par chaque organisme ("False", "Pants on Fire", "Mostly True", "Faux", "Trompeur", ...). Normalisé en label binaire `fake`/`true` par recherche de motifs (`pipeline.cleaning.map_textual_rating_to_label`) ; les évaluations ambiguës (mi-vrai, satire, non vérifiable) sont explicitement exclues plutôt que forcées dans une catégorie.

**Modalités** : texte uniquement (affirmation + titre + corps complet de l'article de vérification). Le support d'image a été volontairement retiré — l'API n'en fournit pas nativement et le scraping d'image a été jugé hors scope pour cette source.

**Méthode d'extraction** (`src/extraction/google_factcheck.py`) :
- Appel paginé de `claims:search` par site de publisher, filtré par `maxAgeDays` (défaut 180 jours) pour limiter les liens morts
- Scraping du corps de l'article via BeautifulSoup (heuristique `<article>`/`<main>`/paragraphes), seuil de longueur minimale (200 caractères) pour écarter les extractions ratées (paywall, page JS)
- Limite configurable de résultats par site (défaut 100), délai de 1 s entre requêtes

**Limite connue** : fort déséquilibre des labels (majorité de `fake`, peu de `true`) — inhérent au métier du fact-checking, qui vérifie surtout des affirmations fausses.

**Amélioration possible** : Faire correspondre l'argument `schedule` du DAG avec `maxAgeDays` pour récupérer seulement des entrées nouvelles.

---

## 2. FakeNewsNet

**Lien** : https://github.com/KaiDMML/FakeNewsNet

**Conditions d'accès** : **aucune licence open-source formelle** sur le dépôt. Copyright "(C) 2019 Arizona Board of Regents on Behalf of ASU" et citation académique demandée (BibTeX fournis par les auteurs). Point important : les auteurs précisent eux-mêmes que le dataset complet **ne peut pas être redistribué**, en raison de la politique de confidentialité de Twitter et **des droits d'auteur des éditeurs de presse** — cet avertissement s'applique directement au scraping des `news_url` mis en œuvre ici : chaque article récupéré reste soumis au droit d'auteur de son site d'origine, non couvert par une quelconque autorisation du dépôt FakeNewsNet. Téléchargement des CSV sans authentification.

**Format** : 4 fichiers CSV locaux (`gossipcop_fake.csv`, `gossipcop_real.csv`, `politifact_fake.csv`, `politifact_real.csv`), colonnes `id, news_url, title, tweet_ids`. Ni image, ni corps d'article inclus — seule l'URL de la source originale est fournie.

**Langue** : anglais.

**Labels** : un fichier = un label (`fake` ou `real`/`true`), déterminé par le nom du fichier, pas par une colonne.

**Modalités** : texte (titre + corps de l'article, récupéré par scraping de `news_url`). Pas d'image, à la demande explicite lors de la conception de cette source.

**Méthode d'extraction** (`src/extraction/fakenewsnet.py`) :
- Échantillonnage aléatoire (`max_rows`, défaut 200) par fichier — les fichiers complets font 5 000 à 17 000 lignes, un scraping exhaustif à 1 req/s serait trop long
- Scraping du texte de l'article, capture du code HTTP pour distinguer lien mort (404/410) d'un blocage temporaire (403, à conserver)
- **Nettoyage progressif de la source** : les liens confirmés morts sont retirés du CSV source à chaque run (les lignes jamais testées restent intactes) — évite de re-tester indéfiniment les mêmes liens morts au fil des exécutions répétées (DAG notamment)

**Limite connue** : dataset vieillissant (URLs de presse de 2016-2018), taux de liens morts significatif et croissant avec le temps.

**Amélioration possible** : Scraping complet des entrées présentes dans les fichiers sources.

---

## 3. ISOT Fake News Dataset

**Lien** : https://www.kaggle.com/datasets/rahulogoel/isot-fake-news-dataset

**Conditions d'accès** : licence **MIT** (confirmée via l'API Kaggle : `licenseNameNullable: "MIT"`) — la plus permissive des sources retenues, sans restriction de réutilisation, redistribution ou usage commercial. Téléchargement public, aucune authentification requise.

**Format** : 2 fichiers CSV locaux (`Fake.csv`, `True.csv`), colonnes `title, text, subject, date`. **Aucune information externe à récupérer** — texte complet déjà inclus, aucun appel réseau nécessaire.

**Langue** : anglais.

**Labels** : un fichier = un label (`fake`/`true`).

**Modalités** : texte uniquement (titre + texte complet).

**Méthode d'extraction** (`src/extraction/isot.py`) :
- Lecture directe des deux fichiers, fusion en une seule sortie taguée par label (contrairement à FakeNewsNet, pas de raison de garder les fichiers séparés puisqu'il n'y a ni scraping ni filtrage spécifique par fichier)
- Aucun réseau, aucune image

**Particularité** : formats de date hétérogènes au sein du même fichier (`Fake.csv` mélange "December 31, 2017" et "19-Feb-18"), nécessitant un parsing tolérant (`format="mixed"`) plutôt qu'un format unique. Sujets variés (`politicsNews`, `worldnews`, `News`, `politics`, `Government News`, `left-news`, `US_News`, `Middle-east`) pouvant servir de métadonnée additionnelle.

---

## 4. Flux RSS Bluesky (AFP Factuel)

**Lien** : https://bsky.app/profile/factuel.afp.com/rss

**Format** : flux RSS 2.0 (XML). Ne contient que le texte court du post et son lien — ni image, ni article complet. Chaque post référence un article de vérification complet sur `factuel.afp.com` via un lien court (`u.afp.com/...`).

**Langue** : français.

**Labels** : **aucun label structuré exploitable actuellement**. Le verdict est présent dans le texte du post sous forme d'émoji (❌ faux, ⚠️ trompeur, rarement ✅ vrai) mais aucune heuristique de détection n'a été implémentée — c'est une limite connue et documentée, à traiter dans un futur travail si cette source doit produire des données exploitables pour l'entraînement.

**Conditions d'accès** : non vérifiées — cette source a été mise de côté, la vérification (robots.txt, conditions d'utilisation AFP/Bluesky) n'a donc pas été menée.

**Modalités** : texte **et image** — seule source du projet où l'image est effectivement récupérée et téléchargée (`og:image`/`twitter:image` de l'article cible).

**Méthode d'extraction** (`src/extraction/bluesky_afp.py`) :
- Parsing XML du flux (`xml.etree.ElementTree`), extraction du lien article comme dernière URL mentionnée dans le texte du post
- Scraping du texte et de l'image de l'article via `curl_cffi` (impersonation TLS Chrome) plutôt que `requests` — le site AFP est protégé par un WAF Akamai qui bloque par empreinte TLS, indépendamment des en-têtes HTTP utilisés
- **Déduplication incrémentale par `guid`** : le flux ne renvoie que les ~30 derniers posts ; chaque exécution ne traite que les posts absents du fichier de sortie existant, et ne re-télécharge pas une image déjà présente sur disque — condition nécessaire pour un DAG exécuté fréquemment (`@hourly`) sans re-solliciter inutilement le site

**Limite connue** : absence de label exploitable (voir ci-dessus) ; fenêtre de collecte limitée aux ~30 derniers posts, donc dépendance à une exécution suffisamment régulière pour ne rien manquer.

**Amélioration possible** : Définir du label à partir du texte de l'article même s'il semble ne s'agir que de label `faux`.

---

## 5. Fakeddit (identifiée, non implémentée)

**Lien** : https://fakeddit.netlify.app/

**Conditions d'accès** : **aucune licence formelle** publiée sur la page du projet — seulement des consignes d'usage pour la recherche (utiliser les colonnes `6_way_label`/`clean_title`, ne pas extraire de vérité terrain depuis internet). Téléchargement (GitHub/Google Drive) sans authentification. Point non couvert par la page Fakeddit elle-même : **les images proviennent de Reddit** (hébergement type `i.redd.it`), donc leur réutilisation reste en principe soumise au [Reddit User Agreement](https://www.redditinc.com/policies/user-agreement) et aux conditions de l'API Reddit — à clarifier avant toute implémentation de cette source.

**Format** : TSV, avec liens vers les images associées à chaque publication Reddit (à télécharger séparément).

**Langue** : anglais.

**Labels** : classification multi-classes native (2, 3 ou 6 voies selon le niveau de granularité choisi par le dataset), contrairement aux autres sources qui n'offrent qu'un binaire fake/true — nécessiterait une réflexion sur le remapping vers le schéma canonique binaire actuel, ou une évolution du schéma pour accueillir un label multi-classes.

**Modalités** : texte (titre du post) et image (photo associée au post Reddit).

**Méthode d'extraction proposée** (non implémentée) :
- Téléchargement du TSV puis lecture directe (pas de scraping HTML nécessaire, à l'instar d'ISOT)
- Téléchargement des images par lot depuis les URLs fournies dans le TSV, avec le même garde-fou de validation syntaxique que le reste du pipeline (`pipeline.cleaning.valide_image`) avant toute tentative de téléchargement
- Adaptateur dédié (`pipeline/adapters/fakeddit.py`) qui devra soit réduire le label au binaire `fake`/`true` (perte d'information mais cohérence immédiate avec le schéma existant), soit être accompagné d'une évolution du champ `label` pour accepter des valeurs multi-classes selon le cas d'usage visé

**Raison de la non-priorisation** : volume très supérieur aux autres sources (~1 million de lignes), ce qui implique une stratégie d'échantillonnage plus réfléchie qu'un simple `max_rows` aléatoire, et un remapping de label qui reste à trancher avant l'implémentation.

---

## Schéma canonique commun

Les quatre sources implémentées convergent vers le même schéma de sortie (`pipeline.schema.CANONICAL_SCHEMA`), documenté en détail dans [schema_conceptuel.md](schema_conceptuel.md) : un adaptateur par source traduit ses colonnes brutes vers ce schéma commun (`claim_text`, `article_text`, `image_url`, `label`, dates harmonisées en ISO 8601 UTC, etc.), pendant que le nettoyage, la validation et l'export restent strictement identiques quelle que soit la source. C'est ce qui permet à Fakeddit — ou toute future source — de s'intégrer sans modifier le reste du pipeline.
