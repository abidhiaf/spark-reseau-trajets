# Analyse d'un réseau de trajets avec Spark (PySpark)

Projet individuel — *Low-Level LakeHouse with Spark*, MSc Data Engineering & Cloud Computing (aivancity), 2025-2026.
Auteur : Ahmed Abidhiaf.

Chaîne de traitement reproductible : CSV bruts → contrôle qualité → indicateurs RDD → indicateurs DataFrame
et couche Parquet → plans d'exécution et benchmark → graphe orienté des stations.

## Versions

| Outil | Version |
|---|---|
| Python | 3.12 |
| PySpark (pip) | 3.5.3 (moteur affiché : Spark 3.5.0) |
| Java | 17 |
| OS de test | macOS, mode `local[*]` |

> PySpark 3.5 ne fonctionne pas avec Python ≥ 3.13 : utiliser Python 3.12.

## Installation

```bash
git clone https://github.com/abidhiaf/spark-reseau-trajets.git
cd spark-reseau-trajets
uv venv --python 3.12 .venv          # ou : python3.12 -m venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt   # ou : pip install -r requirements.txt
```

## Reproduction

Tout relancer d'un coup (tests + étapes 00 à 05) :

```bash
./run_all.sh
```

Ou étape par étape, depuis la racine du projet et dans cet ordre :

```bash
python tests/test_regles.py         # 15 tests unitaires des règles qualité (sans Spark)
python src/00_exploration.py        # exploration des anomalies (lecture seule)
python src/01_ingestion.py          # règles qualité, bilan des rejets
python src/02_rdd_indicateurs.py    # indicateurs RDD
python src/03_dataframe_parquet.py  # indicateurs DataFrame, comparaison, Parquet partitionné par mois
python src/04_plan_benchmark.py     # lignée RDD, explain(), benchmark (option --pause pour l'UI Spark)
python src/05_graphe.py             # graphe orienté, degrés, top 5
```

L'étape 03 compare ses résultats aux CSV produits par l'étape 02 : lancer 02 avant 03.
L'étape 04 lit le Parquet écrit par l'étape 03.

## Organisation

```
data/        stations.csv, trajets.csv (données fournies)
tests/       test_regles.py (tests unitaires des règles)
src/         commun.py (structures typées, règles qualité) + scripts 00 à 05
output/      sorties chiffrées (CSV) ; output/parquet/ est régénéré par l'étape 03
captures/    captures des exécutions et de l'interface Spark
rapport/     rapport.md et rapport.pdf
```

## Sorties chiffrées (`output/`)

| Fichier | Contenu |
|---|---|
| `bilan_rejets.csv`, `rejets.csv` | nombre de rejets par motif, détail des lignes rejetées |
| `rdd_departs_arrivees.csv` | départs et arrivées par station |
| `rdd_duree_moyenne_zone.csv` | durée moyenne par zone de départ (somme, nombre, moyenne) |
| `rdd_top10_couples.csv` | 10 couples départ–arrivée les plus fréquents |
| `benchmark.csv`, `shuffle_volumes.csv` | temps (5 runs par variante) et volumes de shuffle |
| `graphe_degres.csv`, `graphe_top5.csv`, `graphe_arcs.csv` | degrés, top 5, arcs pondérés |
| `parquet/trajets/mois=AAAA-MM/` | trajets nettoyés au format Parquet |

## Règles de qualité (résumé)

Format (7 champs), date valide et comprise dans la période janvier–mars 2026, stations connues,
durée et distance numériques finies et strictement positives, pas de boucle, abonnement connu, `trajet_id` unique. Pour les doublons, on garde la **première occurrence dans l'ordre du fichier**.
Résultat : 24 005 lignes lues, 24 000 valides, 5 rejetées. Détails dans le rapport, section 2.

## Rapport

`rapport/rapport.pdf` (8 pages) est généré depuis `rapport/rapport.md` :

```bash
pip install markdown pymupdf
python rapport/build_pdf.py
```
