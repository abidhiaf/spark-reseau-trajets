---
title: "Analyse d'un réseau de trajets avec Spark (PySpark)"
subtitle: "Low-Level LakeHouse with Spark — Projet individuel"
author: "Ahmed Abidhiaf — MSc Data Engineering & Cloud Computing, 2e année"
date: "2025-2026"
---

<!--
LÉGENDE
  [TOI]      = paragraphe à rédiger par Ahmed (analyse, justification, conclusion)
  [CAPTURE]  = image à placer dans ../captures/ avec le nom indiqué
  [CHIFFRES] = tableau rempli à partir des fichiers de output/
Objectif : 5 à 8 pages au total.
-->

# 1. Introduction et environnement

[TOI] 4-5 lignes : contexte (entreprise de mobilité, 3 mois de trajets), objectif du
pipeline, choix de PySpark et pourquoi.

| Élément | Version |
|---|---|
| PySpark (pip) / moteur Spark affiché | 3.5.3 / 3.5.0 |
| Python | 3.12 |
| Java | 17.0.12 |
| Mode d'exécution | local[*] (macOS) |

Commande de lancement : voir README.

# 2. Ingestion et qualité des données

## 2.1 Structure des enregistrements
[TOI] Décrire la structure typée choisie (dataclass/namedtuple) et les conversions.

## 2.2 Règles de rejet

Les règles sont appliquées dans l'ordre ci-dessous ; une ligne reçoit le motif de la
première règle qu'elle viole, ce qui garantit valides + rejetées = lignes lues.

| Ordre | Motif | Règle |
|---|---|---|
| 1 | format_invalide | 7 champs non vides attendus |
| 2 | date_invalide | `date_heure` au format `yyyy-MM-dd HH:mm:ss` |
| 3 | station_inconnue | départ et arrivée présents dans `stations.csv` |
| 4 | valeur_non_numerique | durée et distance convertibles en nombre |
| 5 | valeur_non_positive | durée > 0 et distance > 0 |
| 6 | boucle | station de départ ≠ station d'arrivée |
| 7 | doublon_id | `trajet_id` unique (voir règle ci-dessous) |

**Règle des doublons.** Lorsqu'un même `trajet_id` apparaît plusieurs fois, seule la
première occurrence dans l'ordre du fichier (numérotée avec `zipWithIndex`) est conservée.

[TOI] 2-3 phrases : pourquoi cette règle est déterministe, pourquoi elle est préférable
à « garder la date la plus ancienne » ici (les deux T000001 ont un contenu différent).

[TOI] Contrôles supplémentaires faits en exploration (00_exploration.py) sans anomalie
trouvée : champs vides, valeurs d'abonnement, plage de dates (01/01 → 31/03/2026),
vitesses (7,2 à 16,8 km/h).

## 2.3 Bilan des rejets

| Motif | Lignes | Ligne du fichier |
|---|---|---|
| valeur_non_positive | 1 | 24001 (durée = 0) |
| station_inconnue | 1 | 24002 (S99) |
| boucle | 1 | 24003 (S03 → S03) |
| valeur_non_numerique | 1 | 24004 (durée = « inconnu ») |
| doublon_id | 1 | 24005 (T000001, déjà vu ligne 1) |
| **Total rejeté** | **5** | |
| **Trajets valides** | **24 000** | |

![Bilan des rejets](../captures/01_bilan_rejets.png)

## 2.4 Contrôle manuel
[TOI] 3-4 lignes du CSV vérifiées à la main vs résultat du pipeline.

![Contrôle manuel](../captures/02_controle_manuel.png)

# 3. Indicateurs avec l'API RDD

## 3.1 Départs et arrivées par station
[CHIFFRES] + ![Départs/arrivées](../captures/03_rdd_departs_arrivees.png)

## 3.2 Durée moyenne par zone de départ
[CHIFFRES] + [TOI] expliquer le couple (somme, nombre).

## 3.3 Top 10 des couples départ–arrivée
[CHIFFRES] + ![Top 10](../captures/04_rdd_top10.png)

# 4. API DataFrame et couche Parquet

## 4.1 Schéma explicite et indicateurs
![Schéma DataFrame](../captures/05_df_schema.png)

## 4.2 Écriture Parquet partitionnée par mois
![Arborescence Parquet](../captures/06_parquet_partitions.png)

## 4.3 Comparaison RDD / DataFrame
[CHIFFRES] Tableau : critère | RDD | DataFrame (schéma, lisibilité, résultats identiques ?)
[TOI] Conclusion personnelle.

# 5. Plan d'exécution et optimisation

## 5.1 Transformations étroites et larges
[CHIFFRES] Tableau : transformation | type | shuffle ?

## 5.2 Lecture du plan / Spark UI
![Plan physique explain()](../captures/07_explain.png)
![Spark UI - stages](../captures/08_spark_ui.png)
[TOI] Où apparaissent les Exchange ?

## 5.3 groupByKey vs reduceByKey
[CHIFFRES] Tableau : variante | run1 | run2 | run3 | moyenne
[TOI] Interprétation prudente (petit jeu de données), rôle du partitionnement et du cache.

# 6. Graphe orienté du réseau

[TOI] Définition annoncée de « station la plus connectée ».
[CHIFFRES] Degrés entrants/sortants + top 5
![Top 5 graphe](../captures/09_graphe_top5.png)

# 7. Limites et conclusion

[TOI] 6-8 lignes.
