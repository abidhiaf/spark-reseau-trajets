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
| Spark / PySpark | 3.5.3 |
| Python | 3.12 |
| Java | 17.0.12 |
| Mode d'exécution | local[*] (macOS) |

Commande de lancement : voir README.

# 2. Ingestion et qualité des données

## 2.1 Structure des enregistrements
[TOI] Décrire la structure typée choisie (dataclass/namedtuple) et les conversions.

## 2.2 Règles de rejet
[CHIFFRES] Tableau : motif | règle | justification

[TOI] Justifier la règle déterministe pour les doublons.

## 2.3 Bilan des rejets
[CHIFFRES] Tableau : motif | nombre de lignes

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
