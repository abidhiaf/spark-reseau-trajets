"""Étape 1 : ingestion, contrôles qualité et bilan des rejets."""
import csv
import os

from commun import MOTIFS, charger_trajets, creer_spark, lire_stations

spark = creer_spark("01_ingestion")
sc = spark.sparkContext
os.makedirs("output", exist_ok=True)

stations = lire_stations(sc)
ids_stations = set(stations.map(lambda s: s.station_id).collect())
valides, rejets = charger_trajets(sc, ids_stations)

nb_brut = sc.textFile("data/trajets.csv").count() - 1
nb_valides = valides.count()
comptes = dict(rejets.map(lambda r: (r[0], 1)).reduceByKey(lambda a, b: a + b).collect())
bilan = [(motif, comptes.get(motif, 0)) for motif in MOTIFS]   # ordre des règles, zéros inclus
nb_rejets = sum(n for _, n in bilan)

print("=" * 50)
print(f"Stations chargées       : {len(ids_stations)}")
print(f"Lignes de données       : {nb_brut}")
print(f"Trajets valides         : {nb_valides}")
print(f"Lignes rejetées         : {nb_rejets}")
print(f"Contrôle valides+rejets : {nb_valides + nb_rejets} (= {nb_brut} ?)")
print("-" * 50)
print("BILAN DES REJETS PAR MOTIF (dans l'ordre d'application des règles)")
for motif, n in bilan:
    print(f"  {motif:<22} {n}")
print("-" * 50)
print("DÉTAIL DES LIGNES REJETÉES")
for motif, num, ligne in sorted(rejets.collect(), key=lambda r: r[1]):
    print(f"  ligne {num:>5} | {motif:<20} | {ligne}")
print("=" * 50)

with open("output/bilan_rejets.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["motif", "nb_lignes"])
    w.writerows(bilan)
with open("output/rejets.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["motif", "numero_ligne", "ligne_brute"])
    w.writerows(sorted(rejets.collect(), key=lambda r: r[1]))

spark.stop()
