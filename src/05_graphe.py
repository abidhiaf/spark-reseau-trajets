"""Étape 5 : graphe orienté du réseau (implémentation RDD).

Sommets = stations ; arc (départ -> arrivée) pondéré par le nombre de trajets.
"""
import csv

from commun import charger_trajets, creer_spark, lire_stations

spark = creer_spark("05_graphe")
sc = spark.sparkContext

stations = lire_stations(sc)
ids_stations = set(stations.map(lambda s: s.station_id).collect())
valides, _ = charger_trajets(sc, ids_stations)

sommets = stations.map(lambda s: (s.station_id, s.nom))
arcs = (valides.map(lambda t: ((t.station_depart, t.station_arrivee), 1))
               .reduceByKey(lambda a, b: a + b)
               .cache())                                   # ((src, dst), poids)

nb_arcs = arcs.count()
nb_sommets = sommets.count()
print("=" * 70)
print(f"GRAPHE : {nb_sommets} sommets, {nb_arcs} arcs orientés "
      f"(maximum possible sans boucle : {nb_sommets * (nb_sommets - 1)})")
print(f"Poids total des arcs = {arcs.map(lambda kv: kv[1]).sum()} trajets")

# Degrés simples (nombre de voisins distincts) et degrés pondérés (nombre de trajets)
deg_sortant = arcs.map(lambda kv: (kv[0][0], (1, kv[1]))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
deg_entrant = arcs.map(lambda kv: (kv[0][1], (1, kv[1]))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

degres = (sommets.join(deg_sortant).join(deg_entrant)
          # (sid, ((nom, (out, out_w)), (in, in_w)))
          .map(lambda kv: (kv[0], kv[1][0][0], kv[1][1][0], kv[1][0][1][0],
                           kv[1][1][1], kv[1][0][1][1]))
          # (sid, nom, degre_in, degre_out, in_pondere, out_pondere)
          .map(lambda x: x + (x[4] + x[5],))
          .sortBy(lambda x: x[0])
          .collect())

print("-" * 70)
print("DEGRÉS PAR STATION (non pondérés = voisins distincts ; pondérés = trajets)")
print(f"{'station':<8}{'deg_in':>8}{'deg_out':>9}{'in_pondéré':>12}{'out_pondéré':>13}{'total':>8}")
for sid, nom, d_in, d_out, w_in, w_out, total in degres:
    print(f"{sid:<8}{d_in:>8}{d_out:>9}{w_in:>12}{w_out:>13}{total:>8}")

tous_complets = all(d[2] == nb_sommets - 1 and d[3] == nb_sommets - 1 for d in degres)
print(f"Toutes les stations ont deg_in = deg_out = {nb_sommets - 1} : {tous_complets}")

# Définition annoncée : station la plus connectée = degré total pondéré (entrants + sortants)
# Égalités départagées par le degré non pondéré, puis par l'identifiant.
top5 = sorted(degres, key=lambda x: (-x[6], -(x[2] + x[3]), x[0]))[:5]
print("-" * 70)
print("TOP 5 DES STATIONS LES PLUS CONNECTÉES (degré total pondéré = trajets entrants + sortants)")
for rang, (sid, nom, d_in, d_out, w_in, w_out, total) in enumerate(top5, 1):
    print(f"{rang}. {sid} ({nom}) : {total} trajets  (entrants {w_in}, sortants {w_out})")

print("-" * 70)
print("ARCS LES PLUS LOURDS (5 premiers)")
for (src, dst), poids in arcs.takeOrdered(5, key=lambda kv: (-kv[1], kv[0])):
    print(f"  {src} -> {dst} : {poids}")
print("=" * 70)

with open("output/graphe_degres.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["station_id", "nom", "degre_entrant", "degre_sortant",
                "degre_entrant_pondere", "degre_sortant_pondere", "degre_total_pondere"])
    w.writerows(degres)
with open("output/graphe_top5.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["rang", "station_id", "nom", "degre_total_pondere", "entrants", "sortants"])
    w.writerows([(i, s[0], s[1], s[6], s[4], s[5]) for i, s in enumerate(top5, 1)])
with open("output/graphe_arcs.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["source", "destination", "poids"])
    w.writerows(sorted((s, d, p) for (s, d), p in arcs.collect()))

spark.stop()
