"""Étape 2 : indicateurs avec l'API RDD (map, flatMap, filter, reduceByKey, join)."""
import csv
import os

from commun import charger_trajets, creer_spark, lire_stations

spark = creer_spark("02_rdd_indicateurs")
sc = spark.sparkContext
os.makedirs("output", exist_ok=True)

stations = lire_stations(sc)
ids_stations = set(stations.map(lambda s: s.station_id).collect())
valides, _ = charger_trajets(sc, ids_stations)
valides = valides.cache()

# (station_id, (nom, zone)) pour les jointures d'enrichissement
infos_stations = stations.map(lambda s: (s.station_id, (s.nom, s.zone)))


def ecrire_csv(chemin, entete, lignes):
    with open(chemin, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(entete)
        w.writerows(lignes)


# 1) Départs et arrivées par station
# flatMap : un trajet produit deux événements ((station, "depart"), 1) et ((station, "arrivee"), 1)
evenements = valides.flatMap(lambda t: [((t.station_depart, "depart"), 1),
                                        ((t.station_arrivee, "arrivee"), 1)])
comptes = evenements.reduceByKey(lambda a, b: a + b)
departs = comptes.filter(lambda kv: kv[0][1] == "depart").map(lambda kv: (kv[0][0], kv[1]))
arrivees = comptes.filter(lambda kv: kv[0][1] == "arrivee").map(lambda kv: (kv[0][0], kv[1]))

par_station = (departs.join(arrivees)                       # (sid, (dep, arr))
                      .join(infos_stations)                 # (sid, ((dep, arr), (nom, zone)))
                      .map(lambda kv: (kv[0], kv[1][1][0], kv[1][1][1], kv[1][0][0], kv[1][0][1]))
                      .sortBy(lambda x: x[0])
                      .collect())

print("=" * 60)
print("1) DÉPARTS ET ARRIVÉES PAR STATION")
print(f"{'station':<8}{'nom':<13}{'zone':>5}{'départs':>10}{'arrivées':>10}")
for sid, nom, zone, dep, arr in par_station:
    print(f"{sid:<8}{nom:<13}{zone:>5}{dep:>10}{arr:>10}")
print(f"Total départs = {sum(x[3] for x in par_station)}, "
      f"total arrivées = {sum(x[4] for x in par_station)}")
ecrire_csv("output/rdd_departs_arrivees.csv",
           ["station_id", "nom", "zone", "nb_departs", "nb_arrivees"], par_station)

# 2) Durée moyenne par zone de départ : on agrège (somme, nombre) puis on divise à la fin
duree_par_zone = (valides.map(lambda t: (t.station_depart, t.duree_min))
                         .join(infos_stations)                           # (sid, (duree, (nom, zone)))
                         .map(lambda kv: (kv[1][1][1], (kv[1][0], 1)))   # (zone, (duree, 1))
                         .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                         .map(lambda kv: (kv[0], kv[1][0], kv[1][1], kv[1][0] / kv[1][1]))
                         .sortBy(lambda x: x[0])
                         .collect())

print("-" * 60)
print("2) DURÉE MOYENNE PAR ZONE DE DÉPART")
print(f"{'zone':<6}{'somme_min':>12}{'nb_trajets':>12}{'moyenne_min':>13}")
for zone, somme, nb, moy in duree_par_zone:
    print(f"{zone:<6}{somme:>12.0f}{nb:>12}{moy:>13.2f}")
ecrire_csv("output/rdd_duree_moyenne_zone.csv",
           ["zone", "somme_duree_min", "nb_trajets", "duree_moyenne_min"],
           [(z, s, n, round(m, 2)) for z, s, n, m in duree_par_zone])

# 3) Top 10 des couples départ -> arrivée
top10 = (valides.map(lambda t: ((t.station_depart, t.station_arrivee), 1))
                .reduceByKey(lambda a, b: a + b)
                .takeOrdered(10, key=lambda kv: (-kv[1], kv[0])))   # égalités : ordre alphabétique

# enrichissement avec les noms : jointure sur le départ puis sur l'arrivée
top10_rdd = sc.parallelize([(dep, (arr, n)) for (dep, arr), n in top10])
top10_noms = (top10_rdd.join(infos_stations)                                  # (dep, ((arr, n), (nom_dep, z)))
                       .map(lambda kv: (kv[1][0][0], (kv[0], kv[1][1][0], kv[1][0][1])))
                       .join(infos_stations)                                  # (arr, ((dep, nom_dep, n), (nom_arr, z)))
                       .map(lambda kv: (kv[1][0][0], kv[1][0][1], kv[0], kv[1][1][0], kv[1][0][2]))
                       .sortBy(lambda x: (-x[4], x[0], x[2]))
                       .collect())

print("-" * 60)
print("3) TOP 10 DES COUPLES DÉPART -> ARRIVÉE")
for rang, (dep, nom_dep, arr, nom_arr, n) in enumerate(top10_noms, 1):
    print(f"{rang:>2}. {dep} ({nom_dep}) -> {arr} ({nom_arr}) : {n} trajets")
print("=" * 60)
ecrire_csv("output/rdd_top10_couples.csv",
           ["rang", "station_depart", "nom_depart", "station_arrivee", "nom_arrivee", "nb_trajets"],
           [(i, *row) for i, row in enumerate(top10_noms, 1)])

spark.stop()
