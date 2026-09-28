"""Étape 4 : plans d'exécution, groupByKey vs reduceByKey, partitionnement et cache.

Usage : python src/04_plan_benchmark.py [--pause]
  --pause : garde Spark ouvert à la fin pour consulter l'interface http://localhost:4040
"""
import csv
import statistics
import sys
import time

from pyspark.sql import functions as F

from commun import charger_trajets, creer_spark, lire_stations

NB_RUNS = 5

spark = creer_spark("04_plan_benchmark")
sc = spark.sparkContext

stations = lire_stations(sc)
ids_stations = set(stations.map(lambda s: s.station_id).collect())
valides, _ = charger_trajets(sc, ids_stations)
valides = valides.cache()
print(f"Trajets valides (mis en cache) : {valides.count()}")
print(f"Partitions du RDD valides : {valides.getNumPartitions()}")


def chronometrer(nom, fonction):
    """Exécute la fonction NB_RUNS fois (après un run d'échauffement) et renvoie les temps."""
    fonction()
    temps = []
    for _ in range(NB_RUNS):
        t0 = time.perf_counter()
        fonction()
        temps.append(time.perf_counter() - t0)
    print(f"  {nom:<38} " + " ".join(f"{t:6.3f}" for t in temps)
          + f"  | médiane {statistics.median(temps):.3f} s")
    return nom, temps


# ---------------------------------------------------------------------------
# 1) Plan d'exécution RDD : lignée et frontières de stage
# ---------------------------------------------------------------------------
departs = (valides.map(lambda t: (t.station_depart, 1))        # étroite
                  .filter(lambda kv: kv[0].startswith("S"))     # étroite
                  .reduceByKey(lambda a, b: a + b))             # large (shuffle)
print("=" * 70)
print("1) LIGNÉE RDD (toDebugString) - départs par station")
print(departs.toDebugString().decode())

# ---------------------------------------------------------------------------
# 2) Plan physique DataFrame : repérer les Exchange (= shuffles)
# ---------------------------------------------------------------------------
df_trajets = spark.read.parquet("output/parquet/trajets")
df_stations = spark.read.csv("data/stations.csv", header=True, inferSchema=True)
df_zone = (df_trajets.join(df_stations.select(F.col("station_id").alias("station_depart"), "zone"),
                           "station_depart")
                     .groupBy("zone").agg(F.avg("duree_min").alias("duree_moyenne")))
print("=" * 70)
print("2) PLAN PHYSIQUE DATAFRAME - durée moyenne par zone")
df_zone.explain(mode="formatted")

# ---------------------------------------------------------------------------
# 3) Benchmark
# ---------------------------------------------------------------------------
paires = valides.map(lambda t: (t.station_depart, t.duree_min)).cache()
paires.count()


def avec_group_by_key():
    # toutes les durées de chaque station traversent le réseau, puis sont additionnées
    return paires.groupByKey().mapValues(lambda v: (sum(v), len(v))).collect()


def avec_reduce_by_key():
    # chaque partition pré-agrège (somme, nombre) avant le shuffle (combiner côté map)
    return (paires.mapValues(lambda d: (d, 1))
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                  .collect())


assert sorted(avec_group_by_key()) == sorted(avec_reduce_by_key()), "résultats différents !"

print("=" * 70)
print(f"3) TEMPS D'EXÉCUTION ({NB_RUNS} runs après 1 run d'échauffement, en secondes)")
resultats = []

print("a) groupByKey vs reduceByKey (somme et nombre de durées par station)")
resultats.append(chronometrer("RDD groupByKey", avec_group_by_key))
resultats.append(chronometrer("RDD reduceByKey", avec_reduce_by_key))

print("b) Partitionnement du shuffle DataFrame (spark.sql.shuffle.partitions)")
for nb in (200, 8):
    spark.conf.set("spark.sql.shuffle.partitions", nb)
    resultats.append(chronometrer(f"DataFrame groupBy - {nb} partitions",
                                  lambda: df_zone.collect()))
spark.conf.set("spark.sql.shuffle.partitions", 200)

print("c) Cache : deux indicateurs calculés sur le même RDD")


def deux_indicateurs(rdd):
    rdd.map(lambda t: (t.station_depart, 1)).reduceByKey(lambda a, b: a + b).collect()
    rdd.map(lambda t: ((t.station_depart, t.station_arrivee), 1)).reduceByKey(lambda a, b: a + b).collect()


valides_sans_cache, _ = charger_trajets(sc, ids_stations)
resultats.append(chronometrer("Sans cache (relit et revalide le CSV)",
                              lambda: deux_indicateurs(valides_sans_cache)))
resultats.append(chronometrer("Avec cache", lambda: deux_indicateurs(valides)))

with open("output/benchmark.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["variante"] + [f"run{i}" for i in range(1, NB_RUNS + 1)] + ["mediane_s"])
    for nom, temps in resultats:
        w.writerow([nom] + [round(t, 3) for t in temps] + [round(statistics.median(temps), 3)])
print("Temps enregistrés dans output/benchmark.csv")
print("=" * 70)

if "--pause" in sys.argv:
    print("Interface Spark : http://localhost:4040  (onglets Jobs, Stages, SQL)")
    input("Fais tes captures, puis appuie sur Entrée pour terminer... ")

spark.stop()
