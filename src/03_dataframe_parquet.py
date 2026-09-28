"""Étape 3 : mêmes indicateurs avec l'API DataFrame (schéma explicite) + écriture Parquet."""
import csv
import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from commun import creer_spark

spark = creer_spark("03_dataframe_parquet")
SORTIE_PARQUET = "output/parquet/trajets"

schema_stations = StructType([
    StructField("station_id", StringType(), False),
    StructField("nom", StringType(), False),
    StructField("zone", IntegerType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])
schema_trajets = StructType([
    StructField("trajet_id", StringType(), True),
    StructField("date_heure", TimestampType(), True),
    StructField("station_depart", StringType(), True),
    StructField("station_arrivee", StringType(), True),
    StructField("duree_min", DoubleType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("type_abonnement", StringType(), True),
])

stations = spark.read.csv("data/stations.csv", header=True, schema=schema_stations)
# mode PERMISSIVE : une valeur non convertible (ex. durée "inconnu") devient null
brut = (spark.read.csv("data/trajets.csv", header=True, schema=schema_trajets,
                       timestampFormat="yyyy-MM-dd HH:mm:ss", mode="PERMISSIVE")
             .withColumn("num_ligne", F.monotonically_increasing_id()))

print("=" * 60)
print("SCHÉMA EXPLICITE DES TRAJETS")
brut.printSchema()

# Mêmes règles que la version RDD, écrites avec des colonnes
ids = stations.select("station_id")
valides_regles = (brut
    .dropna(subset=["trajet_id", "date_heure", "station_depart", "station_arrivee",
                    "duree_min", "distance_km", "type_abonnement"])
    .join(ids.withColumnRenamed("station_id", "station_depart"), "station_depart", "left_semi")
    .join(ids.withColumnRenamed("station_id", "station_arrivee"), "station_arrivee", "left_semi")
    .filter((F.col("duree_min") > 0) & (F.col("distance_km") > 0))
    .filter(F.col("station_depart") != F.col("station_arrivee"))
    .filter((F.col("date_heure") >= "2026-01-01") & (F.col("date_heure") < "2026-04-01"))
    .filter(~F.isnan("duree_min") & ~F.isnan("distance_km"))
    .filter(F.col("type_abonnement").isin("annuel", "mensuel", "occasionnel")))

# Doublons : première occurrence dans l'ordre du fichier (num_ligne croissant)
w = Window.partitionBy("trajet_id").orderBy("num_ligne")
valides = (valides_regles.withColumn("rang", F.row_number().over(w))
                         .filter(F.col("rang") == 1)
                         .drop("rang", "num_ligne")
                         .withColumn("mois", F.date_format("date_heure", "yyyy-MM"))
                         .cache())

print(f"Lignes lues : {brut.count()}  |  trajets valides : {valides.count()}")

# Indicateur 1 : départs et arrivées par station
departs = valides.groupBy(F.col("station_depart").alias("station_id")).agg(F.count("*").alias("nb_departs"))
arrivees = valides.groupBy(F.col("station_arrivee").alias("station_id")).agg(F.count("*").alias("nb_arrivees"))
df_dep_arr = (stations.select("station_id", "nom", "zone")
                      .join(departs, "station_id").join(arrivees, "station_id")
                      .orderBy("station_id"))

# Indicateur 2 : durée moyenne par zone de départ
df_zone = (valides.join(stations.select(F.col("station_id").alias("station_depart"), "zone"), "station_depart")
                  .groupBy("zone")
                  .agg(F.sum("duree_min").alias("somme_duree_min"),
                       F.count("*").alias("nb_trajets"),
                       F.round(F.avg("duree_min"), 2).alias("duree_moyenne_min"))
                  .orderBy("zone"))

# Indicateur 3 : top 10 des couples, enrichi avec les noms
noms_dep = stations.select(F.col("station_id").alias("station_depart"), F.col("nom").alias("nom_depart"))
noms_arr = stations.select(F.col("station_id").alias("station_arrivee"), F.col("nom").alias("nom_arrivee"))
df_top10 = (valides.groupBy("station_depart", "station_arrivee").agg(F.count("*").alias("nb_trajets"))
                   .orderBy(F.desc("nb_trajets"), "station_depart", "station_arrivee")
                   .limit(10)
                   .join(noms_dep, "station_depart").join(noms_arr, "station_arrivee")
                   .select("station_depart", "nom_depart", "station_arrivee", "nom_arrivee", "nb_trajets")
                   .orderBy(F.desc("nb_trajets"), "station_depart", "station_arrivee"))

print("-" * 60)
print("DATAFRAME - DÉPARTS / ARRIVÉES (5 premières stations)")
df_dep_arr.show(5)
print("DATAFRAME - DURÉE MOYENNE PAR ZONE")
df_zone.show()
print("DATAFRAME - TOP 10 DES COUPLES")
df_top10.show(truncate=False)


# Comparaison automatique avec les résultats RDD de l'étape 2
def lire_csv(chemin):
    with open(chemin) as f:
        return list(csv.DictReader(f))


rdd_dep_arr = {r["station_id"]: (int(r["nb_departs"]), int(r["nb_arrivees"]))
               for r in lire_csv("output/rdd_departs_arrivees.csv")}
df_dep_arr_d = {r.station_id: (r.nb_departs, r.nb_arrivees) for r in df_dep_arr.collect()}
rdd_zone = {int(r["zone"]): float(r["duree_moyenne_min"]) for r in lire_csv("output/rdd_duree_moyenne_zone.csv")}
df_zone_d = {r.zone: r.duree_moyenne_min for r in df_zone.collect()}
rdd_top = [(r["station_depart"], r["station_arrivee"], int(r["nb_trajets"]))
           for r in lire_csv("output/rdd_top10_couples.csv")]
df_top = [(r.station_depart, r.station_arrivee, r.nb_trajets) for r in df_top10.collect()]

print("-" * 60)
print("COMPARAISON RDD / DATAFRAME")
print(f"  Départs/arrivées identiques : {rdd_dep_arr == df_dep_arr_d}")
print(f"  Durée moyenne par zone identique : {rdd_zone == df_zone_d}")
print(f"  Top 10 identique : {rdd_top == df_top}")

# Écriture Parquet partitionnée par mois
valides.write.mode("overwrite").partitionBy("mois").parquet(SORTIE_PARQUET)
relu = spark.read.parquet(SORTIE_PARQUET)
print("-" * 60)
print(f"PARQUET écrit dans {SORTIE_PARQUET}")
for d in sorted(os.listdir(SORTIE_PARQUET)):
    if d.startswith("mois="):
        print(f"  {d}/")
print("Relecture du Parquet - trajets par mois :")
relu.groupBy("mois").count().orderBy("mois").show()
print("=" * 60)

spark.stop()
