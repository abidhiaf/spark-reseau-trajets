from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.master("local[*]").appName("exploration").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

brut = sc.textFile("data/trajets.csv")
entete = brut.first()
lignes = brut.filter(lambda l: l != entete).map(lambda l: l.split(","))

print("Nb lignes de données :", lignes.count())

# 1) Nombre de colonnes par ligne
print("Colonnes par ligne :", lignes.map(len).countByValue())

# 2) Champs vides par colonne
noms = entete.split(",")
for i, nom in enumerate(noms):
    nb = lignes.filter(lambda c, i=i: len(c) <= i or c[i].strip() == "").count()
    print(f"Vides dans {nom} : {nb}")

# 3) Valeurs de type_abonnement
print("Abonnements :", lignes.filter(lambda c: len(c) == 7).map(lambda c: c[6]).countByValue())

# 4) Stations inconnues
stations = set(sc.textFile("data/stations.csv").filter(lambda l: not l.startswith("station_id"))
               .map(lambda l: l.split(",")[0]).collect())
inconnues = lignes.filter(lambda c: len(c) == 7 and (c[2] not in stations or c[3] not in stations))
print("Stations inconnues :", inconnues.count(), inconnues.take(5))

# 5) Boucles
print("Boucles :", lignes.filter(lambda c: len(c) == 7 and c[2] == c[3]).count())

# 6) Durées / distances non numériques ou <= 0
def pas_positif(v):
    try:
        return float(v) <= 0
    except ValueError:
        return True
print("Durée invalide :", lignes.filter(lambda c: len(c) == 7 and pas_positif(c[4])).take(10))
print("Distance invalide :", lignes.filter(lambda c: len(c) == 7 and pas_positif(c[5])).take(10))

# 7) Dates non parsables + plage de dates
def date_ok(v):
    try:
        datetime.strptime(v, "%Y-%m-%d %H:%M:%S"); return True
    except ValueError:
        return False
print("Dates invalides :", lignes.filter(lambda c: len(c) == 7 and not date_ok(c[1])).take(10))
dates = lignes.filter(lambda c: len(c) == 7 and date_ok(c[1])).map(lambda c: c[1])
print("Date min / max :", dates.min(), "/", dates.max())

# 8) Identifiants en double
doublons = lignes.map(lambda c: (c[0], 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] > 1)
print("IDs en double :", doublons.count(), doublons.take(10))

# 9) Les 5 dernières lignes (lignes de contrôle)
for l in brut.zipWithIndex().filter(lambda x: x[1] >= 24001).collect():
    print(l)

spark.stop()