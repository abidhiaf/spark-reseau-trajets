"""Fonctions partagées : lecture des CSV, structure typée et règles de qualité."""
import math
import os
import sys
from collections import namedtuple
from datetime import datetime

Station = namedtuple("Station", ["station_id", "nom", "zone", "latitude", "longitude"])
Trajet = namedtuple("Trajet", ["trajet_id", "date_heure", "station_depart", "station_arrivee",
                               "duree_min", "distance_km", "type_abonnement"])

FORMAT_DATE = "%Y-%m-%d %H:%M:%S"
DEBUT_PERIODE = datetime(2026, 1, 1)
FIN_PERIODE = datetime(2026, 4, 1)          # borne exclue : trajets de janvier à mars 2026
ABONNEMENTS = {"annuel", "mensuel", "occasionnel"}
MOTIFS = ["format_invalide", "date_invalide", "hors_periode", "station_inconnue",
          "valeur_non_numerique", "valeur_non_positive", "boucle", "abonnement_inconnu",
          "doublon_id"]


def creer_spark(nom):
    """SparkSession locale ; driver et workers utilisent le même Python (celui du venv)."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[*]").appName(nom)
                         .config("spark.ui.showConsoleProgress", "false")
                         .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addPyFile(os.path.join(os.path.dirname(__file__), "commun.py"))
    return spark


def lire_stations(sc, chemin="data/stations.csv"):
    """RDD de Station (en-tête retiré, zone et coordonnées converties)."""
    brut = sc.textFile(chemin)
    entete = brut.first()

    def parser(ligne):
        c = ligne.split(",")
        return Station(c[0], c[1], int(c[2]), float(c[3]), float(c[4]))

    return brut.filter(lambda l: l != entete).map(parser)


def valider(ligne, ids_stations):
    """Renvoie ("OK", Trajet) ou ("REJET", motif).
    Les règles sont testées dans un ordre fixe : une ligne reçoit un seul motif."""
    c = ligne.split(",")
    if len(c) != 7 or any(v.strip() == "" for v in c):
        return ("REJET", "format_invalide")
    tid, date_txt, dep, arr, duree_txt, dist_txt, abo = c
    try:
        date = datetime.strptime(date_txt, FORMAT_DATE)
    except ValueError:
        return ("REJET", "date_invalide")
    if not DEBUT_PERIODE <= date < FIN_PERIODE:
        return ("REJET", "hors_periode")
    if dep not in ids_stations or arr not in ids_stations:
        return ("REJET", "station_inconnue")
    try:
        duree, dist = float(duree_txt), float(dist_txt)
    except ValueError:
        return ("REJET", "valeur_non_numerique")
    if not (math.isfinite(duree) and math.isfinite(dist)):   # "nan", "inf" passent float()
        return ("REJET", "valeur_non_numerique")
    if duree <= 0 or dist <= 0:
        return ("REJET", "valeur_non_positive")
    if dep == arr:
        return ("REJET", "boucle")
    if abo not in ABONNEMENTS:
        return ("REJET", "abonnement_inconnu")
    return ("OK", Trajet(tid, date, dep, arr, duree, dist, abo))


def charger_trajets(sc, ids_stations, chemin="data/trajets.csv"):
    """Renvoie (valides, rejets).
    valides : RDD de Trajet ; rejets : RDD de (motif, numero_ligne, ligne_brute)."""
    brut = sc.textFile(chemin)
    entete = brut.first()
    ids_bc = sc.broadcast(ids_stations)

    # numéro de ligne dans le fichier (1 = première ligne de données) -> ordre déterministe
    numerotees = (brut.filter(lambda l: l != entete)
                      .zipWithIndex()
                      .map(lambda x: (x[1] + 1, x[0])))
    controlees = numerotees.map(lambda x: (x[0], x[1], valider(x[1], ids_bc.value))).cache()

    rejets_regles = (controlees.filter(lambda x: x[2][0] == "REJET")
                              .map(lambda x: (x[2][1], x[0], x[1])))
    ok = controlees.filter(lambda x: x[2][0] == "OK").map(lambda x: (x[0], x[1], x[2][1]))

    # Doublons : on garde la première occurrence du trajet_id dans le fichier
    premiere = ok.map(lambda x: (x[2].trajet_id, x[0])).reduceByKey(min)
    avec_premiere = ok.map(lambda x: (x[2].trajet_id, x)).join(premiere)
    valides = avec_premiere.filter(lambda kv: kv[1][0][0] == kv[1][1]).map(lambda kv: kv[1][0][2])
    rejets_doublons = (avec_premiere.filter(lambda kv: kv[1][0][0] != kv[1][1])
                                    .map(lambda kv: ("doublon_id", kv[1][0][0], kv[1][0][1])))

    return valides, rejets_regles.union(rejets_doublons)
