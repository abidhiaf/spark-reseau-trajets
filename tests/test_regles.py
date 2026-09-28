"""Tests unitaires des règles de qualité (sans Spark) : python tests/test_regles.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from commun import valider  # noqa: E402

IDS = {"S01", "S02", "S03"}
OK = "T1,2026-02-01 10:00:00,S01,S02,15,3.2,annuel"

CAS = [
    ("ligne correcte", OK, None),
    ("6 champs", "T1,2026-02-01 10:00:00,S01,S02,15,3.2", "format_invalide"),
    ("champ vide", "T1,2026-02-01 10:00:00,S01,,15,3.2,annuel", "format_invalide"),
    ("date illisible", "T1,01/02/2026 10:00,S01,S02,15,3.2,annuel", "date_invalide"),
    ("date avant la période", "T1,2025-12-31 23:59:59,S01,S02,15,3.2,annuel", "hors_periode"),
    ("date après la période", "T1,2026-04-01 00:00:00,S01,S02,15,3.2,annuel", "hors_periode"),
    ("station inconnue", "T1,2026-02-01 10:00:00,S01,S99,15,3.2,annuel", "station_inconnue"),
    ("durée texte", "T1,2026-02-01 10:00:00,S01,S02,inconnu,3.2,annuel", "valeur_non_numerique"),
    ("durée NaN", "T1,2026-02-01 10:00:00,S01,S02,nan,3.2,annuel", "valeur_non_numerique"),
    ("distance infinie", "T1,2026-02-01 10:00:00,S01,S02,15,inf,annuel", "valeur_non_numerique"),
    ("durée nulle", "T1,2026-02-01 10:00:00,S01,S02,0,3.2,annuel", "valeur_non_positive"),
    ("distance négative", "T1,2026-02-01 10:00:00,S01,S02,15,-1,annuel", "valeur_non_positive"),
    ("boucle", "T1,2026-02-01 10:00:00,S03,S03,15,3.2,annuel", "boucle"),
    ("abonnement inconnu", "T1,2026-02-01 10:00:00,S01,S02,15,3.2,gratuit", "abonnement_inconnu"),
    ("ordre : station avant valeur", "T1,2026-02-01 10:00:00,S01,S99,0,3.2,annuel", "station_inconnue"),
]

echecs = 0
for nom, ligne, attendu in CAS:
    statut, info = valider(ligne, IDS)
    obtenu = None if statut == "OK" else info
    ok = obtenu == attendu
    echecs += not ok
    print(f"{'OK ' if ok else 'KO '} {nom:<30} attendu={attendu} obtenu={obtenu}")
print(f"\n{len(CAS) - echecs}/{len(CAS)} tests réussis")
sys.exit(1 if echecs else 0)
