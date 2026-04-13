#!/usr/bin/env python3
"""
Met à jour data/marges_2022_2026.csv avec la semaine en cours.
Appelé par GitHub Actions chaque lundi matin.
"""

import io
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

LITRES_PAR_BARIL = 158.987
LITRES_PAR_TONNE = 1163

# TICPE fixe depuis 2020 (aucune modification législative depuis)
TICPE = {
    "Gazole":   0.5974,
    "SP95-E10": 0.6582,
    "SP98":     0.6937,
}

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"
ECB_URL  = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata&startPeriod=2022-01-01"
DGEC_URL = (
    "https://www.ecologie.gouv.fr/sites/default/files/documents/"
    "Historique%20de%20la%20marge%20brute%20de%20raffinage%20sur%20Brent%20depuis%202015"
    "%20%28moyennes%20mensuelles%29.xlsx"
)
PRIX_URL     = "https://donnees.roulez-eco.fr/opendata/instantane"
PRIX_GOV_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json?limit=-1&timezone=Europe%2FParis"

HEADERS = {"User-Agent": "data-carburant-bot/1.0 (https://github.com/NicolasBrnd/transparence-carburant-streamlit)"}

CSV_PATH = Path(__file__).parent.parent / "data" / "marges_2022_2026.csv"


def get_semaine() -> date:
    """Retourne le lundi de la semaine en cours."""
    today = date.today()
    return today - timedelta(days=today.weekday())


def _parse_zip_xml(content: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        with z.open(z.namelist()[0]) as f:
            tree = ET.parse(f)
    root = tree.getroot()
    prix = {"Gazole": [], "SP95-E10": [], "SP98": []}
    n_stations = 0
    for station in root.findall("pdv"):
        if station.get("type") == "A":
            continue
        n_stations += 1
        for el in station.findall("prix"):
            nom = el.get("nom", "")
            val = el.get("valeur", "")
            if nom in prix and val:
                try:
                    p = float(val) / 1000
                    if 0.8 <= p <= 4.0:
                        prix[nom].append(p)
                except Exception:
                    pass
    print(f"  {n_stations} stations analysées")
    return {k: round(sum(v) / len(v), 6) for k, v in prix.items() if v}


def _fetch_via_gov_api() -> dict:
    """Fallback : API REST officielle data.economie.gouv.fr."""
    print("  Utilisation de l'API gouvernement (fallback)...")
    r = requests.get(PRIX_GOV_URL, timeout=60, headers=HEADERS)
    r.raise_for_status()
    records = r.json()
    mapping = {
        "Gazole":   "gazole_prix",
        "SP95-E10": "e10_prix",
        "SP98":     "sp98_prix",
    }
    prix = {"Gazole": [], "SP95-E10": [], "SP98": []}
    for rec in records:
        if rec.get("type_de_vente") == "A":
            continue
        for carb, field in mapping.items():
            val = rec.get(field)
            if val is not None:
                try:
                    p = float(val)
                    if 0.8 <= p <= 4.0:
                        prix[carb].append(p)
                except Exception:
                    pass
    print(f"  {len(records)} stations via API gouvernement")
    return {k: round(sum(v) / len(v), 6) for k, v in prix.items() if v}


def fetch_prix_pompe() -> dict:
    try:
        r = requests.get(PRIX_URL, timeout=90, stream=True, headers=HEADERS)
        r.raise_for_status()
        content = b""
        for chunk in r.iter_content(chunk_size=256 * 1024):
            content += chunk
            if len(content) > 20 * 1024 * 1024:
                break
        result = _parse_zip_xml(content)
        if result:
            return result
        print("  roulez-eco.fr: aucun prix trouvé dans le ZIP")
    except Exception as e:
        print(f"  roulez-eco.fr indisponible: {e}")

    return _fetch_via_gov_api()


def fetch_brent_eur_litre() -> float:
    r_brent = requests.get(FRED_URL, timeout=30)
    r_brent.raise_for_status()
    df_b = pd.read_csv(io.StringIO(r_brent.text))
    df_b.columns = ["DATE", "BRENT_USD"]
    df_b["DATE"] = pd.to_datetime(df_b["DATE"])
    df_b["BRENT_USD"] = pd.to_numeric(df_b["BRENT_USD"], errors="coerce")
    df_b = df_b.dropna().tail(7)

    r_ecb = requests.get(ECB_URL, timeout=30)
    r_ecb.raise_for_status()
    df_e = pd.read_csv(io.StringIO(r_ecb.text))
    df_e = df_e[["TIME_PERIOD", "OBS_VALUE"]].copy()
    df_e.columns = ["DATE", "OBS"]
    df_e["DATE"] = pd.to_datetime(df_e["DATE"])
    df_e["OBS"] = pd.to_numeric(df_e["OBS"], errors="coerce")
    df_e = df_e.dropna().tail(7)
    df_e["TAUX"] = 1.0 / df_e["OBS"]

    merged = pd.merge(df_b, df_e[["DATE", "TAUX"]], on="DATE", how="inner")
    if merged.empty:
        raise ValueError("Aucune donnée Brent/EUR-USD en commun")

    brent_usd = merged["BRENT_USD"].mean()
    taux = merged["TAUX"].mean()
    return round((brent_usd * taux) / LITRES_PAR_BARIL, 6)


def fetch_raffinage() -> float:
    r = requests.get(DGEC_URL, timeout=30)
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), header=None)
    resultats = {}
    for _, row in df.iloc[2:].iterrows():
        try:
            dt = pd.to_datetime(row[0])
            marge = float(row[2]) / LITRES_PAR_TONNE
            resultats[dt.strftime("%Y-%m")] = round(marge, 6)
        except Exception:
            continue
    mois = date.today().strftime("%Y-%m")
    mois_prec = (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    val = resultats.get(mois) or resultats.get(mois_prec)
    if val is None:
        raise ValueError("Marge raffinage introuvable dans le fichier DGEC")
    return val


def main():
    semaine = get_semaine()
    print(f"Semaine cible : {semaine}")

    df = pd.read_csv(CSV_PATH, parse_dates=["semaine"])

    if (df["semaine"] == pd.Timestamp(semaine)).any():
        print(f"Semaine {semaine} déjà présente — rien à faire.")
        return

    print("Récupération Brent + EUR/USD (FRED + BCE)...")
    brent = fetch_brent_eur_litre()
    print(f"  Brent : {brent} €/L")

    print("Récupération marge raffinage (DGEC)...")
    raffinage = fetch_raffinage()
    print(f"  Raffinage : {raffinage} €/L")

    print("Récupération prix pompe (prix-carburants.gouv.fr)...")
    prix_pompe = fetch_prix_pompe()
    print(f"  Prix pompe : {prix_pompe}")

    nouvelles_lignes = []
    for carburant in ["Gazole", "SP95-E10", "SP98"]:
        prix = prix_pompe.get(carburant)
        if not prix:
            print(f"  Prix manquant pour {carburant}, ignoré.")
            continue
        ticpe = TICPE[carburant]
        tva = round(prix - prix / 1.2, 6)
        distribution = round(prix / 1.2 - brent - raffinage - ticpe, 6)
        nouvelles_lignes.append({
            "semaine":      semaine,
            "carburant":    carburant,
            "prix":         prix,
            "brent":        brent,
            "raffinage":    raffinage,
            "ticpe":        ticpe,
            "tva":          tva,
            "distribution": distribution,
        })

    if not nouvelles_lignes:
        print("Aucune ligne à ajouter — prix pompe indisponibles, semaine ignorée.")
        sys.exit(0)

    df_new = pd.DataFrame(nouvelles_lignes)
    df_updated = pd.concat([df, df_new], ignore_index=True)
    df_updated["semaine"] = pd.to_datetime(df_updated["semaine"])
    df_updated = df_updated.sort_values(["semaine", "carburant"]).reset_index(drop=True)
    df_updated.to_csv(CSV_PATH, index=False, date_format="%Y-%m-%d")

    print(f"\nOK — {len(nouvelles_lignes)} lignes ajoutées pour la semaine du {semaine}.")


if __name__ == "__main__":
    main()
