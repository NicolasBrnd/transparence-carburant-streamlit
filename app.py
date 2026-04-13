"""
Transparence Carburant
Le prix du carburant, composante par composante.
Données publiques officielles depuis 2015.
"""

import streamlit as st
import pandas as pd
import requests
import io
import zipfile
import xml.etree.ElementTree as ET
import plotly.graph_objects as go
from datetime import date

# ─── CONFIG ──────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Transparence Carburant",
    page_icon="⛽",
    layout="wide",
    menu_items={
        "Report a bug": "https://github.com/NicolasBrnd/transparence-carburant-streamlit/issues",
        "About": "Données publiques officielles — prix-carburants.gouv.fr · FRED · DGEC",
    }
)

# ─── CONSTANTES ──────────────────────────────────────────────────────────────

LITRES_PAR_BARIL = 158.987
LITRES_PAR_TONNE = 1163

TICPE = {
    "Gazole":   {2015: 0.4284, 2016: 0.4596, 2017: 0.4896, 2018: 0.5390, 2019: 0.5974, 2020: 0.5974},
    "SP95-E10": {2015: 0.6139, 2016: 0.6390, 2017: 0.6492, 2018: 0.6512, 2019: 0.6582, 2020: 0.6582},
    "SP98":     {2015: 0.6469, 2016: 0.6720, 2017: 0.6822, 2018: 0.6842, 2019: 0.6937, 2020: 0.6937},
}

COULEURS = {
    "Pétrole brut": "#f59e0b",
    "Raffinage":    "#f97316",
    "Distribution": "#3b82f6",
    "Taxes":        "#1e293b",
}

FRED_URL   = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"
ECB_URL    = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata&startPeriod=2022-01-01"
DGEC_MARGE = (
    "https://www.ecologie.gouv.fr/sites/default/files/documents/"
    "Historique%20de%20la%20marge%20brute%20de%20raffinage%20sur%20Brent%20depuis%202015"
    "%20%28moyennes%20mensuelles%29.xlsx"
)
PRIX_URL   = "https://donnees.roulez-eco.fr/opendata/instantane"

# ─── DATA FETCHING ───────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def load_historique():
    df = pd.read_csv("data/marges_historiques.csv", parse_dates=["semaine"])
    agg = df.groupby(["semaine", "carburant"]).agg(
        prix=("prix_moyen", "mean"),
        brent=("brent_eur_litre", "mean"),
        raffinage=("marge_raffinage_eur_litre", "mean"),
        ticpe=("ticpe_eur_litre", "mean"),
        distribution=("marge_distribution", "mean"),
    ).reset_index()
    agg["tva"] = agg["prix"] - agg["prix"] / 1.2

    df2 = pd.read_csv("data/marges_2022_2026.csv", parse_dates=["semaine"])

    combined = pd.concat([agg, df2], ignore_index=True)
    combined = combined.drop_duplicates(subset=["semaine", "carburant"], keep="last")
    combined = combined.sort_values(["semaine", "carburant"]).reset_index(drop=True)
    return combined


@st.cache_data(ttl=3600)
def fetch_brent_eurusd():
    try:
        r_brent = requests.get(FRED_URL, timeout=30)
        df_b = pd.read_csv(io.StringIO(r_brent.text))
        df_b.columns = ["DATE", "BRENT_USD"]
        df_b["DATE"] = pd.to_datetime(df_b["DATE"])
        df_b["BRENT_USD"] = pd.to_numeric(df_b["BRENT_USD"], errors="coerce")
        df_b = df_b.dropna().tail(30)

        r_ecb = requests.get(ECB_URL, timeout=30)
        df_e = pd.read_csv(io.StringIO(r_ecb.text))
        df_e = df_e[["TIME_PERIOD", "OBS_VALUE"]].copy()
        df_e.columns = ["DATE", "OBS"]
        df_e["DATE"] = pd.to_datetime(df_e["DATE"])
        df_e["OBS"] = pd.to_numeric(df_e["OBS"], errors="coerce")
        df_e = df_e.dropna()
        df_e["TAUX"] = 1.0 / df_e["OBS"]

        merged = pd.merge(df_b, df_e[["DATE", "TAUX"]], on="DATE", how="inner")
        if merged.empty:
            return None, None, None

        last = merged.iloc[-1]
        brent_eur_litre = (last["BRENT_USD"] * last["TAUX"]) / LITRES_PAR_BARIL
        return round(brent_eur_litre, 4), round(last["BRENT_USD"], 2), round(last["TAUX"], 4)
    except Exception:
        return None, None, None


@st.cache_data(ttl=86400)
def fetch_dgec_raffinage():
    try:
        r = requests.get(DGEC_MARGE, timeout=30)
        df = pd.read_excel(io.BytesIO(r.content), header=None)
        resultats = {}
        for _, row in df.iloc[2:].iterrows():
            try:
                dt = pd.to_datetime(row[0])
                marge = float(row[2]) / LITRES_PAR_TONNE
                resultats[dt.strftime("%Y-%m")] = round(marge, 4)
            except Exception:
                continue
        mois_actuel = date.today().strftime("%Y-%m")
        mois_precedent = (date.today().replace(day=1) - pd.Timedelta(days=1)).strftime("%Y-%m")
        return resultats.get(mois_actuel) or resultats.get(mois_precedent) or 0.05
    except Exception:
        return 0.05


@st.cache_data(ttl=900)
def fetch_prix_pompe():
    try:
        r = requests.get(PRIX_URL, timeout=90, stream=True)
        content = b""
        for chunk in r.iter_content(chunk_size=1024 * 256):
            content += chunk
            if len(content) > 20 * 1024 * 1024:
                break
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            with z.open(z.namelist()[0]) as f:
                tree = ET.parse(f)
        root = tree.getroot()
        prix = {"Gazole": [], "SP95-E10": [], "SP98": []}
        for station in root.findall("pdv"):
            if station.get("type") == "A":
                continue
            for el in station.findall("prix"):
                nom = el.get("nom", "")
                val = el.get("valeur", "")
                if nom in prix and val:
                    try:
                        p = float(val)
                        if p > 10:
                            p = p / 1000
                        if 0.8 <= p <= 4.0:
                            prix[nom].append(p)
                    except Exception:
                        pass
        return {k: round(sum(v) / len(v), 4) for k, v in prix.items() if v}
    except Exception:
        return {}


# ─── APP ─────────────────────────────────────────────────────────────────────

# CSS global : radio buttons → pills, supprime l'ancre des titres
st.markdown("""
<style>
/* Largeur principale */
.main .block-container,
section.main > div.block-container {
    max-width: 860px !important;
    margin-left: auto !important;
    margin-right: auto !important;
    padding-top: 2rem !important;
    padding-left: 1.5rem !important;
    padding-right: 1.5rem !important;
}

/* Radio buttons as pills */
div[role="radiogroup"] { display:flex; flex-direction:row; gap:6px; flex-wrap:wrap; }
div[role="radiogroup"] label {
    background:#f3f4f6; border:1px solid #e5e7eb; border-radius:8px;
    padding:5px 14px; cursor:pointer; font-size:0.85rem; font-weight:500; color:#374151;
}
div[role="radiogroup"] label:has(input:checked) {
    background:#111827 !important; border-color:#111827 !important;
}
div[role="radiogroup"] label:has(input:checked),
div[role="radiogroup"] label:has(input:checked) * {
    color: white !important;
}
div[role="radiogroup"] input[type="radio"] { position:absolute; opacity:0; width:0; height:0; }
</style>
""", unsafe_allow_html=True)

# Header — <div> au lieu de <h1> pour éviter l'ancre Streamlit
st.markdown(
    "<div style='font-size:2rem;font-weight:700;margin-bottom:0.25rem;line-height:1.2'>"
    "Le prix du carburant, composante par composante.</div>",
    unsafe_allow_html=True,
)
st.markdown(
    "<p style='color:#6b7280;margin-bottom:2rem'>"
    "Pétrole brut, raffinage, distribution et taxes calculés à partir des données publiques officielles depuis 2015.</p>",
    unsafe_allow_html=True,
)

# Sélecteur carburant
carburant = st.radio(
    "Carburant",
    ["Gazole", "SP95-E10", "SP98"],
    horizontal=True,
    label_visibility="collapsed",
)

st.divider()

# ─── PRIX ACTUEL ─────────────────────────────────────────────────────────────

st.markdown("<div style='font-size:1.4rem;font-weight:600;margin:0.5rem 0'>Prix actuel</div>", unsafe_allow_html=True)

with st.spinner("Chargement des données en temps réel..."):
    prix_pompe   = fetch_prix_pompe()
    brent, brent_usd, taux = fetch_brent_eurusd()
    raffinage    = fetch_dgec_raffinage()

# Fallback sur la dernière semaine du CSV si les APIs sont indisponibles
prix = prix_pompe.get(carburant, 0)
_distribution_csv = None
if prix == 0 or brent is None:
    hist_fb = load_historique()
    last = hist_fb[hist_fb["carburant"] == carburant].sort_values("semaine").iloc[-1]
    prix              = float(last["prix"])
    brent             = float(last["brent"])
    raffinage         = float(last["raffinage"])
    _distribution_csv = float(last["distribution"])
    brent_usd, taux   = None, None
    _fallback = True
else:
    _fallback = False

if prix > 0 and brent:
    annee = date.today().year
    ticpe = TICPE.get(carburant, {}).get(annee, TICPE.get(carburant, {}).get(2020, 0.6))
    tva   = prix - prix / 1.2
    distribution = _distribution_csv if _distribution_csv is not None else max(prix / 1.2 - brent - raffinage - ticpe, 0)

    composantes = {
        "Pétrole brut": brent,
        "Raffinage":    raffinage,
        "Distribution": distribution,
        "Taxes":        tva + ticpe,
    }
    total = sum(composantes.values())

    # Prix affiché
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(
            f"<p style='font-size:0.8rem;color:#9ca3af;margin:0'>1 litre de {carburant} · Moyenne nationale</p>"
            f"<p style='font-size:2.2rem;font-weight:700;margin:0;line-height:1.1'>"
            f"{prix:.3f} <span style='font-size:1rem;font-weight:400;color:#6b7280'>€</span></p>",
            unsafe_allow_html=True,
        )
    with col2:
        if brent_usd and taux:
            st.markdown(
                f"<p style='font-size:0.75rem;color:#9ca3af;text-align:right;margin:0'>"
                f"Brent FRED · {brent_usd:.0f} $/baril<br>1 $ = {taux:.4f} €</p>",
                unsafe_allow_html=True,
            )

    if _fallback:
        st.caption("Données en temps réel indisponibles. Dernière semaine du CSV affichée.")

    # Barre de décomposition
    fig_bar = go.Figure()
    for label, val in composantes.items():
        pct = val / total * 100
        fig_bar.add_trace(go.Bar(
            x=[pct], y=[""],
            orientation="h",
            name=label,
            marker_color=COULEURS[label],
            text=f"{val:.3f}€" if pct > 12 else (f"{pct:.0f}%" if pct > 6 else ""),
            textposition="inside",
            insidetextanchor="middle",
            hovertemplate=f"<b>{label}</b><br>{val:.3f} €/L · {pct:.1f}%<extra></extra>",
        ))
    fig_bar.update_layout(
        barmode="stack",
        height=60,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
        xaxis=dict(visible=False, range=[0, 100]),
        yaxis=dict(visible=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

    # Cards — grille responsive (4 colonnes desktop, 2x2 mobile)
    details = {
        "Pétrole brut": "Cours Brent",
        "Raffinage":    "Marge raffineur",
        "Distribution": "Logistique + station",
        "Taxes":        "TVA 20% + TICPE",
    }
    cards_html = "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-top:0.25rem'>"
    for label, val in composantes.items():
        pct = val / total * 100
        cards_html += (
            f"<div style='border:1px solid #e5e7eb;border-radius:12px;padding:12px;"
            f"border-top:3px solid {COULEURS[label]}'>"
            f"<p style='font-size:0.75rem;font-weight:600;color:#374151;margin:0'>{label}</p>"
            f"<p style='font-size:1.2rem;font-weight:700;margin:4px 0'>{val:.3f}"
            f"<span style='font-size:0.78rem;font-weight:400;color:#6b7280'> €/L</span></p>"
            f"<p style='font-size:0.7rem;color:#9ca3af;margin:0'>{pct:.0f}% · {details[label]}</p>"
            f"</div>"
        )
    cards_html += "</div>"
    st.markdown(cards_html, unsafe_allow_html=True)

    st.markdown(
        "<p style='font-size:0.7rem;color:#9ca3af;margin-top:0.75rem'>"
        "La composante Distribution est un résidu calculé, pas une mesure directe. "
        "Elle inclut logistique, exploitation de la station et absorbe les imprécisions du modèle.</p>",
        unsafe_allow_html=True,
    )
else:
    st.info("Données en temps réel indisponibles — affichage de la dernière semaine disponible.")

st.divider()

# ─── QUI PREND QUOI ──────────────────────────────────────────────────────────

st.markdown("<div style='font-size:1.4rem;font-weight:600;margin:0.5rem 0'>Qui prend quoi ?</div>", unsafe_allow_html=True)
st.caption("Derrière chaque litre, quatre acteurs aux logiques très différentes.")

_brent_info = f" · {brent_usd:.0f} $/baril" if brent_usd else ""
_taux_info  = f" (actuellement 1 $ = {taux:.4f} €)" if taux else ""
_ticpe_str  = f"{ticpe:.4f}" if ticpe else "0.5974"

blocs = [
    {
        "titre": "Pétrole brut (Brent)",
        "sous":  f"Marchés mondiaux · OPEP{_brent_info}",
        "color": COULEURS["Pétrole brut"],
        "texte": (
            f"La composante la plus volatile. Son prix dépend du cours mondial du brut "
            f"et du taux de change €/${_taux_info}. "
            f"Quand le dollar monte, le pétrole coûte plus cher en euros même si son prix en dollars ne change pas."
        ),
    },
    {
        "titre": "Raffinage",
        "sous":  "Raffineurs (TotalEnergies, Esso...) · DGEC",
        "color": COULEURS["Raffinage"],
        "texte": (
            "Marge du raffineur pour transformer le brut en carburant utilisable. "
            "Actuellement très faible, signe de surcapacité mondiale. "
            "Cette marge peut être négative en période de crise."
        ),
    },
    {
        "titre": "Distribution",
        "sous":  "Transport, logistique, station, marge nette · prix libres",
        "color": COULEURS["Distribution"],
        "texte": (
            "Agrège tout ce qui n'est pas mesuré séparément : transport raffinerie-dépôt-station, "
            "stockage, coûts d'exploitation de la station (loyer, personnel, énergie), "
            "incorporation biocarburant, et la marge nette du distributeur. "
            "La marge nette ne représente qu'une partie de ce total."
        ),
    },
    {
        "titre": "État (TVA + TICPE)",
        "sous":  "Fixé par la loi · Journal Officiel",
        "color": COULEURS["Taxes"],
        "texte": (
            f"La TICPE est fixe par litre ({_ticpe_str} €/L), "
            f"votée au Parlement. La TVA de 20% s'applique sur le tout, y compris sur la TICPE elle-même."
        ),
    },
]

blocs_html = "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;margin-top:0.5rem'>"
for b in blocs:
    c, titre, sous, texte = b["color"], b["titre"], b["sous"], b["texte"]
    blocs_html += (
        f"<div style='border:1px solid #e5e7eb;border-radius:14px;padding:16px;border-top:3px solid {c}'>"
        f"<p style='font-size:0.875rem;font-weight:600;color:#111827;margin:0'>{titre}</p>"
        f"<p style='font-size:0.75rem;color:#9ca3af;margin:2px 0 10px'>{sous}</p>"
        f"<p style='font-size:0.875rem;color:#374151;line-height:1.55;margin:0'>{texte}</p>"
        f"</div>"
    )
blocs_html += "</div>"
st.markdown(blocs_html, unsafe_allow_html=True)

st.divider()

# ─── HISTORIQUE ──────────────────────────────────────────────────────────────

st.markdown("<div style='font-size:1.4rem;font-weight:600;margin:0.5rem 0'>Évolution depuis 2015</div>", unsafe_allow_html=True)
st.caption("Moyennes nationales, décomposition annuelle ou mois par mois.")

hist = load_historique()
df_carb = hist[hist["carburant"] == carburant].copy()

vue = st.radio("Vue", ["Par année", "Par mois"], index=1, horizontal=True, label_visibility="collapsed")

if vue == "Par année":
    df_carb["annee"] = df_carb["semaine"].dt.year
    agg = df_carb.groupby("annee").agg(
        prix=("prix", "mean"),
        brent=("brent", "mean"),
        raffinage=("raffinage", "mean"),
        distribution=("distribution", "mean"),
        tva=("tva", "mean"),
        ticpe=("ticpe", "mean"),
    ).reset_index()
    x = agg["annee"].astype(str)
    rows = agg
else:
    df_carb["mois"] = df_carb["semaine"].dt.to_period("M").astype(str)
    rows = df_carb.groupby("mois").agg(
        prix=("prix", "mean"),
        brent=("brent", "mean"),
        raffinage=("raffinage", "mean"),
        distribution=("distribution", "mean"),
        tva=("tva", "mean"),
        ticpe=("ticpe", "mean"),
    ).reset_index()
    x = rows["mois"]

fig = go.Figure()
fig.add_trace(go.Bar(x=x, y=rows["brent"],       name="Pétrole brut", marker_color=COULEURS["Pétrole brut"]))
fig.add_trace(go.Bar(x=x, y=rows["raffinage"],   name="Raffinage",    marker_color=COULEURS["Raffinage"]))
fig.add_trace(go.Bar(x=x, y=rows["distribution"],name="Distribution", marker_color=COULEURS["Distribution"]))
fig.add_trace(go.Bar(x=x, y=rows["tva"] + rows["ticpe"], name="Taxes", marker_color=COULEURS["Taxes"]))

fig.update_layout(
    barmode="stack",
    height=400,
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
    yaxis_title="€ / litre",
    plot_bgcolor="white",
    paper_bgcolor="white",
    xaxis=dict(showgrid=False),
    yaxis=dict(gridcolor="#f3f4f6"),
)
st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

# ─── MÉTHODE ─────────────────────────────────────────────────────────────────

st.divider()
with st.expander("Méthode de calcul"):
    st.markdown("""
**Formule**

```
Distribution = Prix TTC / 1,2 - Brent - Raffinage - TICPE
```

La TVA de 20% est retirée en divisant le prix TTC par 1,2. La composante Distribution est le résidu : ce qui reste après avoir soustrait le coût du pétrole brut, la marge du raffineur et la TICPE.

**Sources**
- Prix pompe : prix-carburants.gouv.fr via donnees.roulez-eco.fr
- Brent : FRED, Federal Reserve Bank of St. Louis
- Taux EUR/USD : Banque Centrale Européenne
- Marge de raffinage : DGEC, ecologie.gouv.fr
- TICPE : Journal Officiel

**Limites**

La composante Distribution est un résidu calculé, pas une mesure directe. Elle inclut transport, logistique, exploitation de la station et absorbe les imprécisions du modèle. Elle ne représente pas la marge nette du distributeur.
    """)

st.markdown(
    "<p style='text-align:center;font-size:0.75rem;color:#9ca3af;margin-top:2rem'>"
    "Données publiques · prix-carburants.gouv.fr · FRED · DGEC · "
    "<a href='https://github.com/NicolasBrnd/transparence-carburant-streamlit' style='color:#9ca3af'>GitHub</a></p>",
    unsafe_allow_html=True,
)
