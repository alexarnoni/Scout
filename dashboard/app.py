import requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

API_BASE = "http://localhost:8000"

POSITION_METRICS = {
    "Goleiro":    ["saves_p90", "goals_conceded_p90", "clean_sheet_rate"],
    "Defensor":   ["goals_p90", "assists_p90", "fouls_p90"],
    "Meio-campo": ["goals_p90", "assists_p90", "shots_on_target_p90"],
    "Atacante":   ["goals_p90", "assists_p90", "conversion_rate"],
}

METRIC_LABELS = {
    "saves_p90": "Defesas/90",
    "goals_conceded_p90": "Gols Sofridos/90",
    "clean_sheet_rate": "Clean Sheet %",
    "goals_p90": "Gols/90",
    "assists_p90": "Assistências/90",
    "fouls_p90": "Faltas/90",
    "shots_on_target_p90": "Chutes no Gol/90",
    "conversion_rate": "Conversão %",
    "shots_p90": "Chutes/90",
    "red_cards_p90": "Cartões Verm./90",
    "yellow_cards_p90": "Cartões Amar./90",
}

st.set_page_config(page_title="Scout — Brasileirão Série A 2026", layout="wide")

# Session state defaults
if "page" not in st.session_state:
    st.session_state.page = "ranking"
if "player_id" not in st.session_state:
    st.session_state.player_id = None

# Load competitions (needed on both pages)
try:
    comp_resp = requests.get(f"{API_BASE}/competitions", timeout=5)
    comp_resp.raise_for_status()
    competitions = comp_resp.json()
except Exception:
    st.error("API não disponível. Certifique-se que o backend está rodando.")
    st.stop()

# Sidebar filters (always visible)
with st.sidebar:
    st.header("Filtros")
    comp_map = {c["name"]: c["id"] for c in competitions}
    selected_comp = st.selectbox("Competição", list(comp_map.keys()))
    competition_id = comp_map[selected_comp]
    position = st.selectbox("Posição", ["Goleiro", "Defensor", "Meio-campo", "Atacante"])
    min_minutes = st.slider("Mínimo de minutos", min_value=90, max_value=450, value=180, step=90)

    st.divider()
    st.header("Ver card do jogador")
    player_selectbox_placeholder = st.empty()


# ── PAGE: PLAYER CARD ────────────────────────────────────────────────────────
def show_player_card():
    if st.button("← Voltar"):
        st.session_state.page = "ranking"
        st.rerun()

    player_id = st.session_state.player_id
    try:
        resp = requests.get(
            f"{API_BASE}/scout/player/{player_id}",
            params={"competition_id": competition_id, "min_minutes": min_minutes},
            timeout=5,
        )
        resp.raise_for_status()
        p = resp.json()
    except Exception:
        st.error("API não disponível. Certifique-se que o backend está rodando.")
        return

    is_gem = p["score"] >= 60 and p["total_minutes"] <= 270

    st.title(p["player_name"])
    st.subheader(f"{p['team_name']} · {p['position']}")

    if is_gem:
        st.success("🔍 Joia Escondida")

    col1, col2, col3 = st.columns(3)
    col1.metric("Score", f"{p['score']:.2f}")
    col2.metric("Minutos jogados", p["total_minutes"])
    col3.metric("Partidas", p["matches_played"])

    st.divider()

    # Radar chart — normalize 0-100 using fixed reasonable ranges per metric
    metrics = p.get("metrics", {})
    metric_keys = [k for k in metrics if metrics[k] is not None]
    labels = [METRIC_LABELS.get(k, k) for k in metric_keys]

    # Simple min-max normalization against plausible max values per metric
    METRIC_MAX = {
        "saves_p90": 10.0, "goals_conceded_p90": 5.0, "clean_sheet_rate": 1.0,
        "goals_p90": 2.0, "assists_p90": 1.5, "shots_p90": 8.0,
        "shots_on_target_p90": 4.0, "fouls_p90": 6.0, "conversion_rate": 1.0,
        "yellow_cards_p90": 1.0, "red_cards_p90": 0.5,
    }
    values = []
    for k in metric_keys:
        raw = metrics[k] or 0.0
        max_v = METRIC_MAX.get(k, 1.0)
        values.append(min(round(raw / max_v * 100, 1), 100.0))

    if labels:
        fig = go.Figure(go.Scatterpolar(
            r=values + [values[0]],
            theta=labels + [labels[0]],
            fill="toself",
            line_color="#1f77b4",
        ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            showlegend=False,
            margin=dict(t=40, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Métricas brutas")
    raw_rows = [
        {"Métrica": METRIC_LABELS.get(k, k), "Valor": round(metrics[k], 4) if metrics[k] is not None else "-"}
        for k in metrics
    ]
    st.dataframe(pd.DataFrame(raw_rows), use_container_width=True, hide_index=True)


# ── PAGE: RANKING ────────────────────────────────────────────────────────────
def show_ranking():
    st.title("Scout — Brasileirão Série A 2026")

    try:
        resp = requests.get(
            f"{API_BASE}/scout/ranking",
            params={"competition_id": competition_id, "position": position, "min_minutes": min_minutes},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        st.error("API não disponível. Certifique-se que o backend está rodando.")
        return

    if not data:
        st.info("Nenhum jogador encontrado com os filtros selecionados.")
        return

    metric_cols = POSITION_METRICS[position]

    # Build dataframe
    MEDAL = {1: "🥇", 2: "🥈", 3: "🥉"}
    rows = []
    for rank, player in enumerate(data, start=1):
        metrics = player.get("metrics", {})
        is_gem = player["score"] >= 60 and player["total_minutes"] <= 270
        name = f"{player['player_name']} 🔍" if is_gem else player["player_name"]
        score_label = f"{MEDAL[rank]} {player['score']:.2f}" if rank <= 3 else f"{player['score']:.2f}"
        row = {
            "#": rank,
            "Jogador": name,
            "Time": player["team_name"],
            "Min": player["total_minutes"],
            "Jogos": player["matches_played"],
            "Score": score_label,
        }
        for m in metric_cols:
            val = metrics.get(m)
            row[METRIC_LABELS.get(m, m)] = f"{val:.2f}" if val is not None else "-"
        rows.append(row)

    df = pd.DataFrame(rows)

    GOLD   = "background-color: #FFD700; color: #000"
    SILVER = "background-color: #C0C0C0; color: #000"
    BRONZE = "background-color: #CD7F32; color: #fff"

    def highlight_podium(row):
        rank = row["#"]
        style = ""
        if rank == 1:
            style = GOLD
        elif rank == 2:
            style = SILVER
        elif rank == 3:
            style = BRONZE
        return [style] * len(row)

    styled = df.style.apply(highlight_podium, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True)

    translated = [METRIC_LABELS.get(m, m) for m in metric_cols]
    st.caption(f"Métricas exibidas: {', '.join(translated)}")

    # Populate sidebar selectbox with current ranking
    options = ["— selecione —"] + [
        f"{r['#']}. {p['player_name']} ({p['team_name']})"
        for r, p in zip(rows, data)
    ]
    player_id_map = {
        f"{r['#']}. {p['player_name']} ({p['team_name']})": p["player_id"]
        for r, p in zip(rows, data)
    }
    selected = player_selectbox_placeholder.selectbox("Ver card do jogador", options, key="ranking_selectbox")
    if selected != "— selecione —":
        st.session_state.player_id = player_id_map[selected]
        st.session_state.page = "player"
        st.rerun()


# ── ROUTER ───────────────────────────────────────────────────────────────────
if st.session_state.page == "player" and st.session_state.player_id:
    show_player_card()
else:
    show_ranking()
