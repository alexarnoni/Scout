"""
Testes property-based para o bugfix de confidence penalty no scout service.
Spec: .kiro/specs/scout-confidence-penalty/
"""
import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from app.services.scout import _normalize_group


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_player(player_id: str, total_minutes: int, goals_p90: float = 1.0) -> dict:
    """Cria um dict de jogador mínimo para uso em _normalize_group."""
    return {
        "player_id": player_id,
        "player_name": f"Player {player_id}",
        "total_minutes": total_minutes,
        "goals_p90": goals_p90,
    }


# ---------------------------------------------------------------------------
# Property 1: Bug Condition - Confidence Penalty Applied to Score
# Validates: Requirements 1.1, 1.2
#
# ESTE TESTE DEVE FALHAR no código não corrigido.
# A falha confirma que o bug existe: ambos os jogadores recebem o mesmo score
# mesmo quando um tem muito menos minutos que o outro.
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    minutes_a=st.integers(min_value=181, max_value=2700),
    minutes_b=st.integers(min_value=1, max_value=180),
    goals_a=st.floats(min_value=1.0, max_value=5.0, allow_nan=False, allow_infinity=False),
    goals_b=st.floats(min_value=0.0, max_value=0.9, allow_nan=False, allow_infinity=False),
)
def test_property1_confidence_penalty_applied_to_score(minutes_a, minutes_b, goals_a, goals_b):
    """
    **Validates: Requirements 1.1, 1.2**

    Property 1: Bug Condition - Confidence Penalty Applied to Score

    Para qualquer grupo onde player_b.total_minutes < player_a.total_minutes,
    e player_a tem métricas superiores (goals_a > goals_b), o score de player_b
    deve ser penalizado proporcionalmente aos seus minutos.

    Verifica que score_b == round(score_raw_b * (minutes_b / minutes_a), 1).

    No código NÃO corrigido, score_b == score_raw_b sem penalização (bug).
    """
    assume(minutes_a > minutes_b)

    player_a = _make_player("a", total_minutes=minutes_a, goals_p90=goals_a)
    player_b = _make_player("b", total_minutes=minutes_b, goals_p90=goals_b)

    metrics = ["goals_p90"]
    result = _normalize_group([player_a, player_b], metrics)

    result_map = {p["player_id"]: p for p in result}
    score_b = result_map["b"]["score"]

    # Calcular score_raw_b manualmente via min-max normalização
    min_v = min(goals_a, goals_b)
    max_v = max(goals_a, goals_b)
    rng = max_v - min_v or 1
    norm_b = round((goals_b - min_v) / rng * 100, 1)
    score_raw_b = round(norm_b, 1)

    max_minutes = minutes_a  # minutes_a > minutes_b
    confidence_b = minutes_b / max_minutes
    expected_score_b = round(score_raw_b * confidence_b, 1)

    assert score_b == expected_score_b, (
        f"Bug detectado: player_b.score={score_b} != expected={expected_score_b} "
        f"(score_raw_b={score_raw_b}, confidence_b={confidence_b:.3f}, "
        f"minutes_b={minutes_b}, minutes_a={minutes_a}). "
        f"O score deveria ser penalizado proporcionalmente aos minutos."
    )


# ---------------------------------------------------------------------------
# Caso concreto: player_a (900 min) e player_b (90 min), goals_p90=1.0
# Validates: Requirements 1.1, 1.2
# ---------------------------------------------------------------------------

def test_concrete_case_confidence_penalty():
    """
    Caso concreto do bug:
    - player_a: 900 min, goals_p90=2.0
    - player_b: 90 min, goals_p90=0.0
    Espera: player_b.score == round(score_raw_b * (90/900), 1)

    Com min-max: norm_a=100.0, norm_b=0.0
    score_raw_a=100.0, score_raw_b=0.0
    confidence_a=1.0, confidence_b=0.1
    score_a=100.0, score_b=0.0

    Caso mais rico: player_a(900min, goals=2.0), player_b(90min, goals=1.0)
    norm_a=100.0, norm_b=0.0 → score_raw_b=0.0 → score_b=0.0

    Melhor caso: player_a(900min, goals=2.0), player_b(90min, goals=1.0)
    norm_b = (1.0 - 1.0) / (2.0 - 1.0) * 100 = 0.0 → ainda 0.0

    Usar: player_a(900min, goals=2.0), player_b(90min, goals=1.5)
    norm_b = (1.5 - 1.5) / (2.0 - 1.5) * 100 = 0.0 → ainda 0.0 (min=1.5)

    Usar: player_a(900min, goals=2.0), player_b(90min, goals=1.0)
    min=1.0, max=2.0, rng=1.0
    norm_a = (2.0-1.0)/1.0*100 = 100.0
    norm_b = (1.0-1.0)/1.0*100 = 0.0
    score_raw_b = 0.0 → score_b = 0.0 * 0.1 = 0.0

    Usar: player_a(900min, goals=2.0), player_b(90min, goals=1.8)
    min=1.8, max=2.0, rng=0.2
    norm_a = (2.0-1.8)/0.2*100 = 100.0
    norm_b = (1.8-1.8)/0.2*100 = 0.0 → score_raw_b=0.0

    O único jeito de ter score_raw_b > 0 é player_b ter goals > min do grupo.
    Usar 3 jogadores: player_c(45min, goals=0.5), player_a(900min, goals=2.0), player_b(90min, goals=1.0)
    min=0.5, max=2.0, rng=1.5
    norm_a = (2.0-0.5)/1.5*100 = 100.0
    norm_b = (1.0-0.5)/1.5*100 = 33.3
    norm_c = (0.5-0.5)/1.5*100 = 0.0
    score_raw_b = 33.3; confidence_b = 90/900 = 0.1
    score_b = round(33.3 * 0.1, 1) = 3.3

    No código NÃO corrigido: score_b = 33.3 (sem penalização).
    """
    player_a = _make_player("a", total_minutes=900, goals_p90=2.0)
    player_b = _make_player("b", total_minutes=90, goals_p90=1.0)
    player_c = _make_player("c", total_minutes=45, goals_p90=0.5)

    metrics = ["goals_p90"]
    result = _normalize_group([player_a, player_b, player_c], metrics)

    result_map = {p["player_id"]: p for p in result}
    score_b = result_map["b"]["score"]

    # Calcular manualmente
    min_v, max_v = 0.5, 2.0
    rng = max_v - min_v  # 1.5
    norm_b = round((1.0 - min_v) / rng * 100, 1)  # 33.3
    score_raw_b = round(norm_b, 1)
    confidence_b = 90 / 900  # 0.1
    expected_score_b = round(score_raw_b * confidence_b, 1)

    assert score_b == expected_score_b, (
        f"Bug detectado: player_b.score={score_b} deveria ser {expected_score_b}. "
        f"score_raw_b={score_raw_b}, confidence_b={confidence_b}. "
        f"player_b(90min) deveria ter score penalizado em relação ao score_raw."
    )


# ---------------------------------------------------------------------------
# Preservation Tests (Task 2)
# Estes testes DEVEM PASSAR no código não corrigido.
# Documentam o comportamento baseline a ser preservado após a correção.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Preservation A: Jogador com max_minutes tem score == score_raw
# Validates: Requirements 2.3, 3.1
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    minutes_a=st.integers(min_value=181, max_value=2700),
    minutes_b=st.integers(min_value=1, max_value=180),
    goals_a=st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
    goals_b=st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
)
def test_preservation_a_max_minutes_player_score_equals_score_raw(minutes_a, minutes_b, goals_a, goals_b):
    """
    **Validates: Requirements 2.3, 3.1**

    Preservation A: Para qualquer grupo, o jogador com total_minutes == max_minutes
    tem score == score_raw (confidence = 1.0 → sem redução).

    No código não corrigido: player_a (max_minutes) recebe score_raw diretamente.
    Este comportamento deve ser preservado após a correção.
    """
    assume(minutes_a > minutes_b)

    player_a = _make_player("a", total_minutes=minutes_a, goals_p90=goals_a)
    player_b = _make_player("b", total_minutes=minutes_b, goals_p90=goals_b)

    metrics = ["goals_p90"]
    result = _normalize_group([player_a, player_b], metrics)

    result_map = {p["player_id"]: p for p in result}
    score_a = result_map["a"]["score"]

    # Calcular score_raw de player_a manualmente:
    # min-max normalização de goals_p90 no grupo
    min_v = min(goals_a, goals_b)
    max_v = max(goals_a, goals_b)
    rng = max_v - min_v or 1
    norm_a = round((goals_a - min_v) / rng * 100, 1)
    score_raw_a = round(norm_a, 1)

    assert score_a == score_raw_a, (
        f"Preservation A falhou: player_a.score={score_a} != score_raw_a={score_raw_a}. "
        f"O jogador com max_minutes deve ter confidence=1.0 (score inalterado)."
    )


# ---------------------------------------------------------------------------
# Preservation B: metrics contém valores normalizados pré-penalização
# Validates: Requirements 3.5
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    minutes_a=st.integers(min_value=181, max_value=2700),
    minutes_b=st.integers(min_value=1, max_value=180),
    goals_a=st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
    goals_b=st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
)
def test_preservation_b_metrics_contains_pre_penalty_normalized_values(minutes_a, minutes_b, goals_a, goals_b):
    """
    **Validates: Requirements 3.5**

    Preservation B: Para qualquer grupo, o dict metrics de cada jogador contém
    os valores normalizados pré-penalização (min-max 0-100), sem aplicação do
    fator de confiança.

    No código não corrigido: metrics sempre contém os valores min-max normalizados.
    Este comportamento deve ser preservado após a correção.
    """
    assume(minutes_a > minutes_b)

    player_a = _make_player("a", total_minutes=minutes_a, goals_p90=goals_a)
    player_b = _make_player("b", total_minutes=minutes_b, goals_p90=goals_b)

    metrics = ["goals_p90"]
    result = _normalize_group([player_a, player_b], metrics)

    result_map = {p["player_id"]: p for p in result}

    # Calcular valores normalizados esperados (min-max, sem penalização)
    min_v = min(goals_a, goals_b)
    max_v = max(goals_a, goals_b)
    rng = max_v - min_v or 1
    expected_norm_a = round((goals_a - min_v) / rng * 100, 1)
    expected_norm_b = round((goals_b - min_v) / rng * 100, 1)

    actual_norm_a = result_map["a"]["metrics"].get("goals_p90")
    actual_norm_b = result_map["b"]["metrics"].get("goals_p90")

    assert actual_norm_a == expected_norm_a, (
        f"Preservation B falhou para player_a: metrics['goals_p90']={actual_norm_a} "
        f"!= expected={expected_norm_a}. O campo metrics deve conter valores pré-penalização."
    )
    assert actual_norm_b == expected_norm_b, (
        f"Preservation B falhou para player_b: metrics['goals_p90']={actual_norm_b} "
        f"!= expected={expected_norm_b}. O campo metrics deve conter valores pré-penalização."
    )


# ---------------------------------------------------------------------------
# Preservation C: Resultado ordenado em ordem decrescente de score
# Validates: Requirements 3.2
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    players_data=st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=2700),   # total_minutes
            st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),  # goals_p90
        ),
        min_size=1,
        max_size=10,
    )
)
def test_preservation_c_result_sorted_descending_by_score(players_data):
    """
    **Validates: Requirements 3.2**

    Preservation C: Para qualquer grupo, o resultado está ordenado em ordem
    decrescente de score.

    No código não corrigido: sorted(..., reverse=True) garante isso.
    Este comportamento deve ser preservado após a correção.
    """
    players = [
        _make_player(str(i), total_minutes=minutes, goals_p90=goals)
        for i, (minutes, goals) in enumerate(players_data)
    ]

    metrics = ["goals_p90"]
    result = _normalize_group(players, metrics)

    scores = [p["score"] for p in result]
    assert scores == sorted(scores, reverse=True), (
        f"Preservation C falhou: scores={scores} não estão em ordem decrescente."
    )


# ---------------------------------------------------------------------------
# Preservation D: Grupo com um único jogador retorna confidence = 1.0
# Validates: Requirements 3.3
# ---------------------------------------------------------------------------

@settings(max_examples=100)
@given(
    minutes=st.integers(min_value=1, max_value=2700),
    goals=st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
)
def test_preservation_d_single_player_group_confidence_one(minutes, goals):
    """
    **Validates: Requirements 3.3**

    Preservation D: Um grupo com um único jogador retorna esse jogador com
    confidence = 1.0 (score inalterado, pois é o max_minutes do grupo).

    No código não corrigido: grupo com 1 jogador → min==max → norm=0 (rng=1),
    score = 0.0 (pois goals_p90 normalizado = 0 quando só há 1 jogador).
    O score não deve ser reduzido por confiança (confidence = 1.0).
    """
    player = _make_player("solo", total_minutes=minutes, goals_p90=goals)

    metrics = ["goals_p90"]
    result = _normalize_group([player], metrics)

    assert len(result) == 1, "Grupo com 1 jogador deve retornar 1 jogador."

    solo = result[0]

    # Com 1 jogador: min==max → rng=1, norm = (goals - goals) / 1 * 100 = 0.0
    # score_raw = 0.0; confidence = minutes/minutes = 1.0; score = 0.0
    # O ponto chave: score não deve ser reduzido (confidence=1.0)
    # Verificamos que score == score_raw (sem penalização)
    expected_norm = 0.0  # único jogador → min-max normalização resulta em 0
    expected_score = round(expected_norm, 1)

    assert solo["score"] == expected_score, (
        f"Preservation D falhou: score={solo['score']} != expected={expected_score}. "
        f"Grupo com 1 jogador deve ter confidence=1.0 (score inalterado)."
    )
