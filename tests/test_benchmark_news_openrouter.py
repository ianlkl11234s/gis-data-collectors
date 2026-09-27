import json
from unittest.mock import Mock

import pytest

from collectors.news_events import TownshipGazetteer
from scripts import benchmark_news_openrouter as benchmark


@pytest.fixture
def gazetteer():
    return TownshipGazetteer([
        {"code": "63000050", "name": "臺北市中正區"},
        {"code": "64000110", "name": "高雄市鼓山區"},
    ])


@pytest.fixture
def rows():
    return [
        {"title": "臺北火警", "summary": "測試一"},
        {"title": "高雄事故", "summary": "測試二"},
    ]


def annotation(idx, county="臺北市", township="中正區"):
    return {
        "idx": idx, "county": county, "township": township, "category": "accident",
        "gis_relevance": 3, "severity": 2, "is_event": True,
    }


def test_hard_budget_stops_before_network(monkeypatch, rows, gazetteer):
    request = Mock()
    monkeypatch.setattr(benchmark, "_request_model", request)

    report = benchmark.run_benchmark(rows, ["first", "second"], gazetteer, "test-key", 10, 0, 1)

    request.assert_not_called()
    assert [entry["skipped"] for entry in report["models"]] == ["hard_budget_stop", "hard_budget_stop"]


def test_partial_response_is_not_json_complete(rows, gazetteer):
    metrics = benchmark.evaluate_annotations(rows, [annotation(0)], gazetteer)

    assert metrics["json_complete"] is False
    assert metrics["missing_idx"] == [1]
    assert "gold_accuracy" not in metrics


def test_invalid_township_is_not_counted_as_valid_gazetteer_claim(rows, gazetteer):
    predicted = [annotation(0, "臺北市", "建成區"), annotation(1, "高雄市", None)]

    metrics = benchmark.evaluate_annotations(rows, predicted, gazetteer)

    assert metrics["gazetteer"] == {
        "location_claims": 2,
        "valid_claims": 1,
        "validation_rate": 0.5,
        "county_claims": 2,
        "valid_counties": 2,
        "township_claims": 1,
        "valid_townships": 0,
        "downgraded_to_county": 1,
    }


def test_malformed_provider_response_is_safe_and_has_no_content(monkeypatch, rows, gazetteer):
    response = Mock()
    response.json.return_value = {"choices": [{"message": {"content": "not-json-secret-news"}}]}
    monkeypatch.setattr(benchmark.requests, "post", Mock(return_value=response))

    result = benchmark._request_model("model-a", rows, gazetteer, "test-key", 10)

    assert result["annotations"] == []
    assert result["error"] == "OpenRouter request failed (JSONDecodeError)"
    assert "secret-news" not in result["error"]


def test_structured_request_disables_reasoning_and_caps_output(monkeypatch, rows, gazetteer):
    response = Mock()
    response.json.return_value = {
        "choices": [{"message": {"content": json.dumps([annotation(0), annotation(1)])}}],
        "usage": {},
    }
    post = Mock(return_value=response)
    monkeypatch.setattr(benchmark.requests, "post", post)

    result = benchmark._request_model(
        "model-a", rows, gazetteer, "test-key", 10,
        structured=True, disable_reasoning=True, max_tokens=2048,
    )

    assert result["error"] is None
    body = post.call_args.kwargs["json"]
    assert body["reasoning"] == {"enabled": False}
    assert body["max_tokens"] == 2048
    assert body["provider"] == {"require_parameters": True}
    assert body["response_format"]["json_schema"]["strict"] is True
    assert body["response_format"]["json_schema"]["schema"]["minItems"] == 2


def test_gold_metrics_are_exact_and_field_based(rows, gazetteer):
    gold = [annotation(0), annotation(1, "高雄市", "鼓山區")]
    predicted = [annotation(0), annotation(1, "高雄市", "鼓山區")]

    metrics = benchmark.evaluate_annotations(rows, predicted, gazetteer, gold)

    assert metrics["gold_accuracy"]["exact"] == 1
    assert metrics["gold_accuracy"]["fields"]["county"] == 1


def test_without_gold_report_never_claims_accuracy(rows, gazetteer):
    metrics = benchmark.evaluate_annotations(rows, [annotation(0), annotation(1)], gazetteer)
    report = {
        "note": "Accuracy is omitted without gold labels; consistency is not correctness.",
        "models": [{"model": "model-a", "metrics": metrics, "latency_seconds": 0.1,
                    "manual_review": []}],
    }

    assert "gold_accuracy" not in metrics
    assert "Gold exact accuracy" not in benchmark._markdown(report)
