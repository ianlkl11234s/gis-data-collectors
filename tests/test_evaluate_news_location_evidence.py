"""離線新聞地點 evidence runner 的測試。"""

import json

import pytest

import collectors.news_events as news_events
from scripts.evaluate_news_location_evidence import (
    DEFAULT_LIMIT,
    JsonlInputError,
    bounded_limit,
    evaluate_jsonl,
    main,
    render_markdown,
    write_outputs,
)


def _write_jsonl(path, rows):
    path.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows), encoding='utf-8')


class TestEvaluateNewsLocationEvidence:

    def test_evaluates_bounded_input_and_reports_contract_counts(self, tmp_path, monkeypatch):
        input_path = tmp_path / 'articles.jsonl'
        _write_jsonl(input_path, [
            {'article_id': 'a', 'title': '鹿草鄉農路事故', 'county_hint': '雲林縣'},
            {'article_id': 'b', 'title': '日本北海道強震'},
            {'article_id': 'c', 'title': '市場價格波動'},
            {'article_id': 'd', 'title': '市場價格波動'},
        ])

        class NoConfigAccess:
            def __getattr__(self, name):
                raise AssertionError(f'offline runner must not access config.{name}')

        monkeypatch.setattr(news_events, 'config', NoConfigAccess())
        result = evaluate_jsonl(input_path, limit=3)

        assert result['article_count'] == 3
        assert result['statistics']['location_scope'] == {
            'foreign': 1, 'taiwan_local': 1, 'unknown': 1,
        }
        assert result['statistics']['accepted_count'] == 2
        assert result['statistics']['unresolved_count'] == 1
        assert result['articles'][0]['location_evidence']['resolved_county'] == '嘉義縣'
        assert result['manual_review_notice'] in render_markdown(result)

    def test_relation_candidates_and_output_files_are_local_artifacts(self, tmp_path):
        input_path = tmp_path / 'articles.jsonl'
        _write_jsonl(input_path, [
            {'article_id': 'cna', 'title': '高雄鼓山民宅火警 消防出動30人搶救'},
            {'article_id': 'ltn', 'title': '高雄鼓山民宅火警　消防出動30人搶救 - 自由時報'},
        ])
        result = evaluate_jsonl(input_path, limit=DEFAULT_LIMIT)
        json_output = tmp_path / 'evaluation.json'
        markdown_output = tmp_path / 'evaluation.md'
        write_outputs(result, json_output, markdown_output)

        assert result['statistics']['relation_candidate_count'] == 1
        assert result['relation_candidates'][0]['relation_type'] == 'same_story_candidate'
        assert json.loads(json_output.read_text(encoding='utf-8')) == result
        assert '並非 accuracy 驗證' in markdown_output.read_text(encoding='utf-8')

    def test_markdown_manual_review_is_bounded_and_has_evidence_columns(self, tmp_path):
        input_path = tmp_path / 'articles.jsonl'
        _write_jsonl(input_path, [
            {'article_id': f'item-{index}', 'title': f'鹿草鄉測試標題 {index}'}
            for index in range(21)
        ])

        markdown = render_markdown(evaluate_jsonl(input_path, limit=DEFAULT_LIMIT))

        assert '## Bounded manual review（最多 20 筆）' in markdown
        assert '| article_key | title | scope | status | precision | evidence_field | evidence_text |' in markdown
        assert markdown.count('| item-') == 20
        assert '| item-20 |' not in markdown

    def test_invalid_jsonl_fails_closed_before_outputs_are_written(self, tmp_path):
        input_path = tmp_path / 'bad.jsonl'
        input_path.write_text('{"title": "有效"}\nnot-json\n', encoding='utf-8')
        json_output = tmp_path / 'should-not-exist.json'
        markdown_output = tmp_path / 'should-not-exist.md'

        assert main([
            str(input_path), '--json-output', str(json_output), '--markdown-output', str(markdown_output),
        ]) == 2
        assert not json_output.exists()
        assert not markdown_output.exists()

    @pytest.mark.parametrize('contents', ('', '\n'))
    def test_empty_jsonl_fails_closed_without_empty_report(self, tmp_path, contents):
        input_path = tmp_path / 'empty.jsonl'
        input_path.write_text(contents, encoding='utf-8')
        json_output = tmp_path / 'empty-report.json'
        markdown_output = tmp_path / 'empty-report.md'

        assert main([
            str(input_path), '--json-output', str(json_output), '--markdown-output', str(markdown_output),
        ]) == 2
        assert not json_output.exists()
        assert not markdown_output.exists()

    @pytest.mark.parametrize('value', ('0', '201', '-1', 'not-a-number'))
    def test_limit_is_strictly_bounded(self, value):
        with pytest.raises(SystemExit):
            # argparse wraps ArgumentTypeError as SystemExit for its public CLI surface.
            main(['input.jsonl', '--limit', value])

    def test_bounded_limit_accepts_supported_range(self):
        assert bounded_limit('1') == 1
        assert bounded_limit(DEFAULT_LIMIT) == DEFAULT_LIMIT

    def test_non_object_jsonl_record_fails_closed(self, tmp_path):
        input_path = tmp_path / 'array.jsonl'
        input_path.write_text('[]\n', encoding='utf-8')
        with pytest.raises(JsonlInputError, match='must be an object'):
            evaluate_jsonl(input_path, limit=1)
