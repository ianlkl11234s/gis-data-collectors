"""離線評估新聞地點 evidence POC；不讀 DB、網路或任何 credential。"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Sequence

if __package__ in (None, ''):  # 支援 `python3 scripts/...py` 直接執行
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collectors.news_events import (  # noqa: E402
    build_article_relation_candidates,
    build_location_evidence,
)


DEFAULT_LIMIT = 200
MAX_MANUAL_REVIEW_ROWS = 20
DEFAULT_JSON_OUTPUT = Path('/tmp/news_location_evidence_evaluation.json')
DEFAULT_MARKDOWN_OUTPUT = Path('/tmp/news_location_evidence_evaluation.md')
MANUAL_REVIEW_NOTICE = (
    '人工審閱欄位只供抽樣與後續查核，並非 accuracy 驗證，'
    '也不代表該位置具有 geometry eligibility 或可畫點資格。'
)


class JsonlInputError(ValueError):
    """輸入 JSONL 不完整或不合法時 fail closed。"""


def bounded_limit(value: str | int) -> int:
    """CLI 與呼叫端共用的安全上限：只允許 1..200。"""
    try:
        limit = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError('limit must be an integer from 1 to 200') from error
    if not 1 <= limit <= DEFAULT_LIMIT:
        raise argparse.ArgumentTypeError('limit must be in the range 1..200')
    return limit


def read_articles_jsonl(input_path: Path, limit: int) -> list[dict]:
    """完整驗證至 limit 範圍內的 JSONL，發現任何錯誤即不產生評估結果。"""
    checked_limit = bounded_limit(limit)
    articles = []
    try:
        with input_path.open(encoding='utf-8') as stream:
            for line_number, raw_line in enumerate(stream, start=1):
                if len(articles) >= checked_limit:
                    break
                if not raw_line.strip():
                    raise JsonlInputError(f'{input_path}:{line_number}: empty JSONL line')
                try:
                    value = json.loads(raw_line)
                except json.JSONDecodeError as error:
                    raise JsonlInputError(
                        f'{input_path}:{line_number}: invalid JSON ({error.msg})'
                    ) from error
                if not isinstance(value, dict):
                    raise JsonlInputError(f'{input_path}:{line_number}: JSONL record must be an object')
                articles.append(value)
    except OSError as error:
        raise JsonlInputError(f'cannot read {input_path}: {error}') from error
    if not articles:
        raise JsonlInputError(f'{input_path}: no JSONL records; refusing to create an empty report')
    return articles


def evaluate_articles(articles: list[dict]) -> dict:
    """評估已載入文章；只呼叫 news_events 的純 helper。"""
    if not articles:
        raise JsonlInputError('no articles supplied; refusing to create an empty report')
    evaluations = []
    scopes = Counter()
    statuses = Counter()
    precisions = Counter()
    evidence_fields = Counter()

    for index, article in enumerate(articles):
        evidence = build_location_evidence(article).to_dict()
        scopes[evidence['location_scope']] += 1
        statuses[evidence['location_status']] += 1
        precisions[evidence['location_precision']] += 1
        evidence_fields[evidence['evidence_field'] or 'none'] += 1
        article_key = str(
            article.get('article_id') or article.get('url_norm') or article.get('url') or index
        )
        evaluations.append({
            'article_key': article_key,
            'title': str(article.get('title') or ''),
            'location_evidence': evidence,
        })

    relations = [candidate.to_dict() for candidate in build_article_relation_candidates(articles)]
    return {
        'evaluation_method': 'news-location-evidence-poc/v1',
        'article_count': len(articles),
        'statistics': {
            'location_scope': dict(sorted(scopes.items())),
            'location_status': dict(sorted(statuses.items())),
            'location_precision': dict(sorted(precisions.items())),
            'evidence_field': dict(sorted(evidence_fields.items())),
            'accepted_count': statuses['accepted'],
            'unresolved_count': statuses['unresolved'],
            'relation_candidate_count': len(relations),
        },
        'articles': evaluations,
        'relation_candidates': relations,
        'manual_review_notice': MANUAL_REVIEW_NOTICE,
    }


def evaluate_jsonl(input_path: Path, limit: int) -> dict:
    """先完整讀取與驗證，再評估；壞 JSONL 不會留下部分結果。"""
    return evaluate_articles(read_articles_jsonl(Path(input_path), limit))


def _counter_markdown(title: str, values: dict) -> list[str]:
    lines = [f'## {title}', '', '| 值 | 件數 |', '| --- | ---: |']
    lines.extend(f'| {key or "none"} | {value} |' for key, value in values.items())
    return lines + ['']


def _markdown_cell(value: object, max_length: int = 80) -> str:
    text = str(value or '—').replace('|', '\\|').replace('\n', ' ').replace('\r', ' ')
    return text if len(text) <= max_length else f'{text[:max_length - 1]}…'


def _manual_review_markdown(articles: list[dict]) -> list[str]:
    """最多 20 筆供人檢視；這不是 accuracy 或 geometry 判定。"""
    lines = [
        f'## Bounded manual review（最多 {MAX_MANUAL_REVIEW_ROWS} 筆）',
        '',
        '| article_key | title | scope | status | precision | evidence_field | evidence_text |',
        '| --- | --- | --- | --- | --- | --- | --- |',
    ]
    for article in articles[:MAX_MANUAL_REVIEW_ROWS]:
        evidence = article['location_evidence']
        lines.append(
            '| {article_key} | {title} | {scope} | {status} | {precision} | {field} | {text} |'.format(
                article_key=_markdown_cell(article['article_key'], 48),
                title=_markdown_cell(article['title']),
                scope=_markdown_cell(evidence['location_scope'], 32),
                status=_markdown_cell(evidence['location_status'], 32),
                precision=_markdown_cell(evidence['location_precision'], 32),
                field=_markdown_cell(evidence['evidence_field'], 32),
                text=_markdown_cell(evidence['evidence_text']),
            )
        )
    return lines + ['']


def render_markdown(result: dict) -> str:
    """輸出可閱讀的統計摘要，明確保留人工 review 的證據邊界。"""
    stats = result['statistics']
    lines = [
        '# News location evidence POC evaluation',
        '',
        f"- 評估文章：{result['article_count']}",
        f"- accepted：{stats['accepted_count']}；unresolved：{stats['unresolved_count']}",
        f"- 同稿 relation candidates：{stats['relation_candidate_count']}",
        f'- 注意：{result["manual_review_notice"]}',
        '',
    ]
    for title, values in (
        ('location_scope', stats['location_scope']),
        ('location_status', stats['location_status']),
        ('location_precision', stats['location_precision']),
        ('evidence_field', stats['evidence_field']),
    ):
        lines.extend(_counter_markdown(title, values))
    lines.extend(_manual_review_markdown(result['articles']))
    return '\n'.join(lines)


def write_outputs(result: dict, json_output: Path, markdown_output: Path) -> None:
    """在 caller 指定的本地路徑輸出，不會寫入 live、DB 或 input。"""
    json_output = Path(json_output)
    markdown_output = Path(markdown_output)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    markdown_output.write_text(render_markdown(result), encoding='utf-8')


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input_jsonl', type=Path, help='本地 article JSONL（每列一個 object）')
    parser.add_argument('--limit', type=bounded_limit, default=DEFAULT_LIMIT, help='1..200，預設 200')
    parser.add_argument('--json-output', type=Path, default=DEFAULT_JSON_OUTPUT)
    parser.add_argument('--markdown-output', type=Path, default=DEFAULT_MARKDOWN_OUTPUT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = evaluate_jsonl(args.input_jsonl, args.limit)
        write_outputs(result, args.json_output, args.markdown_output)
    except JsonlInputError as error:
        print(f'news location evidence evaluation failed closed: {error}', file=sys.stderr)
        return 2
    print(
        f"evaluated {result['article_count']} articles; "
        f"wrote {args.json_output} and {args.markdown_output}"
    )
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
