"""錯誤訊息脫敏：遮掉 URL query 中的金鑰類參數值與 Bearer token。

requests 的 HTTPError 訊息含完整 URL（例如 ?Authorization=<key>&format=JSON），
不可原樣 print、通知或寫進 collector_status。
"""

import re

REDACTED = "<REDACTED>"

# ?name=value / &name=value：name 含 authorization、key、token 等字樣即遮值
_QUERY_SECRET = re.compile(
    r"(?P<pre>[?&;])(?P<name>[\w.\-]*(?:authorization|api[_\-]?key|apikey|token|secret|"
    r"password|passwd|signature|key|sig|auth)[\w.\-]*)=(?P<val>[^&\s'\")#]*)",
    re.IGNORECASE,
)
_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=\-]{8,}")
_HEADER = re.compile(
    r"(?i)\b(authorization|x-api-key|api-key)(\s*[:=]\s*)(?!<REDACTED>)[^\s,;'\"}]+(?:\s+[A-Za-z0-9._~+/=\-]{8,})?"
)


def redact_secrets(text) -> str:
    """回傳已遮蔽金鑰的字串（None 回空字串）。"""
    if text is None:
        return ""
    s = str(text)
    s = _QUERY_SECRET.sub(lambda m: f"{m.group('pre')}{m.group('name')}={REDACTED}", s)
    s = _BEARER.sub(lambda m: f"{m.group(1)} {REDACTED}", s)
    s = _HEADER.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", s)
    return s
