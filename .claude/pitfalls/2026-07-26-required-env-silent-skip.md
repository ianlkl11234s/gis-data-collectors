# required_env 未在 config.py 宣告 → main.py silent skip

> 記錄日 2026-07-26，事故日不詳（約 2026-04，USWG collector 上線時）。原記在 CLAUDE.md 常見踩雷，瘦身時搬出成檔。

## 症狀

新 collector 部署後完全沒有跑，不報錯、不 crash，啟動 log 只有一行不起眼的：

```
⚠️  IOW_CLIENT_ID 未設定，跳過 都市淹水感知器收集器（USWG）
```

env 明明已在 Zeabur 平台設好，卻被判定「未設定」。USWG collector 曾因此燒掉一小時排查。

## 根因

`main.py` 檢查 `required_env` 的方式是讀 **config 模組屬性**，不是直接讀環境變數：

```python
missing = [k for k in entry.required_env if not getattr(config, k, None)]
```

所以 `collectors/registry.py` 的 `required_env` 只是名單；變數本體**必須在 `config.py` 用 `os.getenv` 宣告**成模組屬性。漏宣告 → `getattr` 永遠拿到 None → collector 被 silent skip。

## 解法

新增 collector 時（必要步驟表步驟 3 + 4）兩邊缺一不可：

1. `collectors/registry.py` 的 `CollectorEntry` 填 `required_env=("XXX_KEY",)`
2. `config.py` 同步宣告 `XXX_KEY = os.getenv('XXX_KEY')`

## 給後人的教訓

1. **skip 不是 error**：啟動 log 的 ⚠️ 行是唯一線索，部署後務必掃一眼啟動 log
2. **required_env 是雙檔契約**：registry 宣告名單、config 宣告本體，只改一邊就是 silent skip
