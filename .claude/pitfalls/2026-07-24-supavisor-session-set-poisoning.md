# 2026-07-24：Supavisor transaction pool 下 session 級 SET，毒到共用 backend 致 collector 唯讀爆炸

## 症狀

realtime→live schema 搬遷日，主站 collector 突然大面積寫入失敗：

```
cannot execute INSERT in a read-only transaction
```

DB 本身健康、權限沒動過，但錯誤「隨機、時好時壞」——同一 collector 有的輪次成功、有的失敗。

## 根因

盤點 agent 用 psycopg2 `set_session(readonly=True)` 連 **Supavisor transaction pool（port 6543）** 做唯讀盤點：

- transaction mode 下 client 與 Postgres backend **不是 1:1**：每個 transaction 結束後 backend 丟回池子給別的 client 用
- `set_session()` 發的是 session 級 `SET default_transaction_read_only = on`，**殘留在 backend 上，不會隨 transaction 結束還原**
- 盤點 agent 斷線後，2 條被毒的 backend 回到池子 → 哪個 collector 抽到哪條就爆，形成隨機唯讀錯誤

## 修復

- **解毒**：多執行緒併發搶連線，抽到中毒 backend 就 `SET default_transaction_read_only = off`，直到 2 條全數復原
- **防呆**：sentinel `_common.py` 補丁，禁止對 6543 連線做 session 級設定

## 給後人的教訓

1. **走 6543（transaction pool）絕不可下 session 級 SET**——包含 psycopg2 `set_session()`、不帶 LOCAL 的 `SET`，任何 session 狀態都會殘留毒到共用連線的其他 client
2. 唯讀需求的正確做法：**autocommit + 純 SELECT**，或 `SET LOCAL`（transaction 結束自動還原）
3. 真的需要 session 狀態（cursor、temp table、session GUC）→ **走 5432 direct / session mode**
4. 「隨機、時好時壞」的 DB 錯誤要想到連線池污染：錯誤跟著 backend 走，不跟著 client 走
