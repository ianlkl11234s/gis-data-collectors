# GFW v4 daily production boundary

The East Asia v4 drivers remain local POCs.  They are not scheduled and must
not publish their output directly: the drivers use local artifacts and the
Fishing Effort helper retains a local raw payload for POC evidence.

`tasks/gfw_v4_daily_publish.py` defines the formal safety boundary.  A future
live adapter must provide a reviewed `gfw_v4_normalized_daily_source` envelope
with a resolved dataset version, complete pagination proof, normalized records
only, and `raw_response_saved=false`.  Until that adapter and a manifest-last
publisher are reviewed together, `GFWV4DailyPublishTask.run()` raises before
any network, DB ledger, S3, or Cloudflare side effect.
