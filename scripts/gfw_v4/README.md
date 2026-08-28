# GFW East Asia v4 local shadow POC drivers

These scripts are local-only, immutable-output helpers for the fixed v4 POC
bbox `(115.93462, 20.36314, 134.73486, 36.52495)`. They do not write
Supabase, S3, Cloudflare, or production collector state. Output directories
must not already exist.

Run from the data-collectors checkout (or use absolute input/output paths):

```sh
python3 scripts/gfw_v4/build_tracks.py \
  --input <high-phase>/high.ndjson \
  --output-dir <tracks-output> --selected-day 2026-08-21
python3 scripts/gfw_v4/build_grid.py \
  --presence-compare <phase>/presence-compare.sqlite \
  --output-dir <grid-output> --selected-day 2026-08-21
python3 scripts/gfw_v4/build_fishing.py \
  --output-dir <fishing-output> --selected-day 2026-08-21 \
  --env-file <checkout>/.env
python3 scripts/gfw_v4/probe_fishing_latest.py \
  --output-dir <latest-probe-output> --env-file <checkout>/.env
python3 scripts/gfw_v4/finalize_shadow.py \
  --root <release-root> --phase-root <phase> \
  --fishing-root <fishing-output> --latest-probe-dir <latest-probe-output>
```

`GFW_ACCESS_TOKEN` is read only from the environment or the explicitly passed
dotenv file. It is never written to output or printed. The browser alias setup
belongs to Mini Taiwan Pulse and is documented with its own POC script.

The original session drivers remain outside this checkout as provenance; these
parameterized copies are the reproducible versions intended for review.
