# ISOHE Port Marine on HiCloud VM

`isohe.ihmt.gov.tw` must be polled from a Taiwan egress.  Keep the Zeabur
collector disabled; this VM mirror is the production runner after the marine
migration is applied and an operator has performed a Taiwan-IP smoke test.

- cadence: every 10 minutes at :08/:18/…/:58; `TP/KL/TC/KH/HL/SA/BD/AP` × Wave/Current/Tide/Wind.
- write path (2026-10-12 fix): `marine_write.py` is a byte-identical copy of
  `storage/marine_write.py` (test-enforced). Only rows within 3h of the DB's latest
  valid observation are sent; upserts skip unchanged rows and never let an older
  batch (buffer replay) overwrite a newer one; stations are written only when the
  roster hash changes or every 6h (state in `$DATA_DIR/state/station_roster.json`);
  each transaction sets statement_timeout 120s / lock_timeout 10s; the run
  self-terminates after 480s (`SIGALRM`, default disposition) and refuses to
  start while a previous run holds `$DATA_DIR/state/collect.lock`.
- excluded: `MZ` legacy XML (not a failed member of the v1 API roster).
- output: local JSON snapshots for three days, then immutable S3 archive under
  `isohe_port_marine/archives/`; DB writes use the same canonical station,
  history and current tables as `collectors/isohe_port_marine.py`.
- tide: retain `tide_twvd`, `tide_cdl`, `tide_ref` separately. Never average or
  convert them until a documented datum policy is approved.

Deploy gate: migration applied, `SUPABASE_DB_URL` and S3 credentials supplied
on the VM, `test_egress.py` returns HTTP 200 for a TP endpoint, then one manual
dry run validates current-upsert and archive creation. This directory is not
installed or scheduled by this change.

For deployment, copy the runner, normalizer, `marine_write.py`, `external/vm_common/vm_buffer.py`, archiver, setup scripts, egress test and `.env.example`.
`marine_observation.py` is deliberately
standalone so the VM does not need a Git checkout; any semantic change must be
made in the tested main normalizer first, then mirrored here in the same review.

`setup_vm.sh` only installs files and dependencies. It deliberately does not
schedule collection. After migration apply, run `test_egress.py`, one manual
collector smoke, and one archive/S3 verification; only then run `setup_cron.sh`.
`vm_buffer.py` makes a DB failure fail closed (cron receives non-zero) while
retaining the normalized batch for the next 10-minute run.
