# Generic raw archive verification

Raw date directories may be removed only after a full remote tar member readback verifies every regular JSON filename, SHA-256 and byte count, and an atomic local receipt records that manifest and remote identity. Legacy gzip timestamps do not affect content equivalence. Verification pins GET to HEAD VersionId or ETag and closes its stream.

Subsequent runs compare the current local manifest and remote HEAD identity with that receipt. They do not recompress or download the same archive again. A missing remote archive is compressed into a temporary file, uploaded and read back before a receipt is issued; mismatched or inaccessible archives are retained and never overwritten by this task. Compression requires source bytes plus 1 MiB free space. Cleanup requires no compression space, so a full disk does not block verified cleanup.

Cleanup preserves any directory containing unknown files, symlinks or subdirectories. Current-day raw and the existing collector-specific retention windows remain unchanged. Existing GFW licence gates remain unchanged. Receipts are verification evidence, not a replacement for the archive. Cloud deployment and a live restore/readback are separate acceptance gates.

Validation: 17 archive lifecycle and GFW tests passed on 2026-09-18. Tests include corrupt/unknown remote content, changed local content, low-space receipt cleanup and unknown-file preservation.
