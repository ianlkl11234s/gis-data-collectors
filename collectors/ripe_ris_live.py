"""Registry shim for the persistent RIPE RIS Live worker."""

from .base import BaseCollector


class RipeRisLiveCollector(BaseCollector):
    name = "ripe_ris_live"
    interval_minutes = 5

    def collect(self) -> dict:
        raise RuntimeError("RIPE RIS Live uses workers.RipeRisLiveWorker")
