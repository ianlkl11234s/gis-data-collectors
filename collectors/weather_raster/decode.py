"""網格圖磚解碼：調色盤顏色 → uint8 級距（ADR-0021 G 流程 decode）。

規則：
  * 0 = 無（透明）、255 = 缺測（MISSING）、1..254 = 解碼表級距。
  * P-mode PNG 直接取 palette index，每磚對 PLTE 建一次 index→level LUT（不組全幅 RGBA）。
  * 表外顏色：計數並回報（unknown_pixels / unknown_colors），對應像素設 MISSING，不可歸 0。
  * 解碼表放 config/decode_tables/{source}/{product}.yaml，版本化；原始圖磚永遠在冷層，可重算。
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import yaml
from PIL import Image

MISSING = 255
TILE = 256
BLANK_PLACEHOLDER_BYTES = 334  # JMA 未繪製層級／無內容時回的透明 RGBA 佔位圖

DECODE_TABLE_DIR = Path(__file__).resolve().parents[2] / "config" / "decode_tables"


@dataclass
class DecodeTable:
    version: str
    product: str
    levels: dict[tuple[int, int, int], int]
    labels: dict[int, str]
    thresholds: list[int]
    rgb_tolerance: int = 0
    plte_verified: bool = False
    raw: dict = field(default_factory=dict)

    @classmethod
    def load(cls, source: str, product: str, base: Path = DECODE_TABLE_DIR) -> "DecodeTable":
        doc = yaml.safe_load((base / source / f"{product}.yaml").read_text(encoding="utf-8"))
        levels = {tuple(int(c) for c in lv["rgb"]): int(lv["level"]) for lv in doc["levels"]}
        if any(not 1 <= v <= 254 for v in levels.values()):
            raise ValueError(f"{product}: level 必須在 1..254（0=無、255=缺測保留）")
        return cls(
            version=str(doc["version"]),
            product=product,
            levels=levels,
            labels={int(lv["level"]): str(lv.get("label", "")) for lv in doc["levels"]},
            thresholds=[int(t["k"]) for t in doc.get("count_thresholds", [])],
            rgb_tolerance=int(doc.get("rgb_tolerance", 0)),
            plte_verified=bool(doc.get("plte_verified", False)),
            raw=doc,
        )

    def level_of(self, rgb: tuple[int, int, int]) -> Optional[int]:
        lv = self.levels.get(rgb)
        if lv is not None or self.rgb_tolerance <= 0:
            return lv
        best, best_d = None, self.rgb_tolerance + 1
        for ref, v in self.levels.items():
            d = max(abs(a - b) for a, b in zip(ref, rgb))
            if d < best_d:
                best, best_d = v, d
        return best


@dataclass
class TileDecode:
    levels: np.ndarray            # (256,256) uint8
    nonzero: int                  # 1..254 的像素數
    unknown_pixels: int
    unknown_colors: dict[str, int]

    @property
    def is_blank(self) -> bool:
        return self.nonzero == 0 and self.unknown_pixels == 0


def _alpha_per_index(im: Image.Image, n: int) -> np.ndarray:
    alpha = np.full(n, 255, np.uint8)
    trns = im.info.get("transparency")
    if isinstance(trns, (bytes, bytearray)):
        k = min(len(trns), n)
        alpha[:k] = np.frombuffer(bytes(trns[:k]), np.uint8)
    elif isinstance(trns, int) and 0 <= trns < n:
        alpha[trns] = 0
    return alpha


def blank_tile() -> TileDecode:
    return TileDecode(np.zeros((TILE, TILE), np.uint8), 0, 0, {})


def missing_tile() -> TileDecode:
    return TileDecode(np.full((TILE, TILE), MISSING, np.uint8), 0, 0, {})


def decode_tile(data: bytes, table: DecodeTable) -> TileDecode:
    """PNG bytes → 級距。334B 佔位（透明 RGBA）→ 全 0。"""
    im = Image.open(io.BytesIO(data))
    if im.size != (TILE, TILE):
        raise ValueError(f"tile size {im.size} != 256x256")
    unknown: dict[str, int] = {}
    if im.mode == "P":
        idx = np.asarray(im, dtype=np.uint8)
        pal = im.getpalette() or []
        n = max(len(pal) // 3, int(idx.max()) + 1)
        pal = list(pal) + [0] * (n * 3 - len(pal))
        alpha = _alpha_per_index(im, n)
        lut = np.zeros(256, np.uint8)
        bad = np.zeros(256, bool)
        for i in range(n):
            if alpha[i] == 0:
                continue
            rgb = (pal[3 * i], pal[3 * i + 1], pal[3 * i + 2])
            lv = table.level_of(rgb)
            if lv is None:
                lut[i] = MISSING
                bad[i] = True
            else:
                lut[i] = lv
        out = lut[idx]
        if bad.any():
            counts = np.bincount(idx.ravel(), minlength=256)
            for i in np.nonzero(bad & (counts > 0))[0]:
                rgb = (pal[3 * i], pal[3 * i + 1], pal[3 * i + 2])
                unknown["#%02x%02x%02x" % rgb] = unknown.get("#%02x%02x%02x" % rgb, 0) + int(counts[i])
    else:
        a = np.asarray(im.convert("RGBA"), dtype=np.uint8)
        out = np.zeros((TILE, TILE), np.uint8)
        opaque = a[..., 3] > 0
        if opaque.any():
            rgb = a[..., :3][opaque]
            codes = (rgb[:, 0].astype(np.int32) << 16) | (rgb[:, 1].astype(np.int32) << 8) | rgb[:, 2]
            uniq, inv, cnt = np.unique(codes, return_inverse=True, return_counts=True)
            vals = np.empty(len(uniq), np.uint8)
            for j, c in enumerate(uniq):
                rgbt = (int(c) >> 16 & 255, int(c) >> 8 & 255, int(c) & 255)
                lv = table.level_of(rgbt)
                if lv is None:
                    vals[j] = MISSING
                    unknown["#%02x%02x%02x" % rgbt] = unknown.get("#%02x%02x%02x" % rgbt, 0) + int(cnt[j])
                else:
                    vals[j] = lv
            out[opaque] = vals[inv.ravel()]
    nonzero = int(np.count_nonzero((out > 0) & (out < MISSING)))
    return TileDecode(out, nonzero, int(sum(unknown.values())), unknown)


def pool_max(levels: np.ndarray, k: int) -> np.ndarray:
    """k×k 取最大；MISSING(255) 只在整塊都缺測時保留（否則以有效最大值為準）。"""
    h, w = levels.shape
    blocks = levels.reshape(h // k, k, w // k, k)
    valid = np.where(blocks == MISSING, 0, blocks).max(axis=(1, 3))
    all_missing = (blocks == MISSING).all(axis=(1, 3))
    valid[all_missing] = MISSING
    return valid.astype(np.uint8)


def upsample(levels: np.ndarray, k: int) -> np.ndarray:
    return np.repeat(np.repeat(levels, k, axis=0), k, axis=1)
