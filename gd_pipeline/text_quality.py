from __future__ import annotations

import re
from typing import Any


_RTF_HEX_ESCAPE_RE = re.compile(r"\\'[0-9a-fA-F]{2}")
_RTF_CONTROL_WORD_RE = re.compile(r"\\[a-zA-Z]{2,32}-?\d* ?")
_LONG_HEX_RUN_RE = re.compile(r"\b[0-9A-Fa-f]{64,}\b")

# Lower-case only. Signals typical for broken RTF/OOXML binary dumps.
_NOISE_KEYWORDS = (
    "times new roman",
    "calibri",
    "cambria",
    "arial",
    "tahoma",
    "default paragraph font",
    "normal table",
    "table grid",
    "word/2003/wordml",
    "[content_types].xml",
    "mso",
    "mergformat",
    "schemas.microsoft.com",
    "504b0304",
    "d0cf11e0",
)


def _is_cyrillic_char(ch: str) -> bool:
    code = ord(ch)
    return 0x0400 <= code <= 0x052F


def analyze_text_quality(text: str | None) -> dict[str, Any]:
    if not isinstance(text, str):
        return {"is_garbage": False, "score": 0, "reasons": "not_str"}

    sample = text[:120_000]
    if not sample.strip() or len(sample.strip()) < 120:
        return {"is_garbage": False, "score": 0, "reasons": "too_short"}

    total_len = max(1, len(sample))
    lower = sample.lower()

    slash_brace_count = sample.count("\\") + sample.count("{") + sample.count("}")
    slash_brace_ratio = slash_brace_count / total_len
    rtf_escape_count = len(_RTF_HEX_ESCAPE_RE.findall(sample))
    control_word_count = len(_RTF_CONTROL_WORD_RE.findall(sample))
    long_hex_count = len(_LONG_HEX_RUN_RE.findall(sample))
    keyword_hits = sum(1 for kw in _NOISE_KEYWORDS if kw in lower)
    control_chars = sum(1 for ch in sample if ord(ch) < 32 and ch not in "\n\r\t")
    control_ratio = control_chars / total_len

    alpha_count = sum(1 for ch in sample if ch.isalpha())
    cyr_count = sum(1 for ch in sample if _is_cyrillic_char(ch))
    cyr_ratio = (cyr_count / alpha_count) if alpha_count else 0.0

    score = 0
    reasons: list[str] = []

    if slash_brace_ratio >= 0.12:
        score += 2
        reasons.append("slash_brace_ratio>=0.12")
    elif slash_brace_ratio >= 0.07:
        score += 1
        reasons.append("slash_brace_ratio>=0.07")

    if rtf_escape_count >= 30:
        score += 2
        reasons.append("rtf_escape_count>=30")
    elif rtf_escape_count >= 10:
        score += 1
        reasons.append("rtf_escape_count>=10")

    if control_word_count >= 80:
        score += 2
        reasons.append("control_word_count>=80")
    elif control_word_count >= 30:
        score += 1
        reasons.append("control_word_count>=30")

    if long_hex_count >= 2:
        score += 2
        reasons.append("long_hex_count>=2")
    elif long_hex_count >= 1:
        score += 1
        reasons.append("long_hex_count>=1")

    if keyword_hits >= 6:
        score += 2
        reasons.append("keyword_hits>=6")
    elif keyword_hits >= 3:
        score += 1
        reasons.append("keyword_hits>=3")

    if control_ratio >= 0.01:
        score += 1
        reasons.append("control_ratio>=0.01")

    if alpha_count >= 300 and cyr_ratio < 0.30:
        if rtf_escape_count >= 5 or keyword_hits >= 2 or long_hex_count >= 1 or slash_brace_ratio >= 0.05:
            score += 1
            reasons.append("low_cyr_ratio_with_noise")

    strong_signal = (
        rtf_escape_count >= 60
        or long_hex_count >= 3
        or (keyword_hits >= 8 and slash_brace_ratio >= 0.08)
    )
    if strong_signal and "strong_signal" not in reasons:
        reasons.append("strong_signal")

    is_garbage = strong_signal or score >= 3
    return {
        "is_garbage": is_garbage,
        "score": score,
        "reasons": ",".join(reasons) if reasons else "none",
        "slash_brace_ratio": round(slash_brace_ratio, 4),
        "rtf_escape_count": rtf_escape_count,
        "control_word_count": control_word_count,
        "long_hex_count": long_hex_count,
        "keyword_hits": keyword_hits,
        "cyr_ratio": round(cyr_ratio, 4),
    }


def is_probably_garbage_text(text: str | None) -> bool:
    return bool(analyze_text_quality(text).get("is_garbage"))


def garbage_reason_summary(text: str | None) -> str:
    analysis = analyze_text_quality(text)
    return (
        f"score={analysis.get('score', 0)};"
        f"reasons={analysis.get('reasons', 'none')};"
        f"cyr_ratio={analysis.get('cyr_ratio', 0)};"
        f"rtf_escape_count={analysis.get('rtf_escape_count', 0)};"
        f"keyword_hits={analysis.get('keyword_hits', 0)}"
    )

