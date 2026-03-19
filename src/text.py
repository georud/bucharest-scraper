"""Text normalization utilities for cleaning scraped listing data."""

_UNICODE_REPLACEMENTS = {
    "\u2014": "-",   # em dash
    "\u2013": "-",   # en dash
    "\u2018": "'",   # left single quote
    "\u2019": "'",   # right single quote
    "\u201c": '"',   # left double quote
    "\u201d": '"',   # right double quote
    "\u2026": "...", # ellipsis
    "`": "'",        # backtick (breaks Folium JS template literals)
}

_TRANS_TABLE = str.maketrans(_UNICODE_REPLACEMENTS)


def normalize_text(text: str) -> str:
    """Replace special Unicode punctuation with ASCII equivalents."""
    return text.translate(_TRANS_TABLE)
