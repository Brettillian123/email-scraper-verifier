from __future__ import annotations

from functools import lru_cache
from pathlib import Path

# Location of the stopword file (same package as this module)
_STOPWORD_FILE = Path(__file__).with_name("name_stopwords.txt")


@lru_cache(maxsize=1)
def load_name_stopwords() -> set[str]:
    """
    Load name stopwords from the text file.

    - One token per line.
    - Lines starting with '#' or blank lines are ignored.
    - All tokens are normalized to lowercase.
    """
    words: set[str] = set()

    if not _STOPWORD_FILE.exists():
        # Fail-safe: return empty set if file is missing.
        return words

    with _STOPWORD_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            words.add(raw.lower())

    return words


# Convenience alias for modules that just want the set
NAME_STOPWORDS: set[str] = load_name_stopwords()
