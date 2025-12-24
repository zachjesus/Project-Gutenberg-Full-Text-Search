"""Full-text search using mv_books_dc materialized view with query builder."""
from __future__ import annotations
import html
import mimetypes
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker


__all__ = [
    "Config",
    "FullTextSearch",
    "SearchQuery",
    "SearchField",
    "SearchType",
    "OrderBy",
    "SortDirection",
    "FileType",
    "Encoding",
    "Crosswalk",
    "LanguageCode",
    "LoccClass",
    "LANGUAGE_LIST",
    "LOCC_TOP",
    "LOCC_HIERARCHY",
    "LANGUAGE_LABELS",
    "LOCC_LABELS",
    "CURATED_BOOKSHELVES",
    "get_locc_children",
    "get_locc_path",
    "get_broad_genres",
    "format_crosswalk_result",
    "strip_marc_subfields",
    "format_title",
]

from .constants import (
    LANGUAGE_LIST,
    LOCC_TOP,
    LOCC_HIERARCHY,
    LANGUAGE_LABELS,
    LOCC_LABELS,
    CURATED_BOOKSHELVES,
)


class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'


def get_locc_children(parent: str = "") -> list[dict]:
    """
    Very simple LOCC children: return every LOCC key that starts with the parent
    (except the parent itself). Top-level (empty parent) returns LOCC_TOP.
    """
    parent = (parent or "").strip().upper()

    if not parent:
        top_items = sorted(LOCC_TOP, key=lambda x: x['code'])
        return [
            {
                'code': item['code'],
                'label': item['label'],
                'has_children': any(c.startswith(item['code']) and c != item['code'] for c in LOCC_HIERARCHY.keys())
            }
            for item in top_items
        ]

    if parent not in LOCC_HIERARCHY:
        return []

    children_keys = [c for c in LOCC_HIERARCHY.keys() if c.startswith(parent) and c != parent]
    result = []
    for code in sorted(children_keys, key=lambda x: (len(x), x)):
        has_children = any(k.startswith(code) and k != code for k in LOCC_HIERARCHY.keys())
        result.append({
            'code': code,
            'label': LOCC_HIERARCHY.get(code, code),
            'has_children': has_children
        })
    return result

# =============================================================================
# Enums
# =============================================================================

class LanguageCode(str, Enum):
    """Gutenberg language codes (mirrors OPDS facet options)."""
    EN = "en"
    AF = "af"
    ALE = "ale"
    ANG = "ang"
    AR = "ar"
    ARP = "arp"
    BG = "bg"
    BGS = "bgs"
    BO = "bo"
    BR = "br"
    BRX = "brx"
    CA = "ca"
    CEB = "ceb"
    CS = "cs"
    CSB = "csb"
    CY = "cy"
    DA = "da"
    DE = "de"
    EL = "el"
    ENM = "enm"
    EO = "eo"
    ES = "es"
    ET = "et"
    FA = "fa"
    FI = "fi"
    FR = "fr"
    FUR = "fur"
    FY = "fy"
    GA = "ga"
    GL = "gl"
    GLA = "gla"
    GRC = "grc"
    HAI = "hai"
    HE = "he"
    HU = "hu"
    IA = "ia"
    ILO = "ilo"
    IS = "is"
    IT = "it"
    IU = "iu"
    JA = "ja"
    KHA = "kha"
    KLD = "kld"
    KO = "ko"
    LA = "la"
    LT = "lt"
    MI = "mi"
    MYN = "myn"
    NAH = "nah"
    NAI = "nai"
    NAP = "nap"
    NAV = "nav"
    NL = "nl"
    NO = "no"
    OC = "oc"
    OJI = "oji"
    PL = "pl"
    PT = "pt"
    RMQ = "rmq"
    RO = "ro"
    RU = "ru"
    SA = "sa"
    SCO = "sco"
    SL = "sl"
    SR = "sr"
    SV = "sv"
    TE = "te"
    TL = "tl"
    YI = "yi"
    ZH = "zh"


class LoccClass(str, Enum):
    """Library of Congress Classification top-level classes (mirrors OPDS facet options)."""
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"
    H = "H"
    J = "J"
    K = "K"
    L = "L"
    M = "M"
    N = "N"
    P = "P"
    Q = "Q"
    R = "R"
    S = "S"
    T = "T"
    U = "U"
    V = "V"
    Z = "Z"


class FileType(str, Enum):
    """MIME types for file filtering."""
    EPUB = "application/epub+zip"
    KINDLE = "application/x-mobipocket-ebook"
    PDF = "application/pdf"
    TXT = "text/plain"
    HTML = "text/html"


class Encoding(str, Enum):
    """Character encodings for file filtering."""
    ASCII = "us-ascii"
    UTF8 = "utf-8"
    LATIN1 = "iso-8859-1"
    WINDOWS1252 = "windows-1252"


class SearchType(str, Enum):
    """Search algorithm types."""
    FTS = "fts"
    FUZZY = "fuzzy"
    CONTAINS = "contains"


class SearchField(str, Enum):
    """Searchable fields."""
    BOOK = "book"
    TITLE = "title"
    AUTHOR = "author"
    SUBJECT = "subject"
    BOOKSHELF = "bookshelf"
    SUBTITLE = "subtitle"
    ATTRIBUTE = "attribute"


class OrderBy(str, Enum):
    """Sort options."""
    RELEVANCE = "relevance"
    DOWNLOADS = "downloads"
    TITLE = "title"
    AUTHOR = "author"
    RELEASE_DATE = "release_date"
    RANDOM = "random"


class SortDirection(str, Enum):
    """Sort direction."""
    ASC = "asc"
    DESC = "desc"


class Crosswalk(str, Enum):
    """Output format transformers."""
    FULL = "full"
    PG = "pg"
    OPDS = "opds"
    CUSTOM = "custom"
    MINI = "mini"


# =============================================================================
# Internal Configuration
# =============================================================================

_FIELD_COLS = {
    SearchField.BOOK:      ("tsvec",           "book_text"),
    SearchField.TITLE:     ("title_tsvec",     "title"),
    SearchField.SUBTITLE:  ("subtitle_tsvec",  "subtitle"),
    SearchField.AUTHOR:    ("author_tsvec",    "all_authors"),
    SearchField.SUBJECT:   ("subject_tsvec",   "all_subjects"),
    SearchField.BOOKSHELF: ("bookshelf_tsvec", "bookshelf_text"),
    SearchField.ATTRIBUTE: ("attribute_tsvec", "attribute_text"),
}

_TRIGRAM_FIELDS = {
    SearchField.BOOK, SearchField.TITLE, SearchField.SUBTITLE,
    SearchField.AUTHOR, SearchField.SUBJECT, SearchField.BOOKSHELF,
}

_ORDER_COLUMNS = {
    OrderBy.DOWNLOADS: ("downloads", SortDirection.DESC, None),
    OrderBy.TITLE: ("title", SortDirection.ASC, None),
    OrderBy.AUTHOR: ("all_authors", SortDirection.ASC, "LAST"),
    OrderBy.RELEASE_DATE: ("release_date", SortDirection.DESC, "LAST"),
    OrderBy.RANDOM: ("RANDOM()", None, None),
}

_SELECT = "book_id, title, all_authors, downloads, dc, is_audio"

_SUBQUERY = """book_id, title, all_authors, all_subjects, downloads, release_date, dc,
    copyrighted, lang_codes, is_audio,
    max_author_birthyear, min_author_birthyear,
    max_author_deathyear, min_author_deathyear,
    locc_codes,
    tsvec, title_tsvec, subtitle_tsvec, author_tsvec, subject_tsvec, bookshelf_tsvec, attribute_tsvec,
    book_text, bookshelf_text, attribute_text, subtitle"""

# Accepted filetypes for OPDS output
_OPDS_FILETYPES = {
    "epub3.images",  # Modern EPUB3 with images (98%+ of books)
    "index",         # Audiobook HTML index
}

# =============================================================================
# Formatting Helpers (based on libgutenberg.DublinCore)
# =============================================================================

_RE_MARC_SUBFIELD = re.compile(r"\$[a-z]")
_RE_MARC_SPSEP = re.compile(r"[\n ](,|:)([A-Za-z0-9])")
_RE_CURLY_SINGLE = re.compile("[\u2018\u2019]")  # ' '
_RE_CURLY_DOUBLE = re.compile("[\u201c\u201d]")  # " "
_RE_TITLE_SPLITTER = re.compile(r"\s*[;:]\s*")

_TITLE_FIELDS = frozenset({"title", "subtitle", "alt_title"})
_STRIP_FIELDS = frozenset({
    "author", "name", "publisher", "subject", "bookshelf",
    "subjects", "bookshelves",  
})


def _strip_marc_subfields(text: str | None) -> str | None:
    """
    Strip MARC subfield markers ($a, $b, etc.) from text.
    Based on libgutenberg.DublinCore.strip_marc_subfields.
    """
    if not text or not isinstance(text, str):
        return text
    text = _RE_MARC_SUBFIELD.sub("", text)
    text = _RE_MARC_SPSEP.sub(r"\1 \2", text)  # move space to behind separator
    return text.strip()


def _format_title(text: str | None) -> str | None:
    """
    Format title: straighten curly quotes, normalize title/subtitle separators.
    Based on libgutenberg.DublinCore.format_title.
    """
    if not text or not isinstance(text, str):
        return text
    text = _RE_CURLY_SINGLE.sub("'", text)
    text = _RE_CURLY_DOUBLE.sub('"', text)
    text = _RE_TITLE_SPLITTER.sub(": ", text)  # "Title: Subtitle" format
    return text.rstrip(": ").strip()  # Clean trailing ": " like "Beowulf: " -> "Beowulf"


def _format_value(key: str, value: str) -> str:
    """Format a single string value based on its field name. O(1) lookup."""
    if key in _TITLE_FIELDS:
        return _format_title(_strip_marc_subfields(value)) or value
    if key in _STRIP_FIELDS:
        return _strip_marc_subfields(value) or value
    return value


def _format_dict(d: dict) -> dict:
    """
    Recursively format string fields in a dict.
    O(n) where n = total number of values (not n² - single pass).
    """
    result = {}
    for key, value in d.items():
        if isinstance(value, str):
            result[key] = _format_value(key, value)
        elif isinstance(value, dict):
            result[key] = _format_dict(value)
        elif isinstance(value, list):
            result[key] = _format_list(key, value)
        else:
            result[key] = value
    return result


def _format_list(parent_key: str, lst: list) -> list:
    """
    Format items in a list. O(n) - single pass over items.
    """
    result = []
    for item in lst:
        if isinstance(item, dict):
            result.append(_format_dict(item))
        elif isinstance(item, str):
            # For list items, use parent key context for formatting decision
            result.append(_format_value(parent_key, item))
        else:
            result.append(item)
    return result


def format_crosswalk_result(func: Callable) -> Callable:
    """
    Decorator that automatically formats title, author, publisher, etc. in crosswalk results.
    Strips MARC subfields and normalizes curly quotes/separators.
    
    Time complexity: O(n) where n = total values in result tree (single recursive pass).
    """
    def wrapper(row) -> dict[str, Any]:
        result = func(row)
        return _format_dict(result)
    return wrapper


# Public aliases for formatting utilities (exported in __all__)
def strip_marc_subfields(text: str | None) -> str | None:
    """Strip MARC subfield markers ($a, $b, etc.) from text."""
    return _strip_marc_subfields(text)


def format_title(text: str | None) -> str | None:
    """Format title: straighten curly quotes, normalize separators."""
    return _format_title(text)


# =============================================================================
# Crosswalk Functions
# =============================================================================

@format_crosswalk_result
def _crosswalk_full(row) -> dict[str, Any]:
    return {
        "book_id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads,
        "dc": row.dc
    }


@format_crosswalk_result
def _crosswalk_mini(row) -> dict[str, Any]:
    return {
        "id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads
    }


@format_crosswalk_result
def _crosswalk_pg(row) -> dict[str, Any]:
    dc = row.dc or {}
    return {
        "ebook_no": row.book_id,
        "title": row.title,
        "contributors": [
            {"name": c.get("name"), "role": c.get("role", "Author")}
            for c in dc.get("creators", [])
        ],
        "language": dc.get("language"),
        "subjects": [s["subject"] for s in dc.get("subjects", []) if s.get("subject")],
        "bookshelves": [b["bookshelf"] for b in dc.get("bookshelves", []) if b.get("bookshelf")],
        "release_date": dc.get("date"),
        "downloads_last_30_days": row.downloads,
        "files": [
            {"filename": f.get("filename"), "type": f.get("mediatype"), "size": f.get("extent")}
            for f in dc.get("format", []) if f.get("filename")
        ],
        "cover_url": (dc.get("coverpage") or [None])[0],
    }


@format_crosswalk_result
def _crosswalk_opds(row) -> dict[str, Any]:
    """Transform row to OPDS 2.0 publication format per spec."""
    dc = row.dc or {}
    
    # Build metadata - spec says no blank values (null, "", [], {})
    metadata = {
        "@type": "http://schema.org/Book",
        "identifier": f"urn:gutenberg:{row.book_id}",
        "title": row.title,
        "language": (dc.get("language") or [{}])[0].get("code") or "en",
    }
    
    # Author (can be string or object with name/sortAs)
    creators = dc.get("creators", [])
    if creators and creators[0].get("name"):
        p = creators[0]
        author = {"name": p["name"], "sortAs": p["name"]}
        if p.get("id"):
            author["identifier"] = f"https://www.gutenberg.org/ebooks/author/{p['id']}"
        metadata["author"] = author
    
    # Published date
    if dc.get("date"):
        metadata["published"] = dc["date"]
    
    # Modified date (from MARC 508 field)
    for m in dc.get("marc", []):
        if m.get("code") == 508 and "Updated:" in (m.get("text") or ""):
            try:
                modified = m["text"].split("Updated:")[1].strip().split()[0].rstrip(".")
                if modified:
                    metadata["modified"] = modified
            except (IndexError, AttributeError):
                pass
            break
    
    # Description
    desc_parts = []
    if summary := (dc.get("summary") or [None])[0]:
        desc_parts.append(summary)
    if notes := dc.get("description"):
        desc_parts.append(f"Notes: {'; '.join(notes)}")
    if credits := (dc.get("credits") or [None])[0]:
        desc_parts.append(f"Credits: {credits}")
    for m in dc.get("marc", []):
        if m.get("code") == 908 and m.get("text"):
            desc_parts.append(f"Reading Level: {m['text']}")
            break
    if dcmitype := [t["dcmitype"] for t in dc.get("type", []) if t.get("dcmitype")]:
        desc_parts.append(f"Category: {', '.join(dcmitype)}")
    if rights := dc.get("rights"):
        desc_parts.append(f"Rights: {rights}")
    desc_parts.append(f"Downloads: {row.downloads}")
    
    if desc_parts:
        metadata["description"] = "<p>" + "</p><p>".join(html.escape(p) for p in desc_parts) + "</p>"
    
    # Subjects
    subjects = [s["subject"] for s in dc.get("subjects", []) if s.get("subject")]
    subjects += [c["locc"] for c in dc.get("coverage", []) if c.get("locc")]
    if subjects:
        metadata["subject"] = subjects
    
    # Publisher
    if pub_raw := (dc.get("publisher") or {}).get("raw"):
        metadata["publisher"] = pub_raw
    
    # Collections (belongsTo)
    collections = []
    for b in dc.get("bookshelves", []):
        if b.get("bookshelf"):
            collections.append({"name": b["bookshelf"], "identifier": f"https://www.gutenberg.org/ebooks/bookshelf/{b.get('id', '')}"})
    for c in dc.get("coverage", []):
        if c.get("locc"):
            collections.append({"name": c["locc"], "identifier": f"https://www.gutenberg.org/ebooks/locc/{c.get('id', '')}"})
    if collections:
        metadata["belongsTo"] = {"collection": collections}
    
    # Links - must have at least one acquisition link
    links = []
    
    # Acquisition links - prioritize by content type
    # Audiobooks: use HTML index | Text books: use EPUB3 with images only
    target_format = "index" if row.is_audio else "epub3.images"
    
    for f in dc.get("format", []):
        fn = f.get("filename")
        if not fn:
            continue
        ftype = (f.get("filetype") or "").strip().lower()
        
        # Only include the target format (index for audio, epub3.images for text)
        if ftype != target_format:
            continue
        
        href = fn if fn.startswith(("http://", "https://")) else f"https://www.gutenberg.org/{fn.lstrip('/')}"
        mtype = (f.get("mediatype") or "").strip() or mimetypes.guess_type(fn)[0] or "application/octet-stream"
        
        link = {"rel": "http://opds-spec.org/acquisition/open-access", "href": href, "type": mtype}
        if f.get("extent") is not None and f["extent"] > 0:
            link["length"] = f["extent"]
        if f.get("hr_filetype"):
            link["title"] = f["hr_filetype"]
        links.append(link)
        break  # Only one link per book
    
    # Build result
    result = {"metadata": metadata, "links": links}
    
    # Images collection (should contain at least one jpeg/png/gif/avif)
    images = []
    for f in dc.get("format", []):
        ft = f.get("filetype") or ""
        fn = f.get("filename")
        if fn and ("cover.medium" in ft or ("cover" in ft and not images)):
            href = fn if fn.startswith(("http://", "https://")) else f"https://www.gutenberg.org/{fn.lstrip('/')}"
            img = {"href": href, "type": "image/jpeg"}
            images.append(img)
            if "cover.medium" in ft:
                break
    if images:
        result["images"] = images
    
    return result


_CROSSWALK_MAP = {
    Crosswalk.FULL: _crosswalk_full,
    Crosswalk.MINI: _crosswalk_mini,
    Crosswalk.PG: _crosswalk_pg,
    Crosswalk.OPDS: _crosswalk_opds,
    Crosswalk.CUSTOM: _crosswalk_full,
}


# =============================================================================
# SearchQuery
# =============================================================================

@dataclass
class SearchQuery:
    """Fluent query builder for full-text search."""
    
    _search: list[tuple[str, dict, str]] = field(default_factory=list)
    _filter: list[tuple[str, dict]] = field(default_factory=list)
    _order: OrderBy = OrderBy.RELEVANCE
    _sort_dir: SortDirection | None = None
    _page: int = 1
    _page_size: int = 28
    _crosswalk: Crosswalk = Crosswalk.FULL
    
    # === Magic Methods ===
    
    def __getitem__(self, key: int | tuple) -> SearchQuery:
        """Set pagination: q[3] for page 3, q[2, 50] for page 2 with 50 results."""
        if isinstance(key, tuple):
            self._page = max(1, int(key[0]))
            self._page_size = max(1, min(100, int(key[1])))
        else:
            self._page = max(1, int(key))
        return self
    
    # === Configuration ===
    
    def crosswalk(self, cw: Crosswalk) -> SearchQuery:
        self._crosswalk = cw
        return self
    
    def order_by(self, order: OrderBy, direction: SortDirection | None = None) -> SearchQuery:
        self._order = order
        self._sort_dir = direction
        return self
    
    # === Search Methods ===
    
    def search(self, txt: str, field: SearchField = SearchField.BOOK, search_type: SearchType = SearchType.FTS) -> SearchQuery:
        """
        Add search condition. Supports:
        
        FTS mode (default): Uses PostgreSQL websearch_to_tsquery which supports:
          - "exact phrase" for phrase matching
          - word1 word2 for AND (default)
          - word1 or word2 for OR
          - -word for NOT/exclude
          
        FUZZY mode: Uses trigram similarity with basic boolean support:
          - "exact phrase" for exact substring match
          - word1 word2 for AND (all must match)
          - -word for NOT/exclude
          
        CONTAINS mode: Simple ILIKE substring match
        """
        txt = (txt or "").strip()
        if not txt:
            return self
        
        fts_col, text_col = _FIELD_COLS[field]
        use_trigram = field in _TRIGRAM_FIELDS
        
        if search_type == SearchType.FTS or not use_trigram:
            # websearch_to_tsquery handles "phrases", or, and - natively
            sql = f"{fts_col} @@ websearch_to_tsquery('english', :q)"
            self._search.append((sql, {"q": txt}, fts_col))
        elif search_type == SearchType.FUZZY:
            # Parse query for basic boolean support in fuzzy mode
            conditions, params = self._parse_fuzzy_query(txt, text_col)
            if conditions:
                self._search.append((conditions, params, text_col))
        else:  # CONTAINS
            self._search.append((f"{text_col} ILIKE :q", {"q": f"%{txt}%"}, text_col))
        return self
    
    def _parse_fuzzy_query(self, txt: str, text_col: str) -> tuple[str, dict]:
        """
        Parse query string for fuzzy search with basic boolean support.
        
        Supports:
          - "exact phrase" → ILIKE exact match
          - -word → NOT similarity match
          - word1 word2 → AND (all must fuzzy match)
        """
        original_txt = txt  # Keep for ranking
        
        # Extract quoted phrases
        phrases = re.findall(r'"([^"]+)"', txt)
        txt = re.sub(r'"[^"]*"', '', txt)
        
        # Extract negations
        negations = re.findall(r'-(\S+)', txt)
        txt = re.sub(r'-\S+', '', txt)
        
        # Remaining words (AND logic)
        words = txt.split()
        
        conditions = []
        params = {"q": original_txt}  # Keep original for ranking in _order_sql
        param_idx = 0
        
        # Quoted phrases: exact ILIKE match
        for phrase in phrases:
            phrase = phrase.strip()
            if phrase:
                param_name = f"phrase_{param_idx}"
                conditions.append(f"{text_col} ILIKE :{param_name}")
                params[param_name] = f"%{phrase}%"
                param_idx += 1
        
        # Regular words: fuzzy similarity (AND)
        for word in words:
            word = word.strip()
            if word and word.lower() not in ('or', 'and'):
                param_name = f"word_{param_idx}"
                conditions.append(f":{param_name} <% {text_col}")
                params[param_name] = word
                param_idx += 1
        
        # Negations: NOT similarity match
        for neg in negations:
            neg = neg.strip()
            if neg:
                param_name = f"neg_{param_idx}"
                conditions.append(f"NOT ({text_col} ILIKE :{param_name})")
                params[param_name] = f"%{neg}%"
                param_idx += 1
        
        if not conditions:
            # Fallback: simple fuzzy match on original text
            return f":q <% {text_col}", {"q": original_txt}
        
        return " AND ".join(conditions), params
    
    # === Filter Methods ===
    
    def etext(self, nr: int) -> SearchQuery:
        self._filter.append(("book_id = :id", {"id": int(nr)}))
        return self
    
    def etexts(self, nrs: list[int]) -> SearchQuery:
        self._filter.append(("book_id = ANY(:ids)", {"ids": [int(n) for n in nrs]}))
        return self
    
    def downloads_gte(self, n: int) -> SearchQuery:
        self._filter.append(("downloads >= :dl", {"dl": int(n)}))
        return self
    
    def downloads_lte(self, n: int) -> SearchQuery:
        self._filter.append(("downloads <= :dl", {"dl": int(n)}))
        return self
    
    def public_domain(self) -> SearchQuery:
        self._filter.append(("copyrighted = 0", {}))
        return self
    
    def copyrighted(self) -> SearchQuery:
        self._filter.append(("copyrighted = 1", {}))
        return self
    
    def lang(self, code: str | LanguageCode) -> SearchQuery:
        """Filter by language code (matches any language in multi-language books)."""
        code_val = code.value if isinstance(code, Enum) else str(code)
        # Use array containment to leverage the GIN index on lang_codes.
        self._filter.append(("lang_codes @> ARRAY[CAST(:lang AS text)]", {"lang": code_val}))
        return self
    
    def text_only(self) -> SearchQuery:
        self._filter.append(("is_audio = false", {}))
        return self
    
    def audiobook(self) -> SearchQuery:
        self._filter.append(("is_audio = true", {}))
        return self
    
    def author_born_after(self, year: int) -> SearchQuery:
        self._filter.append(("max_author_birthyear >= :y", {"y": int(year)}))
        return self
    
    def author_born_before(self, year: int) -> SearchQuery:
        self._filter.append(("min_author_birthyear <= :y", {"y": int(year)}))
        return self
    
    def author_died_after(self, year: int) -> SearchQuery:
        self._filter.append(("max_author_deathyear >= :y", {"y": int(year)}))
        return self
    
    def author_died_before(self, year: int) -> SearchQuery:
        self._filter.append(("min_author_deathyear <= :y", {"y": int(year)}))
        return self
    
    def released_after(self, date: str) -> SearchQuery:
        self._filter.append(("release_date >= CAST(:d AS date)", {"d": str(date)}))
        return self
    
    def released_before(self, date: str) -> SearchQuery:
        self._filter.append(("release_date <= CAST(:d AS date)", {"d": str(date)}))
        return self
    
    def locc(self, code: str | LoccClass) -> SearchQuery:
        """Filter by LoCC code (prefix match for top-level codes like 'E', 'F').
        Uses MN table (mn_books_loccs) for fast indexed lookups."""
        code_val = code.value if isinstance(code, Enum) else str(code)
        # Use MN table join instead of array unnest - much faster with proper indexes
        self._filter.append((
            "EXISTS (SELECT 1 FROM mn_books_loccs mbl JOIN loccs lc ON lc.pk = mbl.fk_loccs WHERE mbl.fk_books = book_id AND lc.pk LIKE :locc_pattern)",
            {"locc_pattern": f"{code_val}%"}
        ))
        return self
    
    def has_contributor(self, role: str) -> SearchQuery:
        self._filter.append(("dc->'creators' @> CAST(:j AS jsonb)", {"j": f'[{{"role":"{role}"}}]'}))
        return self
    
    def file_type(self, ft: FileType) -> SearchQuery:
        self._filter.append(("dc->'format' @> CAST(:ft AS jsonb)", {"ft": f'[{{"mediatype":"{ft.value}"}}]'}))
        return self
    
    def author_id(self, aid: int) -> SearchQuery:
        self._filter.append(("dc->'creators' @> CAST(:aid AS jsonb)", {"aid": f'[{{"id":{int(aid)}}}]'}))
        return self
    
    def subject_id(self, sid: int) -> SearchQuery:
        """Filter by subject ID using MN table for fast indexed lookup."""
        self._filter.append((
            "EXISTS (SELECT 1 FROM mn_books_subjects mbs WHERE mbs.fk_books = book_id AND mbs.fk_subjects = :sid)",
            {"sid": int(sid)}
        ))
        return self
    
    def bookshelf_id(self, bid: int) -> SearchQuery:
        """Filter by bookshelf ID using MN table for fast indexed lookup."""
        self._filter.append((
            "EXISTS (SELECT 1 FROM mn_books_bookshelves mbb WHERE mbb.fk_books = book_id AND mbb.fk_bookshelves = :bid)",
            {"bid": int(bid)}
        ))
        return self
    
    def encoding(self, enc: Encoding) -> SearchQuery:
        self._filter.append(("dc->'format' @> CAST(:enc AS jsonb)", {"enc": f'[{{"encoding":"{enc.value}"}}]'}))
        return self
    
    def where(self, sql: str, **params) -> SearchQuery:
        """Add raw SQL filter condition."""
        self._filter.append((sql, params))
        return self
    
    # === SQL Building ===
    
    def _params(self) -> dict[str, Any]:
        params = {}
        for _, p, *_ in self._search:
            params.update(p)
        for _, p in self._filter:
            params.update(p)
        return params
    
    def _order_sql(self, params: dict) -> str:
        if self._order == OrderBy.RELEVANCE and self._search:
            sql, p, col = self._search[-1]
            params["rank_q"] = p["q"].replace("%", "")
            if "<%" in sql or "ILIKE" in sql:
                return f"word_similarity(:rank_q, {col}) DESC, downloads DESC"
            return f"ts_rank_cd({col}, websearch_to_tsquery('english', :rank_q)) DESC, downloads DESC"
        
        if self._order == OrderBy.RANDOM:
            return "RANDOM()"
        
        if self._order not in _ORDER_COLUMNS:
            return "downloads DESC"
        
        col, default_dir, nulls = _ORDER_COLUMNS[self._order]
        direction = self._sort_dir or default_dir
        clause = f"{col} {direction.value.upper()}"
        if nulls:
            clause += f" NULLS {nulls}"
        return clause
    
    def build(self) -> tuple[str, dict]:
        params = self._params()
        order = self._order_sql(params)
        limit, offset = self._page_size, (self._page - 1) * self._page_size
        
        search_sql = " AND ".join(s[0] for s in self._search) if self._search else None
        filter_sql = " AND ".join(f[0] for f in self._filter) if self._filter else None
        
        if search_sql and filter_sql:
            sql = f"SELECT {_SELECT} FROM (SELECT {_SUBQUERY} FROM mv_books_dc WHERE {search_sql}) t WHERE {filter_sql} ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        elif search_sql:
            sql = f"SELECT {_SELECT} FROM mv_books_dc WHERE {search_sql} ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        elif filter_sql:
            sql = f"SELECT {_SELECT} FROM mv_books_dc WHERE {filter_sql} ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        else:
            sql = f"SELECT {_SELECT} FROM mv_books_dc ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        
        return sql, params
    
    def build_count(self) -> tuple[str, dict]:
        params = self._params()
        search_sql = " AND ".join(s[0] for s in self._search) if self._search else None
        filter_sql = " AND ".join(f[0] for f in self._filter) if self._filter else None
        
        if search_sql and filter_sql:
            return f"SELECT COUNT(*) FROM (SELECT {_SUBQUERY} FROM mv_books_dc WHERE {search_sql}) t WHERE {filter_sql}", params
        elif search_sql:
            return f"SELECT COUNT(*) FROM mv_books_dc WHERE {search_sql}", params
        elif filter_sql:
            return f"SELECT COUNT(*) FROM mv_books_dc WHERE {filter_sql}", params
        return "SELECT COUNT(*) FROM mv_books_dc", params


# =============================================================================
# FullTextSearch
# =============================================================================

class FullTextSearch:
    """Main search interface."""
    
    def __init__(self, config: Config | None = None):
        cfg = config or Config()
        self.engine = create_engine(
            f"postgresql://{cfg.PGUSER}@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.PGDATABASE}",
            pool_pre_ping=True,
            pool_recycle=300,
        )
        self.Session = sessionmaker(bind=self.engine)
        self._custom_transformer: Callable | None = None
    
    def set_custom_transformer(self, fn: Callable) -> None:
        """Set custom transformer for Crosswalk.CUSTOM."""
        self._custom_transformer = fn
    
    def query(self, crosswalk: Crosswalk = Crosswalk.FULL) -> SearchQuery:
        """Create a new query builder."""
        q = SearchQuery()
        q._crosswalk = crosswalk
        return q
    
    def _transform(self, row, cw: Crosswalk) -> dict:
        if cw == Crosswalk.CUSTOM and self._custom_transformer:
            return self._custom_transformer(row)
        return _CROSSWALK_MAP[cw](row)
    
    def execute(self, q: SearchQuery) -> dict:
        """Execute query and return paginated results."""
        with self.Session() as session:
            count_sql, count_params = q.build_count()
            total = session.execute(text(count_sql), count_params).scalar() or 0
            total_pages = max(1, (total + q._page_size - 1) // q._page_size)
            q._page = max(1, min(q._page, total_pages))
            
            sql, params = q.build()
            rows = session.execute(text(sql), params).fetchall()
        
        return {
            "results": [self._transform(r, q._crosswalk) for r in rows],
            "page": q._page,
            "page_size": q._page_size,
            "total": total,
            "total_pages": total_pages,
        }
    
    def get(self, etext_nr: int, crosswalk: Crosswalk = Crosswalk.FULL) -> dict | None:
        """Get single book by ID."""
        with self.Session() as session:
            row = session.execute(
                text(f"SELECT {_SELECT} FROM mv_books_dc WHERE book_id = :id"),
                {"id": int(etext_nr)}
            ).fetchone()
            return self._transform(row, crosswalk) if row else None
    
    def get_many(self, nrs: list[int], crosswalk: Crosswalk = Crosswalk.FULL) -> list[dict]:
        """Get multiple books by IDs."""
        if not nrs:
            return []
        with self.Session() as session:
            rows = session.execute(
                text(f"SELECT {_SELECT} FROM mv_books_dc WHERE book_id = ANY(:ids)"),
                {"ids": [int(n) for n in nrs]}
            ).fetchall()
            return [self._transform(r, crosswalk) for r in rows]
    
    def count(self, q: SearchQuery) -> int:
        """Count results without fetching."""
        with self.Session() as session:
            sql, params = q.build_count()
            return session.execute(text(sql), params).scalar() or 0
    
    def list_bookshelves(self) -> list[dict]:
        """
        List all bookshelves with book counts.
        
        Returns:
            List of dicts with 'id', 'name', and 'book_count' keys
        """
        sql = """
            SELECT bs.pk AS id, bs.bookshelf AS name, COUNT(mbbs.fk_books) AS book_count
            FROM bookshelves bs
            LEFT JOIN mn_books_bookshelves mbbs ON bs.pk = mbbs.fk_bookshelves
            GROUP BY bs.pk, bs.bookshelf
            ORDER BY bs.bookshelf
        """
        with self.Session() as session:
            rows = session.execute(text(sql)).fetchall()
            return [{'id': r.id, 'name': r.name, 'book_count': r.book_count} for r in rows]
    
    def list_subjects(self) -> list[dict]:
        """
        List all subjects with book counts.
        
        Returns:
            List of dicts with 'id', 'name', and 'book_count' keys
        """
        sql = """
            SELECT s.pk AS id, s.subject AS name, COUNT(mbs.fk_books) AS book_count
            FROM subjects s
            LEFT JOIN mn_books_subjects mbs ON s.pk = mbs.fk_subjects
            GROUP BY s.pk, s.subject
            ORDER BY book_count DESC, s.subject
        """
        with self.Session() as session:
            rows = session.execute(text(sql)).fetchall()
            return [{'id': r.id, 'name': r.name, 'book_count': r.book_count} for r in rows]
    
    def get_subject_name(self, subject_id: int) -> str | None:
        """
        Get a single subject's name by ID (fast lookup).
        
        Args:
            subject_id: Subject primary key
            
        Returns:
            Subject name or None if not found
        """
        sql = "SELECT subject FROM subjects WHERE pk = :id"
        with self.Session() as session:
            result = session.execute(text(sql), {"id": subject_id}).scalar()
            return result
    
    def get_top_subjects_for_query(self, q: SearchQuery, limit: int = 15, max_books: int = 1000) -> list[dict]:
        """
        Get top N subjects from a search result set for dynamic facets.
        
        Uses MN table (mn_books_subjects) for fast indexed lookups instead of JSONB.
        
        Args:
            q: SearchQuery to derive subjects from
            limit: Maximum number of subjects to return (default 15)
            max_books: Maximum number of matching books to sample (default 1000)
            
        Returns:
            List of dicts with 'id', 'name', and 'count' keys, sorted by count desc
        """
        max_books = max(1, min(5000, int(max_books)))
        limit = max(1, min(100, int(limit)))

        # Build a limited "matched books" set using the same WHERE + ORDER
        params = q._params()
        order_sql = q._order_sql(params)
        search_sql = " AND ".join(s[0] for s in q._search) if q._search else None
        filter_sql = " AND ".join(f[0] for f in q._filter) if q._filter else None
        where_parts = [p for p in (search_sql, filter_sql) if p]
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # Use MN table for fast indexed lookups - much faster than JSONB parsing
        sql = f"""
            WITH matched_books AS (
                SELECT book_id
                FROM mv_books_dc
                {where_clause}
                ORDER BY {order_sql}
                LIMIT :max_books
            )
            SELECT 
                s.pk AS id,
                s.subject AS name,
                COUNT(*) AS count
            FROM matched_books mb
            JOIN mn_books_subjects mbs ON mbs.fk_books = mb.book_id
            JOIN subjects s ON s.pk = mbs.fk_subjects
            GROUP BY s.pk, s.subject
            ORDER BY count DESC
            LIMIT :limit
        """
        params["limit"] = limit
        params["max_books"] = max_books
        
        with self.Session() as session:
            rows = session.execute(text(sql), params).fetchall()
            return [{'id': r.id, 'name': r.name, 'count': r.count} for r in rows]
    
    def get_bookshelf_samples_batch(self, bookshelf_ids: list[int], sample_limit: int = 20, crosswalk: Crosswalk = Crosswalk.OPDS) -> dict[int, dict]:
        """
        Fetch sample publications from multiple bookshelves in a single optimized query.
        Uses MN table (mn_books_bookshelves) for fast indexed lookups instead of JSONB.
        
        Args:
            bookshelf_ids: List of bookshelf IDs to fetch samples for
            sample_limit: Max books per bookshelf
            crosswalk: Output format
            
        Returns:
            Dict mapping bookshelf_id -> {"results": [...], "total": int}
        """
        if not bookshelf_ids:
            return {}
        
        # Use window function for random top-N per bookshelf
        sql = """
            WITH ranked AS (
                SELECT
                    mbb.fk_bookshelves AS bs_id,
                    mv.book_id, mv.title, mv.all_authors, mv.downloads, mv.dc, mv.is_audio,
                    ROW_NUMBER() OVER (PARTITION BY mbb.fk_bookshelves ORDER BY RANDOM()) AS rn
                FROM mn_books_bookshelves mbb
                JOIN mv_books_dc mv ON mv.book_id = mbb.fk_books
                WHERE mbb.fk_bookshelves = ANY(:ids)
            )
            SELECT bs_id, book_id, title, all_authors, downloads, dc, is_audio
            FROM ranked
            WHERE rn <= :sample_limit
            ORDER BY bs_id, RANDOM()
        """
        
        # Get totals using MN table (fast indexed count)
        count_sql = """
            SELECT fk_bookshelves AS bs_id, COUNT(*) as total
            FROM mn_books_bookshelves
            WHERE fk_bookshelves = ANY(:ids)
            GROUP BY fk_bookshelves
        """
        
        transformer = _CROSSWALK_MAP[crosswalk]
        
        with self.Session() as session:
            # Get counts (fast - uses index on fk_bookshelves)
            count_rows = session.execute(text(count_sql), {"ids": bookshelf_ids}).fetchall()
            totals = {r.bs_id: r.total for r in count_rows}
            
            # Get samples
            rows = session.execute(text(sql), {"ids": bookshelf_ids, "sample_limit": sample_limit}).fetchall()
        
        # Group by bookshelf
        result = {bid: {"results": [], "total": totals.get(bid, 0)} for bid in bookshelf_ids}
        for row in rows:
            result[row.bs_id]["results"].append(transformer(row))
        
        return result