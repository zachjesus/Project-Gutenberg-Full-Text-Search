"""Full-text search using mv_books_dc materialized view with query builder."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
import mimetypes
import html


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
    "LOCC_LIST",
    "LANGUAGE_LABELS",
    "LOCC_LABELS",
]


class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'


# =============================================================================
# Catalog vocabulary (OPDS facet labels)
# =============================================================================

LANGUAGE_LIST = [
    {'code': 'en', 'label': 'English'},
    {'code': 'af', 'label': 'Afrikaans'},
    {'code': 'ale', 'label': 'Aleut'},
    {'code': 'ang', 'label': 'Old English'},
    {'code': 'ar', 'label': 'Arabic'},
    {'code': 'arp', 'label': 'Arapaho'},
    {'code': 'bg', 'label': 'Bulgarian'},
    {'code': 'bgs', 'label': 'Basa Banyumasan'},
    {'code': 'bo', 'label': 'Tibetan'},
    {'code': 'br', 'label': 'Breton'},
    {'code': 'brx', 'label': 'Bodo'},
    {'code': 'ca', 'label': 'Catalan'},
    {'code': 'ceb', 'label': 'Cebuano'},
    {'code': 'cs', 'label': 'Czech'},
    {'code': 'csb', 'label': 'Kashubian'},
    {'code': 'cy', 'label': 'Welsh'},
    {'code': 'da', 'label': 'Danish'},
    {'code': 'de', 'label': 'German'},
    {'code': 'el', 'label': 'Greek'},
    {'code': 'enm', 'label': 'Middle English'},
    {'code': 'eo', 'label': 'Esperanto'},
    {'code': 'es', 'label': 'Spanish'},
    {'code': 'et', 'label': 'Estonian'},
    {'code': 'fa', 'label': 'Persian'},
    {'code': 'fi', 'label': 'Finnish'},
    {'code': 'fr', 'label': 'French'},
    {'code': 'fur', 'label': 'Friulian'},
    {'code': 'fy', 'label': 'Western Frisian'},
    {'code': 'ga', 'label': 'Irish'},
    {'code': 'gl', 'label': 'Galician'},
    {'code': 'gla', 'label': 'Scottish Gaelic'},
    {'code': 'grc', 'label': 'Ancient Greek'},
    {'code': 'hai', 'label': 'Haida'},
    {'code': 'he', 'label': 'Hebrew'},
    {'code': 'hu', 'label': 'Hungarian'},
    {'code': 'ia', 'label': 'Interlingua'},
    {'code': 'ilo', 'label': 'Iloko'},
    {'code': 'is', 'label': 'Icelandic'},
    {'code': 'it', 'label': 'Italian'},
    {'code': 'iu', 'label': 'Inuktitut'},
    {'code': 'ja', 'label': 'Japanese'},
    {'code': 'kha', 'label': 'Khasi'},
    {'code': 'kld', 'label': 'Klamath-Modoc'},
    {'code': 'ko', 'label': 'Korean'},
    {'code': 'la', 'label': 'Latin'},
    {'code': 'lt', 'label': 'Lithuanian'},
    {'code': 'mi', 'label': 'Māori'},
    {'code': 'myn', 'label': 'Mayan Languages'},
    {'code': 'nah', 'label': 'Nahuatl'},
    {'code': 'nai', 'label': 'North American Indian'},
    {'code': 'nap', 'label': 'Neapolitan'},
    {'code': 'nav', 'label': 'Navajo'},
    {'code': 'nl', 'label': 'Dutch'},
    {'code': 'no', 'label': 'Norwegian'},
    {'code': 'oc', 'label': 'Occitan'},
    {'code': 'oji', 'label': 'Ojibwa'},
    {'code': 'pl', 'label': 'Polish'},
    {'code': 'pt', 'label': 'Portuguese'},
    {'code': 'rmq', 'label': 'Romani'},
    {'code': 'ro', 'label': 'Romanian'},
    {'code': 'ru', 'label': 'Russian'},
    {'code': 'sa', 'label': 'Sanskrit'},
    {'code': 'sco', 'label': 'Scots'},
    {'code': 'sl', 'label': 'Slovenian'},
    {'code': 'sr', 'label': 'Serbian'},
    {'code': 'sv', 'label': 'Swedish'},
    {'code': 'te', 'label': 'Telugu'},
    {'code': 'tl', 'label': 'Tagalog'},
    {'code': 'yi', 'label': 'Yiddish'},
    {'code': 'zh', 'label': 'Chinese'},
]

LOCC_LIST = [
    {'code': 'A', 'label': 'General Works'},
    {'code': 'B', 'label': 'Philosophy, Psychology, Religion'},
    {'code': 'C', 'label': 'History: Auxiliary Sciences'},
    {'code': 'D', 'label': 'History: General and Eastern Hemisphere'},
    {'code': 'E', 'label': 'History: America'},
    {'code': 'F', 'label': 'History: America (Local)'},
    {'code': 'G', 'label': 'Geography, Anthropology, Recreation'},
    {'code': 'H', 'label': 'Social Sciences'},
    {'code': 'J', 'label': 'Political Science'},
    {'code': 'K', 'label': 'Law'},
    {'code': 'L', 'label': 'Education'},
    {'code': 'M', 'label': 'Music'},
    {'code': 'N', 'label': 'Fine Arts'},
    {'code': 'P', 'label': 'Language and Literature'},
    {'code': 'Q', 'label': 'Science'},
    {'code': 'R', 'label': 'Medicine'},
    {'code': 'S', 'label': 'Agriculture'},
    {'code': 'T', 'label': 'Technology'},
    {'code': 'U', 'label': 'Military Science'},
    {'code': 'V', 'label': 'Naval Science'},
    {'code': 'Z', 'label': 'Bibliography, Library Science'},
]

LANGUAGE_LABELS = {i["code"]: i["label"] for i in LANGUAGE_LIST}
LOCC_LABELS = {i["code"]: i["label"] for i in LOCC_LIST}


# =============================================================================
# Enums
# =============================================================================

class LanguageCode(str, Enum):
    """Common language codes (mirrors OPDS facet options)."""
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
    OrderBy.RELEASE_DATE: ("text_to_date_immutable(dc->>'date')", SortDirection.DESC, "LAST"),
    OrderBy.RANDOM: ("RANDOM()", None, None),
}

_SELECT = "book_id, title, all_authors, downloads, dc"

_SUBQUERY = """book_id, title, all_authors, all_subjects, downloads, dc,
    copyrighted, lang_codes, is_audio,
    max_author_birthyear, min_author_birthyear,
    max_author_deathyear, min_author_deathyear,
    locc_codes,
    tsvec, title_tsvec, subtitle_tsvec, author_tsvec, subject_tsvec, bookshelf_tsvec, attribute_tsvec,
    book_text, bookshelf_text, attribute_text, subtitle"""

# Accepted filetypes for OPDS output
_OPDS_FILETYPES = {
    "epub.images", "epub.noimages", "epub3.images",
    "kf8.images", "kindle.images", "kindle.noimages",
    "pdf", "index",
}


# =============================================================================
# Crosswalk Functions
# =============================================================================

def _crosswalk_full(row) -> dict[str, Any]:
    return {
        "book_id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads,
        "dc": row.dc
    }


def _crosswalk_mini(row) -> dict[str, Any]:
    return {
        "id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads
    }


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
    
    # Acquisition links
    for f in dc.get("format", []):
        fn = f.get("filename")
        if not fn:
            continue
        ftype = (f.get("filetype") or "").strip().lower()
        if ftype not in _OPDS_FILETYPES:
            continue
        
        href = fn if fn.startswith(("http://", "https://")) else f"https://www.gutenberg.org/{fn.lstrip('/')}"
        mtype = (f.get("mediatype") or "").strip() or mimetypes.guess_type(fn)[0] or "application/octet-stream"
        
        link = {"rel": "http://opds-spec.org/acquisition/open-access", "href": href, "type": mtype}
        if f.get("extent") is not None and f["extent"] > 0:
            link["length"] = f["extent"]
        if f.get("hr_filetype"):
            link["title"] = f["hr_filetype"]
        links.append(link)
    
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
    _page_size: int = 25
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
    
    def __len__(self) -> int:
        return len(self._search) + len(self._filter)
    
    def __bool__(self) -> bool:
        return bool(self._search or self._filter)
    
    # === Configuration ===
    
    def crosswalk(self, cw: Crosswalk) -> SearchQuery:
        self._crosswalk = cw
        return self
    
    def order_by(self, order: OrderBy, direction: SortDirection | None = None) -> SearchQuery:
        self._order = order
        self._sort_dir = direction
        return self
    
    # === Search Methods ===
    
    def search(self, txt: str, field: SearchField = SearchField.BOOK, type: SearchType = SearchType.FTS) -> SearchQuery:
        txt = (txt or "").strip()
        if not txt:
            return self
        
        fts_col, text_col = _FIELD_COLS[field]
        use_trigram = field in _TRIGRAM_FIELDS
        
        if type == SearchType.FTS or not use_trigram:
            sql = f"{fts_col} @@ websearch_to_tsquery('english', :q)"
            self._search.append((sql, {"q": txt}, fts_col))
        elif type == SearchType.FUZZY:
            self._search.append((f":q <% {text_col}", {"q": txt}, text_col))
        else:  # CONTAINS
            self._search.append((f"{text_col} ILIKE :q", {"q": f"%{txt}%"}, text_col))
        return self
    
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
        self._filter.append(("text_to_date_immutable(dc->>'date') >= CAST(:d AS date)", {"d": str(date)}))
        return self
    
    def released_before(self, date: str) -> SearchQuery:
        self._filter.append(("text_to_date_immutable(dc->>'date') <= CAST(:d AS date)", {"d": str(date)}))
        return self
    
    def locc(self, code: str | LoccClass) -> SearchQuery:
        """Filter by LoCC code (prefix match for top-level codes like 'E', 'F')."""
        code_val = code.value if isinstance(code, Enum) else str(code)
        self._filter.append((
            "EXISTS (SELECT 1 FROM unnest(locc_codes) AS lc WHERE lc LIKE :locc_pattern)",
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
        self._filter.append(("dc->'subjects' @> CAST(:sid AS jsonb)", {"sid": f'[{{"id":{int(sid)}}}]'}))
        return self
    
    def bookshelf_id(self, bid: int) -> SearchQuery:
        self._filter.append(("dc->'bookshelves' @> CAST(:bid AS jsonb)", {"bid": f'[{{"id":{int(bid)}}}]'}))
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