"""Simple FTS using mv_books_dc materialized view with query builder."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
import mimetypes
import html


class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'


# =============================================================================
# Enums
# =============================================================================

class FileType(str, Enum):
    EPUB = "application/epub+zip"
    KINDLE = "application/x-mobipocket-ebook"
    TXT = "text/plain"
    HTML = "text/html"
    PDF = "application/pdf"
    RDF = "application/rdf+xml"
    MP3 = "audio/mpeg"
    OGG = "audio/ogg"
    M4A = "audio/mp4"
    MIDI = "audio/midi"
    WAV = "audio/x-wav"
    JPEG = "image/jpeg"
    PNG = "image/png"
    GIF = "image/gif"
    TIFF = "image/tiff"


class Encoding(str, Enum):
    ASCII = "us-ascii"
    UTF8 = "utf-8"
    LATIN1 = "iso-8859-1"
    WINDOWS1252 = "windows-1252"


class SearchType(str, Enum):
    FTS = "fts"
    FUZZY = "fuzzy"
    CONTAINS = "contains"


class SearchField(str, Enum):
    BOOK = "book"
    TITLE = "title"
    AUTHOR = "author"
    SUBJECT = "subject"
    BOOKSHELF = "bookshelf"
    SUBTITLE = "subtitle"
    ATTRIBUTE = "attribute"


class OrderBy(str, Enum):
    RELEVANCE = "relevance"
    DOWNLOADS = "downloads"
    TITLE = "title"
    AUTHOR = "author"
    RELEASE_DATE = "release_date"
    RANDOM = "random"


class Crosswalk(str, Enum):
    FULL = "full"
    PG = "pg"
    OPDS = "opds"
    CUSTOM = "custom"
    MINI = "mini"


# =============================================================================
# Configuration
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
    SearchField.BOOK,
    SearchField.TITLE,
    SearchField.SUBTITLE,
    SearchField.AUTHOR,
    SearchField.SUBJECT,
    SearchField.BOOKSHELF,
    # ATTRIBUTE removed - no trigram index, too slow
}

_ORDER_SQL = {
    OrderBy.DOWNLOADS: "downloads DESC",
    OrderBy.TITLE: "title ASC",
    OrderBy.AUTHOR: "all_authors ASC NULLS LAST",
    OrderBy.RELEASE_DATE: "text_to_date_immutable(dc->>'date') DESC NULLS LAST",
    OrderBy.RANDOM: "RANDOM()",
}

_SELECT = "book_id, title, all_authors, downloads, dc"

_SUBQUERY = """book_id, title, all_authors, all_subjects, downloads, dc,
    copyrighted, primary_lang, is_audio,
    max_author_birthyear, min_author_birthyear,
    max_author_deathyear, min_author_deathyear,
    locc_codes,
    tsvec, title_tsvec, subtitle_tsvec, author_tsvec, subject_tsvec, bookshelf_tsvec, attribute_tsvec,
    book_text, bookshelf_text, attribute_text, subtitle"""


# =============================================================================
# Crosswalks
# =============================================================================

def _crosswalk_full(row) -> dict[str, Any]:
    return {"book_id": row.book_id, "title": row.title, "author": row.all_authors, "downloads": row.downloads, "dc": row.dc}

def _crosswalk_mini(row) -> dict[str, Any]:
    return {"id": row.book_id, "title": row.title, "author": row.all_authors, "downloads": row.downloads}

def _crosswalk_pg(row) -> dict[str, Any]:
    dc = row.dc or {}
    return {
        "ebook_no": row.book_id, "title": row.title,
        "contributors": [{"name": c.get("name"), "role": c.get("role", "Author")} for c in dc.get("creators", [])],
        "language": dc.get("language"),
        "subjects": [s.get("subject") for s in dc.get("subjects", []) if s.get("subject")],
        "bookshelves": [b.get("bookshelf") for b in dc.get("bookshelves", []) if b.get("bookshelf")],
        "release_date": dc.get("date"), "downloads_last_30_days": row.downloads,
        "files": [{"filename": f.get("filename"), "type": f.get("mediatype"), "size": f.get("extent")} 
                  for f in dc.get("format", []) if f.get("filename")],
        "cover_url": (dc.get("coverpage") or [None])[0],
    }

def _crosswalk_opds(row) -> dict[str, Any]:
    """
    Walks a book in the materialized view and produces an OPDS 2.0 compliant publication.
    """
    dc = row.dc or {}

    def _abs_href(f: dict) -> str | None:
        fn = f.get("filename") if f else None
        if not fn:
            return None
        if fn.startswith(("http://", "https://")):
            return fn
        return f"https://www.gutenberg.org/{fn.lstrip('/')}"

    FILETYPE_TO_MIME = {
        "epub.images": "application/epub+zip",
        "epub.noimages": "application/epub+zip",
        "epub3.images": "application/epub+zip",
        "kf8.images": "application/x-mobipocket-ebook",
        "kindle.images": "application/x-mobipocket-ebook",
        "kindle.noimages": "application/x-mobipocket-ebook",
        "pdf": "application/pdf",
        "mp3": "audio/mpeg",
        "m4a": "audio/mp4",
        "m4b": "audio/mp4",
        "ogg": "audio/ogg",
        "wav": "audio/x-wav",
    }

    ACCEPT_FILETYPES = {
        "epub.images", "epub.noimages", "epub3.images",
        "kf8.images", "kindle.images", "kindle.noimages",
        "pdf", "mp3", "m4a", "m4b", "ogg", "wav"
    }

    def _mime_for(f: dict) -> str:
        med = (f.get("mediatype")).strip()
        if med:
            return med
        ft = (f.get("filetype")).strip().lower()
        if ft and ft in FILETYPE_TO_MIME:
            return FILETYPE_TO_MIME[ft]
        guessed, _ = mimetypes.guess_type(f.get("filename"))
        return guessed

    creators = dc.get("creators", [])
    primary = creators[0] if creators else {}

    author = None
    if primary.get("name"):
        name = primary["name"]
        if primary.get("birthdate") or primary.get("deathdate"):
            name = f"{name}, {primary.get('birthdate') or '?'}-{primary.get('deathdate') or ''}"
        author = {"name": name, "sortAs": primary["name"]}

    cover = None
    for f in dc.get("format", []):
        ft = f.get("filetype") or ""
        if "cover.medium" in ft:
            cover = _abs_href(f)
            break
        elif "cover" in ft and not cover:
            cover = _abs_href(f)

    published = dc.get("date")
    modified = None
    for m in dc.get("marc", []):
        if m.get("code") == 508:
            txt = m.get("text") or ""
            if "Updated:" in txt:
                modified = txt.split("Updated:")[1].strip().split()[0].rstrip(".")
                break

    reading_level = None
    for m in dc.get("marc", []):
        if m.get("code") == 908:
            reading_level = m.get("text")
            break

    subjects = [s["subject"] for s in dc.get("subjects", []) if s.get("subject")] + [c["locc"] for c in dc.get("coverage", []) if c.get("locc")]
    bookshelves = [
        {"name": b["bookshelf"], "identifier": f"https://www.gutenberg.org/ebooks/bookshelf/{b.get('id', '')}"}
        for b in dc.get("bookshelves", []) if b.get("bookshelf")
    ]
    locc = [
        {"name": c["locc"], "identifier": f"https://www.gutenberg.org/ebooks/locc/{c.get('id', '')}"}
        for c in dc.get("coverage", []) if c.get("locc")
    ]

    desc_parts = []
    if summary := (dc.get("summary") or [None])[0]:
        desc_parts.append(summary)
    if notes := dc.get("description"):
        desc_parts.append(f"Notes: {'; '.join(notes)}")
    if credits := (dc.get("credits") or [None])[0]:
        desc_parts.append(f"Credits: {credits}")
    if reading_level:
        desc_parts.append(f"Reading Level: {reading_level}")
    if dcmitype := [t["dcmitype"] for t in dc.get("type", []) if t.get("dcmitype")]:
        desc_parts.append(f"Category: {', '.join(dcmitype)}")
    if rights := dc.get("rights"):
        desc_parts.append(f"Rights: {rights}")
    desc_parts.append(f"Downloads: {row.downloads}")

    if desc_parts:
        parts = [html.escape(p) for p in desc_parts]
        description_html = "<p>" + "</p><p>".join(parts) + "</p>"
        description = description_html
    else:
        description = None

    metadata = {
        "@type": "http://schema.org/Book",
        "identifier": f"urn:gutenberg:{row.book_id}",
        "title": row.title,
        "language": (dc.get("language") or [{}])[0].get("code", "en"),
    }
    if author:
        metadata["author"] = author
    if published:
        metadata["published"] = published
    if modified:
        metadata["modified"] = modified
    if description:
        metadata["description"] = description
    if subjects:
        metadata["subject"] = subjects
    if pub_raw := (dc.get("publisher") or {}).get("raw"):
        metadata["publisher"] = pub_raw

    collections = bookshelves + locc
    if collections:
        metadata["belongsTo"] = {"collection": collections}

    links = [{"rel": "self", "href": f"https://www.gutenberg.org/ebooks/{row.book_id}", "type": "text/html"}]

    for f in dc.get("format", []):
        href = _abs_href(f)
        if not href:
            continue
        ftype = (f.get("filetype") or "").strip().lower()
        med = (f.get("mediatype") or "").strip().lower()
        accept = False
        if ftype in ACCEPT_FILETYPES:
            accept = True
        elif med.startswith("audio/"):
            accept = True
        elif med == "application/pdf":
            accept = True
        if not accept:
            continue

        mtype = _mime_for(f)
        link = {"href": href, "type": mtype, "rel": "http://opds-spec.org/acquisition/open-access"}
        if f.get("extent") is not None:
            link["length"] = f.get("extent")
        if f.get("encoding"):
            link["encoding"] = f.get("encoding")
        if f.get("hr_filetype"):
            link["title"] = f.get("hr_filetype")
        if ftype:
            link["filetype"] = ftype
        links.append(link)

    result = {"metadata": metadata, "links": links}
    if cover:
        result["images"] = [{"href": cover, "type": "image/jpeg"}]
    return result

_CROSSWALK_MAP = {
    Crosswalk.FULL: _crosswalk_full, 
    Crosswalk.MINI: _crosswalk_mini,
    Crosswalk.PG: _crosswalk_pg, 
    Crosswalk.OPDS: _crosswalk_opds, 
    Crosswalk.CUSTOM: _crosswalk_full  
}

# =============================================================================
# SearchQuery
# =============================================================================

@dataclass
class SearchQuery:
    """Query builder. Searches run in inner query, filters in outer."""
    
    _search: list[tuple[str, dict, str]] = field(default_factory=list)  
    _filter: list[tuple[str, dict]] = field(default_factory=list)
    _order: OrderBy = OrderBy.RELEVANCE
    _page: int = 1
    _page_size: int = 25
    _crosswalk: Crosswalk = Crosswalk.FULL
    
    # === Pagination ===
    
    def __getitem__(self, key: int | tuple) -> "SearchQuery":
        """q[3] for page 3, q[2, 50] for page 2 with 50 results."""
        if isinstance(key, tuple):
            self._page, self._page_size = max(1, key[0]), max(1, min(100, key[1]))
        else:
            self._page = max(1, key)
        return self
    
    def __len__(self) -> int:
        """Number of conditions."""
        return len(self._search) + len(self._filter)
    
    def __bool__(self) -> bool:
        """True if any conditions exist."""
        return bool(self._search or self._filter)
    
    # === Options ===
    
    def crosswalk(self, cw: Crosswalk) -> "SearchQuery":
        self._crosswalk = cw
        return self
    
    def order_by(self, order: OrderBy) -> "SearchQuery":
        self._order = order
        return self
    
    # === Search ===
    
    def search(self, txt: str, field: SearchField = SearchField.BOOK, type: SearchType = SearchType.FTS) -> "SearchQuery":
        if not (txt and txt.strip()):
            return self
        txt = txt.strip()
        fts_col, text_col = _FIELD_COLS[field]
        
        if type == SearchType.FTS:
            self._search.append((f"{fts_col} @@ websearch_to_tsquery('english', :q)", {"q": txt}, fts_col))
        elif type == SearchType.FUZZY:
            if field not in _TRIGRAM_FIELDS:
                self._search.append((f"{fts_col} @@ websearch_to_tsquery('english', :q)", {"q": txt}, fts_col))
            else:
                self._search.append((f":q <% {text_col}", {"q": txt}, text_col))
        else:  
            if field not in _TRIGRAM_FIELDS:
                self._search.append((f"{fts_col} @@ websearch_to_tsquery('english', :q)", {"q": txt}, fts_col))
            else:
                self._search.append((f"{text_col} ILIKE :q", {"q": f"%{txt}%"}, text_col))
        return self
    
    # === Filters ===
    
    def etext(self, nr: int) -> "SearchQuery":
        self._filter.append(("book_id = :id", {"id": nr}))
        return self
    
    def etexts(self, nrs: list[int]) -> "SearchQuery":
        self._filter.append(("book_id = ANY(:ids)", {"ids": nrs}))
        return self
    
    def downloads_gte(self, n: int) -> "SearchQuery":
        self._filter.append(("downloads >= :dl", {"dl": n}))
        return self
    
    def downloads_lte(self, n: int) -> "SearchQuery":
        self._filter.append(("downloads <= :dl", {"dl": n}))
        return self
    
    def public_domain(self) -> "SearchQuery":
        self._filter.append(("copyrighted = 0", {}))
        return self
    
    def copyrighted(self) -> "SearchQuery":
        self._filter.append(("copyrighted = 1", {}))
        return self
    
    def lang(self, code: str) -> "SearchQuery":
        self._filter.append(("primary_lang = :lang", {"lang": code}))
        return self
    
    def text_only(self) -> "SearchQuery":
        self._filter.append(("is_audio = false", {}))
        return self
    
    def audiobook(self) -> "SearchQuery":
        self._filter.append(("is_audio = true", {}))
        return self
    
    def author_born_after(self, year: int) -> "SearchQuery":
        self._filter.append(("max_author_birthyear >= :y", {"y": year}))
        return self
    
    def author_born_before(self, year: int) -> "SearchQuery":
        self._filter.append(("min_author_birthyear <= :y", {"y": year}))
        return self
    
    def released_after(self, date: str) -> "SearchQuery":
        self._filter.append(("text_to_date_immutable(dc->>'date') >= CAST(:d AS date)", {"d": date}))
        return self
    
    def released_before(self, date: str) -> "SearchQuery":
        self._filter.append(("text_to_date_immutable(dc->>'date') <= CAST(:d AS date)", {"d": date}))
        return self
    
    def locc(self, code: str) -> "SearchQuery":
        self._filter.append((":code = ANY(locc_codes)", {"code": code}))
        return self
    
    def has_contributor(self, role: str) -> "SearchQuery":
        self._filter.append(("dc->'creators' @> CAST(:j AS jsonb)", {"j": f'[{{"role":"{role}"}}]'}))
        return self
    
    def file_type(self, ft: FileType) -> "SearchQuery":
        self._filter.append(("dc->'format' @> CAST(:ft AS jsonb)", {"ft": f'[{{"mediatype":"{ft.value}"}}]'}))
        return self
    
    def author_id(self, aid: int) -> "SearchQuery":
        self._filter.append(("dc->'creators' @> CAST(:aid AS jsonb)", {"aid": f'[{{"id":{aid}}}]'}))
        return self
    
    def subject_id(self, sid: int) -> "SearchQuery":
        self._filter.append(("dc->'subjects' @> CAST(:sid AS jsonb)", {"sid": f'[{{"id":{sid}}}]'}))
        return self
    
    def bookshelf_id(self, bid: int) -> "SearchQuery":
        self._filter.append(("dc->'bookshelves' @> CAST(:bid AS jsonb)", {"bid": f'[{{"id":{bid}}}]'}))
        return self
    
    def encoding(self, enc: Encoding) -> "SearchQuery":
        self._filter.append(("dc->'format' @> CAST(:enc AS jsonb)", {"enc": f'[{{"encoding":"{enc.value}"}}]'}))
        return self
    
    def author_died_after(self, year: int) -> "SearchQuery":
        self._filter.append(("max_author_deathyear >= :y", {"y": year}))
        return self
    
    def author_died_before(self, year: int) -> "SearchQuery":
        self._filter.append(("min_author_deathyear <= :y", {"y": year}))
        return self
    
    def where(self, sql: str, **params) -> "SearchQuery":
        self._filter.append((sql, params))
        return self
    
    # === Build ===
    
    def _params(self) -> dict[str, Any]:
        params = {}
        for _, p, *_ in self._search:
            params.update(p)
        for _, p in self._filter:
            params.update(p)
        return params
        
    def _order_sql(self, params: dict) -> str:
        """Build ORDER BY clause. Uses last search for relevance ranking."""
        if self._order == OrderBy.RELEVANCE and self._search:
            sql, p, col = self._search[-1]
            params["rank_q"] = p["q"].replace("%", "")
            if "<%" in sql or "ILIKE" in sql:
                return f"word_similarity(:rank_q, {col}) DESC, downloads DESC"
            return f"ts_rank_cd({col}, websearch_to_tsquery('english', :rank_q)) DESC, downloads DESC"
        return _ORDER_SQL.get(self._order, "downloads DESC")
    
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
            sql = f"SELECT COUNT(*) FROM (SELECT {_SUBQUERY} FROM mv_books_dc WHERE {search_sql}) t WHERE {filter_sql}"
        elif search_sql:
            sql = f"SELECT COUNT(*) FROM mv_books_dc WHERE {search_sql}"
        elif filter_sql:
            sql = f"SELECT COUNT(*) FROM mv_books_dc WHERE {filter_sql}"
        else:
            sql = "SELECT COUNT(*) FROM mv_books_dc"
        
        return sql, params


# =============================================================================
# FullTextSearch
# =============================================================================

class FullTextSearch:
    def __init__(self, config: Config | None = None):
        cfg = config or Config()
        self.engine = create_engine(
            f"postgresql://{cfg.PGUSER}@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.PGDATABASE}",
            pool_pre_ping=True, pool_recycle=300,
        )
        self.Session = sessionmaker(bind=self.engine)
        self._custom_transformer: Callable | None = None
    
    def set_custom_transformer(self, fn: Callable) -> None:
        """Set custom transformer for Crosswalk.CUSTOM."""
        self._custom_transformer = fn
    
    def query(self, crosswalk: Crosswalk = Crosswalk.FULL) -> SearchQuery:
        q = SearchQuery()
        q._crosswalk = crosswalk
        return q
    
    def _transform(self, row, cw: Crosswalk) -> dict:
        if cw == Crosswalk.CUSTOM and self._custom_transformer:
            return self._custom_transformer(row)
        return _CROSSWALK_MAP[cw](row)
    
    def execute(self, q: SearchQuery) -> dict:
        with self.Session() as session:
            count_sql, count_params = q.build_count()
            total = session.execute(text(count_sql), count_params).scalar() or 0
            total_pages = max(1, (total + q._page_size - 1) // q._page_size)
            q._page = max(1, min(q._page, total_pages))
            
            sql, params = q.build()
            rows = session.execute(text(sql), params).fetchall()
        
        return {
            "results": [self._transform(r, q._crosswalk) for r in rows],
            "page": q._page, "page_size": q._page_size,
            "total": total, "total_pages": total_pages
        }
    
    def get(self, etext_nr: int, crosswalk: Crosswalk = Crosswalk.FULL) -> dict | None:
        with self.Session() as session:
            row = session.execute(text(f"SELECT {_SELECT} FROM mv_books_dc WHERE book_id = :id"), {"id": etext_nr}).fetchone()
            return self._transform(row, crosswalk) if row else None
    
    def get_many(self, nrs: list[int], crosswalk: Crosswalk = Crosswalk.FULL) -> list[dict]:
        if not nrs:
            return []
        with self.Session() as session:
            rows = session.execute(text(f"SELECT {_SELECT} FROM mv_books_dc WHERE book_id = ANY(:ids)"), {"ids": nrs}).fetchall()
            return [self._transform(r, crosswalk) for r in rows]
    
    def count(self, q: SearchQuery) -> int:
        with self.Session() as session:
            sql, params = q.build_count()
            return session.execute(text(sql), params).scalar() or 0