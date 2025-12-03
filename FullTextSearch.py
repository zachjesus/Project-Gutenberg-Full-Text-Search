"""Simple FTS using mv_books_dc materialized view with query builder."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Literal
from enum import IntEnum, Enum
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
import time


class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'


class _Priority(IntEnum):
    """Internal query condition priority. Lower = more selective = evaluated first."""
    PK = 1
    FTS = 2
    TRGM = 3
    BTREE = 4
    DATE = 5
    GIN = 6


class FileType(str, Enum):
    """Available file types for filtering."""
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
    TEI = "application/prs.tei"
    TEX = "application/prs.tex"
    RST = "text/x-rst"
    RTF = "text/rtf"
    DOC = "application/msword"
    XML = "text/xml"
    PS = "application/postscript"
    VIDEO_MPEG = "video/mpeg"
    VIDEO_QT = "video/quicktime"
    VIDEO_FLV = "video/x-flv"
    VIDEO_AVI = "video/x-msvideo"
    ISO = "application/x-iso9660-image"
    MUSESCORE = "application/x-musescore"


class Encoding(str, Enum):
    """Available encodings for filtering."""
    ASCII = "us-ascii"
    UTF8 = "utf-8"
    LATIN1 = "iso-8859-1"
    WINDOWS1252 = "windows-1252"


class SearchType(str, Enum):
    """Search algorithm."""
    FTS = "fts"           # Full-text (stemming) - GIN tsvector
    FUZZY = "fuzzy"       # Typo-tolerant - GiST trigram <%
    CONTAINS = "contains" # Substring - GIN trigram ILIKE


class SearchField(str, Enum):
    """Searchable field."""
    BOOK = "book"
    TITLE = "title"
    AUTHOR = "author"
    SUBJECT = "subject"
    BOOKSHELF = "bookshelf"
    SUBTITLE = "subtitle"
    ATTRIBUTE = "attribute"  


OrderBy = Literal["relevance", "downloads", "title", "author"]


@dataclass
class _Condition:
    """Internal: A single WHERE condition with its bound parameters."""
    sql: str
    params: dict[str, Any] = field(default_factory=dict)
    priority: _Priority = _Priority.GIN


@dataclass
class SearchQuery:
    """Query builder for mv_books_dc."""
    
    # Maps field -> (fts_col, text_col)
    _COLS = {
        SearchField.BOOK:      ("tsvec",           "book_text"),
        SearchField.TITLE:     ("title_tsvec",     "title"),
        SearchField.SUBTITLE:  ("subtitle_tsvec",  "subtitle"),
        SearchField.AUTHOR:    ("author_tsvec",    "primary_author"),
        SearchField.SUBJECT:   ("subject_tsvec",   "primary_subject"),
        SearchField.BOOKSHELF: ("bookshelf_tsvec", "bookshelf_text"),
        SearchField.ATTRIBUTE: ("attribute_tsvec", "attribute_text"),
    }
    
    _conditions: list[_Condition] = field(default_factory=list)
    _order: OrderBy = "relevance"
    _page: int = 1
    _page_size: int = 25
    _searches: list[tuple[str, SearchType, str]] = field(default_factory=list)  # (text, type, col)
    _uid: int = 0
    
    def _param(self, value: Any) -> tuple[str, str]:
        """Generate unique param name and return (placeholder, name)."""
        self._uid += 1
        name = f"p{self._uid}"
        return f":{name}", name
    
    def _add(self, sql: str, priority: _Priority = _Priority.GIN, **params) -> "SearchQuery":
        """Add condition with params and priority."""
        self._conditions.append(_Condition(sql, params, priority))
        return self
    
    # === Pagination ===
    
    def __getitem__(self, key: int | tuple) -> "SearchQuery":
        if isinstance(key, tuple):
            self._page, self._page_size = max(1, key[0]), max(1, min(100, key[1]))
        else:
            self._page = max(1, key)
        return self
    
    # === Unified Search ===
    
    def search(
        self, 
        txt: str, 
        field: SearchField = SearchField.BOOK, 
        type: SearchType = SearchType.FTS
    ) -> "SearchQuery":
        """
        Universal search - all fields support all search types.
        Can be chained multiple times for AND logic.
        
        Examples:
            .search("Shakespeare")                                      # FTS on book
            .search("Shakespeare", SearchField.AUTHOR)                  # FTS on author
            .search("Shakspeare", SearchField.AUTHOR, SearchType.FUZZY) # Typo-tolerant
            .search("Novel", SearchField.TITLE).search("Twain", SearchField.AUTHOR)  # AND
        """
        if not txt or not txt.strip():
            return self
        txt = txt.strip()
        
        fts_col, text_col = self._COLS[field]
        ph, nm = self._param(txt)
        
        if type == SearchType.FTS:
            self._searches.append((txt, type, fts_col))
            return self._add(f"{fts_col} @@ websearch_to_tsquery('english', {ph})", _Priority.FTS, **{nm: txt})
        
        # FUZZY or CONTAINS use text column
        self._searches.append((txt, type, text_col))
        
        if type == SearchType.FUZZY:
            return self._add(f"{ph} <% {text_col}", _Priority.TRGM, **{nm: txt})
        
        # CONTAINS
        return self._add(f"{text_col} ILIKE {ph}", _Priority.TRGM, **{nm: f"%{txt}%"})
    
    # === Filters (PK) ===
    
    def etext(self, nr: int) -> "SearchQuery":
        """Search by Project Gutenberg etext number."""
        ph, nm = self._param(nr)
        return self._add(f"book_id = {ph}", _Priority.PK, **{nm: nr})
    
    def etexts(self, nrs: list[int]) -> "SearchQuery":
        """Search by multiple etext numbers."""
        ph, nm = self._param(nrs)
        return self._add(f"book_id = ANY({ph})", _Priority.PK, **{nm: nrs})
    
    # === Filters (B-tree) ===
    
    def downloads_gte(self, n: int) -> "SearchQuery":
        ph, nm = self._param(n)
        return self._add(f"downloads >= {ph}", _Priority.BTREE, **{nm: n})
    
    def downloads_lte(self, n: int) -> "SearchQuery":
        ph, nm = self._param(n)
        return self._add(f"downloads <= {ph}", _Priority.BTREE, **{nm: n})
    
    def public_domain(self) -> "SearchQuery":
        return self._add("copyrighted = 0", _Priority.BTREE)
    
    def copyrighted(self) -> "SearchQuery":
        return self._add("copyrighted = 1", _Priority.BTREE)
    
    def lang(self, code: str) -> "SearchQuery":
        ph, nm = self._param(code)
        return self._add(f"primary_lang = {ph}", _Priority.BTREE, **{nm: code})
    
    def text_only(self) -> "SearchQuery":
        return self._add("is_audio = false", _Priority.BTREE)
    
    def audiobook(self, is_audio: bool = True) -> "SearchQuery":
        return self._add(f"is_audio = {'true' if is_audio else 'false'}", _Priority.BTREE)
    
    def has_files(self) -> "SearchQuery":
        return self._add("has_files = true", _Priority.BTREE)
    
    def has_cover(self) -> "SearchQuery":
        return self._add("has_cover = true", _Priority.BTREE)
    
    def author_born_after(self, year: int) -> "SearchQuery":
        ph, nm = self._param(year)
        return self._add(f"max_author_birthyear >= {ph}", _Priority.BTREE, **{nm: year})
    
    def author_born_before(self, year: int) -> "SearchQuery":
        ph, nm = self._param(year)
        return self._add(f"min_author_birthyear <= {ph}", _Priority.BTREE, **{nm: year})
    
    # === Filters (Date) ===
    
    def released_after(self, date: str) -> "SearchQuery":
        ph, nm = self._param(date)
        return self._add(f"text_to_date_immutable(dc->>'date') >= ({ph})::date", _Priority.DATE, **{nm: date})
    
    def released_before(self, date: str) -> "SearchQuery":
        ph, nm = self._param(date)
        return self._add(f"text_to_date_immutable(dc->>'date') <= ({ph})::date", _Priority.DATE, **{nm: date})
    
    # === Filters (GIN JSONB) ===
    
    def locc(self, code: str) -> "SearchQuery":
        ph, nm = self._param(f'[{{"id": "{code}"}}]')
        return self._add(f"dc->'coverage' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"id": "{code}"}}]'})
    
    def has_contributor(self, role: str) -> "SearchQuery":
        ph, nm = self._param(f'[{{"role": "{role}"}}]')
        return self._add(f"dc->'creators' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"role": "{role}"}}]'})
    
    def file_type(self, ft: FileType) -> "SearchQuery":
        """Filter by file type using FileType enum."""
        mediatype = ft.value
        ph, nm = self._param(f'[{{"mediatype": "{mediatype}"}}]')
        return self._add(f"dc->'format' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"mediatype": "{mediatype}"}}]'})
    
    def author_id(self, author_id: int) -> "SearchQuery":
        ph, nm = self._param(f'[{{"id": {author_id}}}]')
        return self._add(f"dc->'creators' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"id": {author_id}}}]'})
    
    def subject_id(self, subject_id: int) -> "SearchQuery":
        ph, nm = self._param(f'[{{"id": {subject_id}}}]')
        return self._add(f"dc->'subjects' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"id": {subject_id}}}]'})
    
    def bookshelf_id(self, bookshelf_id: int) -> "SearchQuery":
        ph, nm = self._param(f'[{{"id": {bookshelf_id}}}]')
        return self._add(f"dc->'bookshelves' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"id": {bookshelf_id}}}]'})
    
    def encoding(self, enc: Encoding) -> "SearchQuery":
        """Filter by file encoding using Encoding enum."""
        encoding = enc.value
        ph, nm = self._param(f'[{{"encoding": "{encoding}"}}]')
        return self._add(f"dc->'format' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"encoding": "{encoding}"}}]'})
    
    def author_died_after(self, year: int) -> "SearchQuery":
        ph, nm = self._param(year)
        return self._add(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(dc->'creators') c WHERE (c->>'deathdate')::int >= {ph})",
            _Priority.GIN, **{nm: year}
        )
    
    def author_died_before(self, year: int) -> "SearchQuery":
        ph, nm = self._param(year)
        return self._add(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(dc->'creators') c WHERE (c->>'deathdate')::int > 0 AND (c->>'deathdate')::int <= {ph})",
            _Priority.GIN, **{nm: year}
        )

    # === Custom SQL ===
    
    def where(self, sql: str, **params) -> "SearchQuery":
        """Add custom SQL condition. Runs last (lowest priority).
        
        Use named params with :name syntax.
        
        Examples:
            .where("dc->>'publisher' = :pub", pub="Penguin")
            .where("jsonb_array_length(dc->'creators') > :n", n=2)
        """
        remapped_sql = sql
        remapped_params = {}
        for key, value in params.items():
            ph, nm = self._param(value)
            remapped_sql = remapped_sql.replace(f":{key}", ph)
            remapped_params[nm] = value
        return self._add(remapped_sql, _Priority.GIN, **{**remapped_params})

    # === Ordering ===
    
    def order_by(self, order: OrderBy) -> "SearchQuery":
        self._order = order
        return self
    
    def _order_clause(self, params: dict[str, Any]) -> str:
        # Use first search for relevance ordering
        if self._order == "relevance" and self._searches:
            txt, stype, col = self._searches[0]
            self._uid += 1
            nm = f"p{self._uid}"
            params[nm] = txt
            
            if stype in (SearchType.FUZZY, SearchType.CONTAINS):
                return f" ORDER BY word_similarity(:{nm}, {col}) DESC, downloads DESC"
            
            return f" ORDER BY ts_rank_cd({col}, websearch_to_tsquery('english', :{nm})) DESC, downloads DESC"
        
        orders = {
            "relevance": " ORDER BY downloads DESC",
            "downloads": " ORDER BY downloads DESC",
            "title": " ORDER BY title ASC",
            "author": " ORDER BY primary_author ASC NULLS LAST",
        }
        return orders.get(self._order, " ORDER BY downloads DESC")
    
    # === Build ===
    
    def _all_params(self) -> dict[str, Any]:
        merged = {}
        for c in self._conditions:
            merged.update(c.params)
        return merged
    
    def _where(self) -> str:
        if not self._conditions:
            return ""
        sorted_conditions = sorted(self._conditions, key=lambda c: c.priority)
        return " WHERE " + " AND ".join(c.sql for c in sorted_conditions)
    
    def build(self, limit: int | None = None, offset: int | None = None) -> tuple[str, dict[str, Any]]:
        params = self._all_params()
        actual_limit = limit if limit is not None else self._page_size
        actual_offset = offset if offset is not None else (self._page - 1) * self._page_size
        
        # Separate conditions by type
        search_conds = [c for c in self._conditions if c.priority in (_Priority.FTS, _Priority.TRGM)]
        filter_conds = [c for c in self._conditions if c.priority not in (_Priority.FTS, _Priority.TRGM)]
        
        # Columns needed for filtering and ordering
        base_cols = """book_id, title, primary_author, downloads, dc,
           copyrighted, primary_lang, is_audio, has_files, has_cover,
           max_author_birthyear, min_author_birthyear"""
        
        # Add search columns for relevance ordering
        for _, _, col in self._searches:
            if col not in base_cols:
                base_cols += f", {col}"
        
        order = self._order_clause(params)
        
        # Strategy: search in inner query, filters in outer
        if search_conds and filter_conds:
            search_where = " AND ".join(c.sql for c in search_conds)
            filter_where = " AND ".join(c.sql for c in sorted(filter_conds, key=lambda c: c.priority))
            
            sql = f"""SELECT book_id, title, primary_author, downloads, dc 
FROM (
    SELECT {base_cols}
    FROM mv_books_dc 
    WHERE {search_where}
) sub 
WHERE {filter_where}{order} 
LIMIT {actual_limit} OFFSET {actual_offset}"""
            return sql, params
        
        # Only search conditions
        if search_conds:
            search_where = " AND ".join(c.sql for c in search_conds)
            sql = f"""SELECT book_id, title, primary_author, downloads, dc 
FROM mv_books_dc 
WHERE {search_where}{order} 
LIMIT {actual_limit} OFFSET {actual_offset}"""
            return sql, params
        
        # Only filter conditions (or none)
        where = self._where()
        sql = f"""SELECT book_id, title, primary_author, downloads, dc 
FROM mv_books_dc{where}{order} 
LIMIT {actual_limit} OFFSET {actual_offset}"""
        return sql, params

    def build_count(self) -> tuple[str, dict[str, Any]]:
        params = self._all_params()
        
        search_conds = [c for c in self._conditions if c.priority in (_Priority.FTS, _Priority.TRGM)]
        filter_conds = [c for c in self._conditions if c.priority not in (_Priority.FTS, _Priority.TRGM)]
        
        base_cols = """book_id, dc, copyrighted, primary_lang, is_audio, has_files, has_cover,
           max_author_birthyear, min_author_birthyear, downloads"""
        
        if search_conds and filter_conds:
            search_where = " AND ".join(c.sql for c in search_conds)
            filter_where = " AND ".join(c.sql for c in sorted(filter_conds, key=lambda c: c.priority))
            
            sql = f"""SELECT COUNT(*) FROM (
    SELECT 1 FROM (
        SELECT {base_cols}
        FROM mv_books_dc 
        WHERE {search_where}
    ) sub 
    WHERE {filter_where}
) t"""
            return sql, params
        
        return f"SELECT COUNT(*) FROM mv_books_dc{self._where()}", params

class FullTextSearch:
    """Search interface for mv_books_dc."""
    
    def __init__(self, config: Config | None = None):
        cfg = config or Config()
        self.engine = create_engine(
            f"postgresql://{cfg.PGUSER}@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.PGDATABASE}",
            pool_pre_ping=True,
            pool_recycle=300,
        )
        self.Session = sessionmaker(bind=self.engine)
    
    def query(self) -> SearchQuery:
        return SearchQuery()
    
    def execute(self, q: SearchQuery) -> dict:
        with self.Session() as session:
            total = self._exact_count(session, q)
            total_pages = max(1, (total + q._page_size - 1) // q._page_size)
            
            page = max(1, min(q._page, total_pages))
            q._page = page
            
            sql, params = q.build()
            rows = session.execute(text(sql), params).fetchall()
            
            results = [
                {"book_id": r.book_id, "title": r.title, "author": r.primary_author, "downloads": r.downloads, "dc": r.dc}
                for r in rows
            ]
        
        return {
            "results": results,
            "page": page,
            "page_size": q._page_size,
            "total": total,
            "total_pages": total_pages,
        }
    
    def _exact_count(self, session, q: SearchQuery) -> int:
        sql, params = q.build_count()
        return session.execute(text(sql), params).scalar() or 0
    
    def get(self, etext_nr: int) -> dict | None:
        """Get a single book by etext number."""
        with self.Session() as session:
            sql = "SELECT book_id, title, primary_author, downloads, dc FROM mv_books_dc WHERE book_id = :id"
            row = session.execute(text(sql), {"id": etext_nr}).fetchone()
            if not row:
                return None
            return {"book_id": row.book_id, "title": row.title, "author": row.primary_author, "downloads": row.downloads, "dc": row.dc}
    
    def get_many(self, etext_nrs: list[int]) -> list[dict]:
        """Get multiple books by etext numbers in single query."""
        if not etext_nrs:
            return []
        with self.Session() as session:
            sql = "SELECT book_id, title, primary_author, downloads, dc FROM mv_books_dc WHERE book_id = ANY(:ids)"
            rows = session.execute(text(sql), {"ids": etext_nrs}).fetchall()
            return [
                {"book_id": r.book_id, "title": r.title, "author": r.primary_author, "downloads": r.downloads, "dc": r.dc}
                for r in rows
            ]
    
    def count(self, q: SearchQuery) -> int:
        with self.Session() as session:
            sql, params = q.build_count()
            return session.execute(text(sql), params).scalar() or 0