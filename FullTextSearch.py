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
    UTF8 = "utf-8"
    ASCII = "us-ascii"
    LATIN1 = "iso-8859-1"
    WINDOWS1252 = "windows-1252"


FtsField = Literal["book", "author", "subject", "bookshelf"]
TrgmField = Literal["title", "author", "subject"]
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
    
    _FTS_COLS = {"book": "tsvec", "author": "author_tsvec", "subject": "subject_tsvec", "bookshelf": "bookshelf_tsvec"}
    _TRGM_COLS = {"title": "title", "author": "primary_author", "subject": "primary_subject"}
    
    _conditions: list[_Condition] = field(default_factory=list)
    _order: OrderBy = "relevance"
    _page: int = 1
    _page_size: int = 25
    _search_text: str | None = None
    _search_type: str = "fts"
    _search_col: str = "tsvec"
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
    
    # === FTS Search ===
    
    def search(self, txt: str) -> "SearchQuery":
        return self._fts(txt, "book")
    
    def search_author(self, txt: str) -> "SearchQuery":
        return self._fts(txt, "author")
    
    def search_subject(self, txt: str) -> "SearchQuery":
        return self._fts(txt, "subject")
    
    def search_bookshelf(self, txt: str) -> "SearchQuery":
        return self._fts(txt, "bookshelf")
    
    def _fts(self, txt: str, fld: FtsField) -> "SearchQuery":
        if not txt or not txt.strip():
            return self
        txt = txt.strip()
        self._search_text, self._search_type, self._search_col = txt, "fts", self._FTS_COLS[fld]
        ph, nm = self._param(txt)
        return self._add(f"{self._search_col} @@ websearch_to_tsquery('english', {ph})", _Priority.FTS, **{nm: txt})
    
    # === Trigram Search ===
    
    def title_like(self, txt: str) -> "SearchQuery":
        return self._trgm(txt, "title")
    
    def author_like(self, txt: str) -> "SearchQuery":
        return self._trgm(txt, "author")
    
    def subject_like(self, txt: str) -> "SearchQuery":
        return self._trgm(txt, "subject")
    
    def _trgm(self, txt: str, fld: TrgmField) -> "SearchQuery":
        if not txt or not txt.strip():
            return self
        txt = txt.strip()
        self._search_text, self._search_type, self._search_col = txt, "trgm", self._TRGM_COLS[fld]
        ph, nm = self._param(txt)
        return self._add(f"{ph} <% {self._search_col}", _Priority.TRGM, **{nm: txt})

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
    
    def subtitle_contains(self, text: str) -> "SearchQuery":
        ph, nm = self._param(f"%{text}%")
        return self._add(f"subtitle ILIKE {ph}", _Priority.TRGM, **{nm: f"%{text}%"})
    
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
    
    def bookshelf(self, name: str) -> "SearchQuery":
        ph, nm = self._param(f'[{{"bookshelf": "{name}"}}]')
        return self._add(f"dc->'bookshelves' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"bookshelf": "{name}"}}]'})
    
    def subject(self, name: str) -> "SearchQuery":
        ph, nm = self._param(f'[{{"subject": "{name}"}}]')
        return self._add(f"dc->'subjects' @> ({ph})::jsonb", _Priority.GIN, **{nm: f'[{{"subject": "{name}"}}]'})
    
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
        return self._add(remapped_sql, _Priority.GIN, **remapped_params)

    # === Ordering ===
    
    def order_by(self, order: OrderBy) -> "SearchQuery":
        self._order = order
        return self
    
    def _order_clause(self, params: dict[str, Any]) -> str:
        if self._order == "relevance" and self._search_text:
            self._uid += 1
            nm = f"p{self._uid}"
            params[nm] = self._search_text
            if self._search_type == "trgm":
                return f" ORDER BY word_similarity(:{nm}, {self._search_col}) DESC, downloads DESC"
            return f" ORDER BY ts_rank_cd({self._search_col}, websearch_to_tsquery('english', :{nm})) DESC, downloads DESC"
        
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
        
        fts_conds = [c for c in self._conditions if c.priority == _Priority.FTS]
        gin_conds = [c for c in self._conditions if c.priority == _Priority.GIN]
        
        if fts_conds and gin_conds:
            fts_where = " WHERE " + " AND ".join(c.sql for c in fts_conds)
            other_conds = [c for c in self._conditions if c.priority != _Priority.FTS]
            other_where = " AND ".join(c.sql for c in sorted(other_conds, key=lambda c: c.priority))
            order = self._order_clause(params)
            
            sql = f"""SELECT book_id, title, primary_author, downloads, dc FROM (
    SELECT book_id, title, primary_author, downloads, dc, 
           copyrighted, primary_lang, is_audio, has_files, {self._search_col}
    FROM mv_books_dc{fts_where} OFFSET 0
) fts WHERE {other_where}{order} LIMIT {actual_limit} OFFSET {actual_offset}"""
            return sql, params
        
        select = "SELECT book_id, title, primary_author, downloads, dc FROM mv_books_dc"
        where = self._where()
        order = self._order_clause(params)
        limit_clause = f" LIMIT {actual_limit} OFFSET {actual_offset}"
        return f"{select}{where}{order}{limit_clause}", params
    
    def build_count(self) -> tuple[str, dict[str, Any]]:
        fts_conds = [c for c in self._conditions if c.priority == _Priority.FTS]
        gin_conds = [c for c in self._conditions if c.priority == _Priority.GIN]
        params = self._all_params()
        
        if fts_conds and gin_conds:
            fts_where = " WHERE " + " AND ".join(c.sql for c in fts_conds)
            other_where = " AND ".join(c.sql for c in sorted(
                [c for c in self._conditions if c.priority != _Priority.FTS], 
                key=lambda c: c.priority
            ))
            sql = f"""SELECT COUNT(*) FROM (
    SELECT 1 FROM (
        SELECT book_id, dc, copyrighted, primary_lang, is_audio, has_files
        FROM mv_books_dc{fts_where} OFFSET 0
    ) fts WHERE {other_where}
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
    
    def count(self, q: SearchQuery) -> int:
        with self.Session() as session:
            sql, params = q.build_count()
            return session.execute(text(sql), params).scalar() or 0


if __name__ == "__main__":
    s = FullTextSearch()

    print("=" * 130)
    print(f"{'Test':<50} | {'Count':>8} | {'Pages':>6} | {'Page':>5} | {'Fetched':>7} | {'Time':>8} | First Title")
    print("=" * 130)
    
    tests = [
        ("FTS: Shakespeare", s.query().search("Shakespeare")[1, 10]),
        ("FTS Author: Twain", s.query().search_author("Twain")[1, 10]),
        ("Trigram Author: Twian (typo)", s.query().author_like("Twian")[1, 10]),
        ("FTS + lang(en)", s.query().search("Science").lang("en")[1, 10]),
        ("FTS + public_domain", s.query().search("Fiction").public_domain()[1, 10]),
        ("Audiobooks", s.query().audiobook()[1, 10]),
        ("Text only", s.query().search("Adventure").text_only()[1, 10]),
        ("LoCC PR", s.query().locc("PR").public_domain()[1, 10]),
        ("FTS Bookshelf: Science Fiction", s.query().search_bookshelf("Science Fiction")[1, 10]),
        ("FTS Subject: History", s.query().search_subject("History")[1, 10]),
        ("Has Translator", s.query().has_contributor("Translator").public_domain()[1, 10]),
        ("Has EPUB + Dickens", s.query().file_type(FileType.EPUB).search("Dickens")[1, 10]),
        ("Born after 1800", s.query().search("Novel").author_born_after(1800)[1, 10]),
        ("Released after 2020", s.query().released_after("2020-01-01")[1, 10]),
        ("Released 2020-2023", s.query().released_after("2020-01-01").released_before("2023-12-31")[1, 10]),
        ("Subtitle contains Vol", s.query().subtitle_contains("Vol")[1, 10]),
        ("Has cover", s.query().search("Novel").has_cover()[1, 10]),
        ("Combined SQL", s.query().search("History").lang("en").public_domain()[1, 10]),
        ("Combined + text_only", s.query().search("History").lang("en").public_domain().text_only()[1, 10]),
        ("FTS + Author FTS", s.query().search("Adventure").search_author("Twain")[1, 10]),
        ("Born after 1800 (page 5)", s.query().search("Novel").author_born_after(1800)[5, 5]),
        ("Born after 1800 (page 99)", s.query().search("Novel").author_born_after(1800)[99, 5]),        
        ("Author ID (Mark Twain = 53)", s.query().author_id(53)[1, 10]),
        ("Subject ID (Fiction = 1)", s.query().subject_id(1)[1, 10]),
        ("Bookshelf ID (Sci-Fi = 68)", s.query().bookshelf_id(68)[1, 10]),
        ("Bookshelf exact name", s.query().bookshelf("Science Fiction")[1, 10]),
        ("Subject exact name", s.query().subject("Fiction")[1, 10]),
        ("Encoding UTF-8", s.query().encoding(Encoding.UTF8).search("Novel")[1, 10]),
        
        # Etext
        ("Etext 1342 (Pride & Prejudice)", s.query().etext(1342)[1, 1]),
        ("Etexts batch", s.query().etexts([1342, 84, 11])[1, 10]),
        
        # File types
        ("FileType: EPUB", s.query().file_type(FileType.EPUB).search("Shakespeare")[1, 10]),
        ("FileType: KINDLE", s.query().file_type(FileType.KINDLE).search("Shakespeare")[1, 10]),
        ("FileType: PDF", s.query().file_type(FileType.PDF)[1, 10]),
        ("FileType: TXT", s.query().file_type(FileType.TXT).search("Shakespeare")[1, 10]),
        ("FileType: HTML", s.query().file_type(FileType.HTML).search("Shakespeare")[1, 10]),
        ("FileType: MP3", s.query().file_type(FileType.MP3)[1, 10]),
        
        # Custom SQL
        ("Custom: multi-author books", s.query().search("Novel").where("jsonb_array_length(dc->'creators') > :n", n=1)[1, 10]),
        ("Custom: has publisher", s.query().where("dc->>'publisher' IS NOT NULL")[1, 10]),
    ]
    
    for name, q in tests:
        start = time.perf_counter()
        try:
            data = s.execute(q)
            ms = (time.perf_counter() - start) * 1000
            fetched = len(data["results"])
            count_str = f"{data['total']:,}"
            pages_str = f"{data['total_pages']:,}"
            page_str = f"{data['page']}"
            first = data["results"][0]["title"][:20] + "..." if data["results"] else "N/A"
            print(f"{name:<50} | {count_str:>8} | {pages_str:>6} | {page_str:>5} | {fetched:>7} | {ms:>6.1f}ms | {first}")
        except Exception as e:
            ms = (time.perf_counter() - start) * 1000
            print(f"{name:<50} | {'ERROR':>8} | {'-':>6} | {'-':>5} | {'-':>7} | {ms:>6.1f}ms | {e}")
    
    print("=" * 130)