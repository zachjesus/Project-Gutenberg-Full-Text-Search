"""Simple FTS using mv_books_dc materialized view with query builder."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Literal
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
import time
import re


class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'


# Search field types
FtsField = Literal["book", "author", "subject", "bookshelf"]
TrgmField = Literal["title", "author", "subject"]  


@dataclass
class SearchQuery:
    """Query builder with dunder methods for chaining."""
    
    _conditions: list = field(default_factory=list)
    _params: dict = field(default_factory=dict)
    _order: str = "downloads DESC"
    _page: int = 1
    _page_size: int = 25
    _search_text: str = ""
    _search_type: str = "fts"  # "fts" or "trgm"
    _search_field: str = "book"
    _param_count: int = 0
    
    # Field mappings - FTS uses tsvector columns
    FTS_FIELDS = {
        "book": "tsvec",
        "author": "author_tsvec", 
        "subject": "subject_tsvec",
        "bookshelf": "bookshelf_tsvec",
    }
    
    # Trigram for title, author, subject (have gin_trgm_ops indexes)
    TRGM_FIELDS = {
        "title": "title",
        "author": "primary_author",
        "subject": "primary_subject",
    }
    
    def _add_param(self, value: Any) -> str:
        self._param_count += 1
        name = f"p{self._param_count}"
        self._params[name] = value
        return f":{name}"
    
    def _remap_params(self, other: "SearchQuery") -> list[str]:
        """Remap other's params to avoid collision, return remapped conditions."""
        offset = self._param_count
        remapped_conds = []
        
        for cond in other._conditions:
            new_cond = re.sub(
                r':p(\d+)',
                lambda m: f":p{int(m.group(1)) + offset}",
                cond
            )
            remapped_conds.append(new_cond)
        
        for old_name, value in other._params.items():
            new_name = f"p{int(old_name[1:]) + offset}"
            self._params[new_name] = value
        
        self._param_count = offset + other._param_count
        return remapped_conds
    
    def __and__(self, other: "SearchQuery") -> "SearchQuery":
        self._conditions.extend(self._remap_params(other))
        return self
    
    def __or__(self, other: "SearchQuery") -> "SearchQuery":
        if self._conditions and other._conditions:
            other_conds = self._remap_params(other)
            combined = f"(({' AND '.join(self._conditions)}) OR ({' AND '.join(other_conds)}))"
            self._conditions = [combined]
        return self
    
    def __getitem__(self, key: int | tuple) -> "SearchQuery":
        """Pagination: query[page] or query[page, page_size]"""
        if isinstance(key, tuple):
            self._page = max(1, key[0])
            self._page_size = max(1, min(100, key[1]))
        else:
            self._page = max(1, key)
        return self
    
    # =========================================================================
    # FTS Search Methods (tsvector - full-text search)
    # =========================================================================
    
    def search(self, txt: str) -> "SearchQuery":
        """FTS on book tsvec (default - title + subjects + authors)."""
        return self._fts_search(txt, "book")
    
    def search_author(self, txt: str) -> "SearchQuery":
        """FTS on author tsvec."""
        return self._fts_search(txt, "author")
    
    def search_subject(self, txt: str) -> "SearchQuery":
        """FTS on subject tsvec."""
        return self._fts_search(txt, "subject")
    
    def search_bookshelf(self, txt: str) -> "SearchQuery":
        """FTS on bookshelf tsvec."""
        return self._fts_search(txt, "bookshelf")
    
    def _fts_search(self, txt: str, fts_field: FtsField) -> "SearchQuery":
        """Internal FTS search on specified field."""
        if txt and txt.strip():
            self._search_text = txt.strip()
            self._search_type = "fts"
            self._search_field = fts_field
            p = self._add_param(self._search_text)
            col = self.FTS_FIELDS[fts_field]
            self._conditions.append(f"{col} @@ websearch_to_tsquery('english', {p})")
        return self
    
    # =========================================================================
    # Trigram Search Methods (fuzzy matching - title, author, subject)
    # =========================================================================
    
    def title_like(self, txt: str) -> "SearchQuery":
        """Trigram fuzzy search on title."""
        return self._trgm_search(txt, "title")
    
    def author_like(self, txt: str) -> "SearchQuery":
        """Trigram fuzzy search on primary author."""
        return self._trgm_search(txt, "author")
    
    def subject_like(self, txt: str) -> "SearchQuery":
        """Trigram fuzzy search on primary subject."""
        return self._trgm_search(txt, "subject")
    
    def _trgm_search(self, txt: str, trgm_field: TrgmField) -> "SearchQuery":
        """Internal trigram search on specified field."""
        if txt and txt.strip():
            self._search_text = txt.strip()
            self._search_type = "trgm"
            self._search_field = trgm_field
            p = self._add_param(self._search_text)
            col = self.TRGM_FIELDS[trgm_field]
            self._conditions.append(f"{col} % {p}")
        return self
    
    # =========================================================================
    # Filter Methods
    # =========================================================================
    
    def downloads_gte(self, n: int) -> "SearchQuery":
        self._conditions.append(f"downloads >= {self._add_param(n)}")
        return self
    
    def downloads_lte(self, n: int) -> "SearchQuery":
        self._conditions.append(f"downloads <= {self._add_param(n)}")
        return self
    
    def lang(self, code: str) -> "SearchQuery":
        self._conditions.append(f"primary_language = {self._add_param(code)}")
        return self
    
    def author(self, name: str) -> "SearchQuery":
        """ILIKE author filter (contains, not fuzzy)."""
        p = self._add_param(f"%{name}%")
        self._conditions.append(f"primary_author ILIKE {p}")
        return self
    
    def author_born_gte(self, year: int) -> "SearchQuery":
        self._conditions.append(f"author_birthdate >= {self._add_param(year)}")
        return self
    
    def author_born_lte(self, year: int) -> "SearchQuery":
        self._conditions.append(f"author_birthdate <= {self._add_param(year)}")
        return self
    
    def audiobook(self, is_audio: bool = True) -> "SearchQuery":
        self._conditions.append(f"is_audiobook = {self._add_param(is_audio)}")
        return self
    
    def public_domain(self) -> "SearchQuery":
        self._conditions.append("copyrighted = 0")
        return self
    
    def has_files(self) -> "SearchQuery":
        self._conditions.append("has_files = true")
        return self
    
    def order_by(self, clause: str) -> "SearchQuery":
        self._order = clause
        return self
    
    # =========================================================================
    # Build
    # =========================================================================
    
    def build(self) -> tuple[str, dict]:
        select = "SELECT book_id, title, primary_author, downloads, dc FROM mv_books_dc"
        where = f" WHERE {' AND '.join(self._conditions)}" if self._conditions else ""
        
        order = f" ORDER BY {self._order}"
        if self._search_text:
            p = self._add_param(self._search_text)
            if self._search_type == "trgm":
                col = self.TRGM_FIELDS[self._search_field]
                order = f" ORDER BY similarity({col}, {p}) DESC"
            else:
                col = self.FTS_FIELDS[self._search_field]
                order = f" ORDER BY ts_rank_cd({col}, websearch_to_tsquery('english', {p})) DESC"
        
        offset = (self._page - 1) * self._page_size
        limit = f" LIMIT {self._page_size} OFFSET {offset}"
        
        return f"{select}{where}{order}{limit}", self._params
    
    def __repr__(self) -> str:
        return f"SearchQuery(type={self._search_type}, field={self._search_field}, page={self._page}, conditions={len(self._conditions)})"


class FullTextSearch:
    """Simple search interface."""
    
    def __init__(self, config: Config = None):
        cfg = config or Config()
        conn_str = f"postgresql://{cfg.PGUSER}@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.PGDATABASE}"
        self.engine = create_engine(conn_str)
        self.Session = sessionmaker(bind=self.engine)
    
    def query(self) -> SearchQuery:
        return SearchQuery()
    
    def execute(self, q: SearchQuery) -> dict:
        """Execute query and return results with pagination metadata."""
        total = self._count_query(q)
        total_pages = max(1, (total + q._page_size - 1) // q._page_size)
        
        # Cap page to max
        if q._page > total_pages:
            q._page = total_pages
        
        sql, params = q.build()
        
        with self.Session() as session:
            try:
                result = session.execute(text(sql), params)
                results = [
                    {"book_id": r.book_id, "title": r.title, 
                     "author": r.primary_author, "downloads": r.downloads, "dc": r.dc}
                    for r in result
                ]
            except Exception as e:
                print(f"Query error: {e}")
                results = []
        
        return {
            "results": results,
            "page": q._page,
            "page_size": q._page_size,
            "total": total,
            "total_pages": total_pages
        }
    
    def _count_query(self, q: SearchQuery) -> int:
        """Count without modifying original query."""
        where = f" WHERE {' AND '.join(q._conditions)}" if q._conditions else ""
        count_sql = f"SELECT COUNT(*) FROM mv_books_dc{where}"
        
        with self.Session() as session:
            try:
                return session.execute(text(count_sql), q._params).scalar() or 0
            except Exception as e:
                print(f"Count error: {e}")
                return 0
    
    def count(self, q: SearchQuery) -> int:
        """Public count method."""
        return self._count_query(q)


def fmt_result(r: dict, i: int, offset: int = 0) -> str:
    """Format a single result for display."""
    title = ' '.join(r['title'].split())
    author = r['author'] or 'Unknown'
    if len(title) > 60:
        title = title[:57] + '...'
    if len(author) > 40:
        author = author[:37] + '...'
    return f"  {offset + i + 1:2}. {title}\n      by {author} | {r['downloads']:,} downloads"


if __name__ == "__main__":
    s = FullTextSearch()
    
    # Test 1: Default FTS (book tsvec)
    print("=" * 70)
    print("FTS: search('Computer') - book tsvec")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .search("Computers")
        .lang("en")
        .public_domain()
        [1, 10]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))
    
    # Test 2: Author FTS
    print("\n" + "=" * 70)
    print("FTS: search_author('Dickens') - author tsvec")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .search_author("Dickens")
        .public_domain()
        [1, 20]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))
    
    # Test 3: Subject FTS
    print("\n" + "=" * 70)
    print("FTS: search_subject('Science Fiction') - subject tsvec")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .search_subject("Science Fiction")
        .downloads_gte(100)
        [1, 30]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))
    
    # Test 4: Bookshelf FTS
    print("\n" + "=" * 70)
    print("FTS: search_bookshelf('Adventure') - bookshelf tsvec")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .search_bookshelf("Adventure")
        .downloads_gte(50)
        [1, 30]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))
    
    # Test 5: Trigram title search (fuzzy)
    print("\n" + "=" * 70)
    print("Trigram: title_like('Shrlock Holms') - fuzzy title")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .title_like("Shrlock Holms")
        [1, 30]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))
    
    # Test 6: Trigram author search (fuzzy)
    print("\n" + "=" * 70)
    print("Trigram: author_like('Dikens') - fuzzy author")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .author_like("Dikens")
        [1, 30]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))
    
    # Test 7: Chained - FTS subject + trigram author
    print("\n" + "=" * 70)
    print("Chained: search_subject('Fiction') + author_like('Austen')")
    print("=" * 70)
    
    start = time.perf_counter()
    data = s.execute(
        s.query()
        .search_subject("Fiction")
        .author_like("Austen")
        .public_domain()
        [1, 5]
    )
    elapsed = (time.perf_counter() - start) * 1000
    print(f"Page {data['page']} of {data['total_pages']} | {data['total']} total | {elapsed:.1f}ms\n")
    for i, r in enumerate(data['results']):
        print(fmt_result(r, i))