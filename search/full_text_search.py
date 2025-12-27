from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable

from constants import (
    Crosswalk,
    Encoding,
    FileType,
    Language,
    OrderBy,
    SearchField,
    SearchType,
    SortDirection,
)
from crosswalks import CROSSWALK_MAP
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

__all__ = [
    "Config",
    "FullTextSearch",
    "SearchQuery",
]


class Config:
    PGHOST = "localhost"
    PGPORT = "5432"
    PGDATABASE = "gutendb"
    PGUSER = "postgres"


# =============================================================================
# SearchQuery
# =============================================================================


@dataclass
class SearchQuery:
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

    def order_by(
        self, order: OrderBy, direction: SortDirection | None = None
    ) -> SearchQuery:
        self._order = order
        self._sort_dir = direction
        return self

    # === Search Methods ===

    def search(
        self,
        txt: str,
        field: SearchField = SearchField.BOOK,
        search_type: SearchType = SearchType.FTS,
    ) -> SearchQuery:
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
        txt = re.sub(r'"[^"]*"', "", txt)

        # Extract negations
        negations = re.findall(r"-(\S+)", txt)
        txt = re.sub(r"-\S+", "", txt)

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
            if word and word.lower() not in ("or", "and"):
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

    def lang(self, code: str | Language) -> SearchQuery:
        """Filter by language code (matches any language in multi-language books)."""
        if isinstance(code, Language):
            code_val = code.code
        else:
            code_val = code.lower()
        self._filter.append(
            ("lang_codes @> ARRAY[CAST(:lang AS text)]", {"lang": code_val})
        )
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

    def locc(self, code: str) -> SearchQuery:
        self._filter.append(
            (
                "EXISTS (SELECT 1 FROM mn_books_loccs mbl JOIN loccs lc ON lc.pk = mbl.fk_loccs WHERE mbl.fk_books = book_id AND lc.pk LIKE :locc_pattern)",
                {"locc_pattern": f"{code}%"},
            )
        )
        return self

    def has_contributor(self, role: str) -> SearchQuery:
        self._filter.append(
            ("dc->'creators' @> CAST(:j AS jsonb)", {"j": f'[{{"role":"{role}"}}]'})
        )
        return self

    def file_type(self, ft: FileType) -> SearchQuery:
        self._filter.append(
            (
                "dc->'format' @> CAST(:ft AS jsonb)",
                {"ft": f'[{{"mediatype":"{ft.value}"}}]'},
            )
        )
        return self

    def author_id(self, aid: int) -> SearchQuery:
        self._filter.append(
            ("dc->'creators' @> CAST(:aid AS jsonb)", {"aid": f'[{{"id":{int(aid)}}}]'})
        )
        return self

    def subject_id(self, sid: int) -> SearchQuery:
        """Filter by subject ID using MN table for fast indexed lookup."""
        self._filter.append(
            (
                "EXISTS (SELECT 1 FROM mn_books_subjects mbs WHERE mbs.fk_books = book_id AND mbs.fk_subjects = :sid)",
                {"sid": int(sid)},
            )
        )
        return self

    def bookshelf_id(self, bid: int) -> SearchQuery:
        """Filter by bookshelf ID using MN table for fast indexed lookup."""
        self._filter.append(
            (
                "EXISTS (SELECT 1 FROM mn_books_bookshelves mbb WHERE mbb.fk_books = book_id AND mbb.fk_bookshelves = :bid)",
                {"bid": int(bid)},
            )
        )
        return self

    def encoding(self, enc: Encoding) -> SearchQuery:
        self._filter.append(
            (
                "dc->'format' @> CAST(:enc AS jsonb)",
                {"enc": f'[{{"encoding":"{enc.value}"}}]'},
            )
        )
        return self

    def where(self, sql: str, **params) -> SearchQuery:
        """Add raw SQL filter condition. BE CAREFUL WHEN USING!"""
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
            return (
                f"SELECT COUNT(*) FROM (SELECT {_SUBQUERY} FROM mv_books_dc WHERE {search_sql}) t WHERE {filter_sql}",
                params,
            )
        elif search_sql:
            return f"SELECT COUNT(*) FROM mv_books_dc WHERE {search_sql}", params
        elif filter_sql:
            return f"SELECT COUNT(*) FROM mv_books_dc WHERE {filter_sql}", params
        return "SELECT COUNT(*) FROM mv_books_dc", params


# =============================================================================
# FullTextSearch
# =============================================================================

_FIELD_COLS = {
    SearchField.BOOK: ("tsvec", "book_text"),
    SearchField.TITLE: ("title_tsvec", "title"),
    SearchField.SUBTITLE: ("subtitle_tsvec", "subtitle"),
    SearchField.AUTHOR: ("author_tsvec", "all_authors"),
    SearchField.SUBJECT: ("subject_tsvec", "all_subjects"),
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
        return CROSSWALK_MAP[cw](row)

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
            return [
                {"id": r.id, "name": r.name, "book_count": r.book_count} for r in rows
            ]

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
            return [
                {"id": r.id, "name": r.name, "book_count": r.book_count} for r in rows
            ]

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

    def get_top_subjects_for_query(
        self, q: SearchQuery, limit: int = 15, max_books: int = 1000
    ) -> list[dict]:
        """
        Get top N subjects from a search result set for dynamic facets.

        Args:
            q: SearchQuery to derive subjects from
            limit: Maximum number of subjects to return (default 15)
            max_books: Maximum number of matching books to sample (default 1000)

        Returns:
            List of dicts with 'id', 'name', and 'count' keys, sorted by count desc
        """
        max_books = max(1, min(5000, int(max_books)))
        limit = max(1, min(100, int(limit)))

        params = q._params()
        order_sql = q._order_sql(params)
        search_sql = " AND ".join(s[0] for s in q._search) if q._search else None
        filter_sql = " AND ".join(f[0] for f in q._filter) if q._filter else None
        where_parts = [p for p in (search_sql, filter_sql) if p]
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

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
            return [{"id": r.id, "name": r.name, "count": r.count} for r in rows]
