from __future__ import annotations

from typing import Any, Dict, List, Tuple
from urllib.parse import quote, unquote, urlencode

import cherrypy
from sqlalchemy import text

from search.constants import (
    Crosswalk,
    CuratedBookshelves,
    Language,
    LoCCMainClass,
    OrderBy,
    SearchField,
    SearchType,
    SortDirection,
)
from search.full_text_search import FullTextSearch

SAMPLE_LIMIT = 15

LANGUAGE_LIST = [
    {"code": lang_enum.code, "label": lang_enum.label} for lang_enum in Language
]
_VALID_SORTS = set(OrderBy._value2member_map_.keys())


def _parse_field(field: str) -> Tuple[SearchField, SearchType]:
    """Parse field param to (SearchField, SearchType). Default is fuzzy search."""
    if field.startswith("fts_"):
        search_type = SearchType.FTS
        field_name = field[4:]
    elif field.startswith("fuzzy_"):
        search_type = SearchType.FUZZY
        field_name = field[6:]
    else:
        search_type = SearchType.FUZZY
        field_name = field

    field_name = "book" if field_name == "keyword" else field_name

    if field_name not in {f.value for f in SearchField}:
        return SearchField.BOOK, SearchType.FUZZY
    return SearchField(field_name), search_type


def _facet_link(href: str, title: str, is_active: bool) -> dict:
    """Build a facet link. Only includes 'rel' if active (per OPDS 2.0 spec)."""
    link = {"href": href, "type": "application/opds+json", "title": title}
    if is_active:
        link["rel"] = "self"
    return link


def _url_with_params(path: str, params: dict) -> str:
    """Build URL with proper query-string encoding."""
    clean = {k: v for k, v in params.items() if v not in ("", None)}
    qs = urlencode(clean, doseq=True)
    return f"{path}?{qs}" if qs else path


class API:
    def __init__(self):
        self.fts = FullTextSearch()

    def _append_pagination_links(
        self, links: List[Dict[str, Any]], build_url_fn, result: dict
    ) -> None:
        """Append first/previous/next/last pagination links to links list."""
        if result.get("page", 1) > 1:
            links.append(
                {
                    "rel": "first",
                    "href": build_url_fn(1),
                    "type": "application/opds+json",
                }
            )
            links.append(
                {
                    "rel": "previous",
                    "href": build_url_fn(result["page"] - 1),
                    "type": "application/opds+json",
                }
            )
        if result.get("page", 1) < result.get("total_pages", 1):
            links.append(
                {
                    "rel": "next",
                    "href": build_url_fn(result["page"] + 1),
                    "type": "application/opds+json",
                }
            )
            links.append(
                {
                    "rel": "last",
                    "href": build_url_fn(result["total_pages"]),
                    "type": "application/opds+json",
                }
            )

    # ---------- Index (navigation) ----------
    @cherrypy.expose
    @cherrypy.tools.json_out()  # type: ignore[attr-defined]
    def index(self):
        """Root catalog - navigation only (curated bookshelf groups live under /opds/bookshelves?category=...)."""
        navigation = [
            {
                "href": "/opds/search?field=fuzzy_keyword",
                "title": "Search Fuzzy (Typo-Tolerant, Slower)",
                "type": "application/opds+json",
                "rel": "subsection",
            },
            {
                "href": "/opds/search?field=fts_keyword",
                "title": 'Search FTS (Strict, Faster, operators: "quotes", or, and, - for negate)',
                "type": "application/opds+json",
                "rel": "subsection",
            },
            {
                "href": "/opds/bookshelves",
                "title": "Browse by Bookshelf",
                "type": "application/opds+json",
                "rel": "subsection",
            },
            {
                "href": "/opds/loccs",
                "title": "Browse by LoCC (Subject Classification)",
                "type": "application/opds+json",
                "rel": "subsection",
            },
            {
                "href": "/opds/subjects",
                "title": "Browse by Subject",
                "type": "application/opds+json",
                "rel": "subsection",
            },
            {
                "href": "/opds/search?sort=downloads&sort_order=desc",
                "title": "Most Popular",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/sort/popular",
            },
            {
                "href": "/opds/search?sort=release_date&sort_order=desc",
                "title": "Recently Added",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/sort/new",
            },
            {
                "href": "/opds/search?sort=random",
                "title": "Random",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/sort/random",
            },
        ]

        # Return navigation only. Curated bookshelf samples are shown per-category
        return {
            "metadata": {"title": "Project Gutenberg Catalog"},
            "links": [
                {"rel": "self", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {
                    "rel": "search",
                    "href": "/opds/search{?query,field,lang,sort,copyrighted,audiobook,locc}",
                    "type": "application/opds+json",
                    "templated": True,
                },
            ],
            "navigation": navigation,
        }

    # ---------- Bookshelves ----------
    @cherrypy.expose
    @cherrypy.tools.json_out()  # type: ignore[attr-defined]
    def bookshelves(
        self,
        id: int | None = None,
        category: str | None = None,
        page: int = 1,
        limit: int = 28,
        query: str = "",
        lang: str = "",
        copyrighted: str = "",
        audiobook: str = "",
        sort: str = "",
        sort_order: str = "",
    ):
        """
        Bookshelf navigation using CuratedBookshelves.

        - No id/category: list categories + grouped sample publications
        - category: list bookshelf navigation links for that category
        - id: browse books in that bookshelf (with facets & pagination)
        """
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28

        # Detail view for a single bookshelf id -> return publications + facets
        if id is not None:
            try:
                bookshelf_id = int(id)
            except (ValueError, TypeError):
                raise cherrypy.HTTPError(400, "Invalid bookshelf ID")

            # Resolve bookshelf name and parent category
            bookshelf_name = f"Bookshelf {bookshelf_id}"
            parent_category = None
            for cat in CuratedBookshelves:
                for sid, sname in cat.shelves:
                    if sid == bookshelf_id:
                        bookshelf_name = sname
                        parent_category = cat.genre
                        break
                if parent_category:
                    break

            # Build query
            try:
                q = self.fts.query(crosswalk=Crosswalk.OPDS)
                q.bookshelf_id(bookshelf_id)

                if query.strip():
                    sf, st = _parse_field("keyword")
                    q.search(query, field=sf, search_type=st)

                if lang:
                    q.lang(lang)
                if copyrighted == "true":
                    q.copyrighted()
                elif copyrighted == "false":
                    q.public_domain()
                if audiobook == "true":
                    q.audiobook()
                elif audiobook == "false":
                    q.text_only()

                if sort in _VALID_SORTS:
                    direction = (
                        SortDirection.ASC
                        if sort_order == "asc"
                        else SortDirection.DESC
                        if sort_order == "desc"
                        else None
                    )
                    q.order_by(OrderBy(sort), direction)
                elif query.strip():
                    q.order_by(OrderBy.RELEVANCE)
                else:
                    q.order_by(OrderBy.DOWNLOADS)

                q[page, limit]
                result = self.fts.execute(q)
            except Exception as e:
                cherrypy.log(f"Bookshelf browse error: {e}")
                raise cherrypy.HTTPError(500, "Browse failed")

            up_href = (
                f"/opds/bookshelves?category={quote(parent_category)}"
                if parent_category
                else "/opds/bookshelves"
            )

            def build_url(p: int) -> str:
                params = {
                    "id": bookshelf_id,
                    "query": query,
                    "page": p,
                    "limit": limit,
                    "lang": lang,
                    "copyrighted": copyrighted,
                    "audiobook": audiobook,
                    "sort": sort,
                    "sort_order": sort_order,
                }
                return _url_with_params("/opds/bookshelves", params)

            self_href = build_url(result["page"])

            # Get dynamic top subjects for facets
            top_subjects = None
            try:
                q_sub = self.fts.query()
                q_sub.bookshelf_id(bookshelf_id)
                if query.strip():
                    sf, st = _parse_field("keyword")
                    q_sub.search(query, field=sf, search_type=st)
                if lang:
                    q_sub.lang(lang)
                if copyrighted == "true":
                    q_sub.copyrighted()
                elif copyrighted == "false":
                    q_sub.public_domain()
                if audiobook == "true":
                    q_sub.audiobook()
                elif audiobook == "false":
                    q_sub.text_only()
                top_subjects = self.fts.get_top_subjects_for_query(
                    q_sub, limit=15, max_books=500
                )
            except Exception as e:
                cherrypy.log(f"Top subjects error: {e}")

            facets = self._build_bookshelf_facets(
                bookshelf_id,
                query,
                limit,
                lang,
                copyrighted,
                audiobook,
                sort,
                sort_order,
                top_subjects,
            )

            feed = {
                "metadata": {
                    "title": bookshelf_name,
                    "numberOfItems": result["total"],
                    "itemsPerPage": result["page_size"],
                    "currentPage": result["page"],
                },
                "links": [
                    {"rel": "self", "href": self_href, "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {"rel": "up", "href": up_href, "type": "application/opds+json"},
                    {
                        "rel": "search",
                        "href": f"/opds/bookshelves?id={bookshelf_id}{{&query,lang,sort,copyrighted,audiobook}}",
                        "type": "application/opds+json",
                        "templated": True,
                    },
                ],
                "publications": result["results"],
                "facets": facets,
            }

            self._append_pagination_links(feed["links"], build_url, result)
            return feed

        # Category listing -> navigation of shelves in that category + per-shelf sample groups
        if category is not None:
            category = unquote(category)
            found = None
            for cat in CuratedBookshelves:
                if cat.genre == category:
                    found = cat
                    break
            if not found:
                raise cherrypy.HTTPError(404, "Category not found")

            shelves = [{"id": s[0], "name": s[1]} for s in found.shelves]
            navigation = [
                {
                    "href": f"/opds/bookshelves?id={s['id']}",
                    "title": s["name"],
                    "type": "application/opds+json",
                    "rel": "http://opds-spec.org/acquisition",
                }
                for s in shelves
            ]

            # Batch fetch sample publications for all shelves in this category (SAMPLE_LIMIT per shelf)
            SAMPLE_LIMIT = 15
            groups = []
            for s in shelves:
                shelf_id = s["id"]
                try:
                    q = self.fts.query(crosswalk=Crosswalk.OPDS)
                    q.bookshelf_id(shelf_id)
                    # Order randomly to get a sample set
                    q.order_by(OrderBy.RANDOM)
                    # Get first page with SAMPLE_LIMIT items
                    q[1, SAMPLE_LIMIT]
                    result = self.fts.execute(q)
                    pubs = result.get("results", [])
                    total = result.get("total", 0)
                except Exception as e:
                    cherrypy.log(
                        f"Error fetching bookshelf samples for shelf {shelf_id}: {e}"
                    )
                    pubs = []
                    total = 0

                if not pubs:
                    continue

                groups.append(
                    {
                        "metadata": {
                            "title": s["name"],
                            "numberOfItems": total,
                        },
                        "links": [
                            {
                                "href": f"/opds/bookshelves?id={s['id']}",
                                "rel": "self",
                                "type": "application/opds+json",
                            }
                        ],
                        "publications": pubs,
                    }
                )

            return {
                "metadata": {"title": category, "numberOfItems": len(shelves)},
                "links": [
                    {
                        "rel": "self",
                        "href": f"/opds/bookshelves?category={quote(category)}",
                        "type": "application/opds+json",
                    },
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {
                        "rel": "up",
                        "href": "/opds/bookshelves",
                        "type": "application/opds+json",
                    },
                ],
                "navigation": navigation,
                "groups": groups,
            }

        # No id/category -> return category navigation only.
        # Per-category groups (sampled publications) are provided by the category endpoint.
        navigation = [
            {
                "href": f"/opds/bookshelves?category={quote(cat.genre)}",
                "title": f"{cat.genre} ({len(cat.shelves)} shelves)",
                "type": "application/opds+json",
                "rel": "subsection",
            }
            for cat in CuratedBookshelves
        ]
        return {
            "metadata": {
                "title": "Bookshelves",
                "numberOfItems": len(CuratedBookshelves),
            },
            "links": [
                {
                    "rel": "self",
                    "href": "/opds/bookshelves",
                    "type": "application/opds+json",
                },
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/", "type": "application/opds+json"},
            ],
            "navigation": navigation,
        }

    def _build_bookshelf_facets(
        self,
        bookshelf_id,
        query,
        limit,
        lang,
        copyrighted,
        audiobook,
        sort,
        sort_order,
        top_subjects=None,
    ):
        """Build facets for bookshelf browsing (detail view)."""

        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "id": bookshelf_id,
                "query": q,
                "page": 1,
                "limit": limit,
                "lang": lng,
                "copyrighted": cr,
                "audiobook": ab,
                "sort": srt,
                "sort_order": srt_ord,
            }
            return _url_with_params("/opds/bookshelves", params)

        facets = [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "downloads", "desc"),
                        "Most Popular",
                        sort == "downloads" or not sort,
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "relevance", ""),
                        "Relevance",
                        sort == "relevance",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "title", "asc"),
                        "Title (A-Z)",
                        sort == "title",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "author", "asc"),
                        "Author (A-Z)",
                        sort == "author",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "random", ""),
                        "Random",
                        sort == "random",
                    ),
                ],
            },
        ]

        if top_subjects:
            facets.append(
                {
                    "metadata": {"title": "Top Subjects in Results"},
                    "links": [
                        {
                            "href": f"/opds/subjects?id={s['id']}",
                            "type": "application/opds+json",
                            "title": f"{s['name']} ({s['count']})",
                        }
                        for s in top_subjects
                    ],
                }
            )

        facets.extend(
            [
                {
                    "metadata": {"title": "Copyright Status"},
                    "links": [
                        _facet_link(
                            url(query, lang, "", audiobook, sort, sort_order),
                            "Any",
                            not copyrighted,
                        ),
                        _facet_link(
                            url(query, lang, "false", audiobook, sort, sort_order),
                            "Public Domain",
                            copyrighted == "false",
                        ),
                        _facet_link(
                            url(query, lang, "true", audiobook, sort, sort_order),
                            "Copyrighted",
                            copyrighted == "true",
                        ),
                    ],
                },
                {
                    "metadata": {"title": "Format"},
                    "links": [
                        _facet_link(
                            url(query, lang, copyrighted, "", sort, sort_order),
                            "Any",
                            not audiobook,
                        ),
                        _facet_link(
                            url(query, lang, copyrighted, "false", sort, sort_order),
                            "Text",
                            audiobook == "false",
                        ),
                        _facet_link(
                            url(query, lang, copyrighted, "true", sort, sort_order),
                            "Audiobook",
                            audiobook == "true",
                        ),
                    ],
                },
                {
                    "metadata": {"title": "Language"},
                    "links": [
                        _facet_link(
                            url(query, "", copyrighted, audiobook, sort, sort_order),
                            "Any",
                            not lang,
                        )
                    ]
                    + [
                        _facet_link(
                            url(
                                query,
                                item["code"],
                                copyrighted,
                                audiobook,
                                sort,
                                sort_order,
                            ),
                            item["label"],
                            lang == item["code"],
                        )
                        for item in LANGUAGE_LIST
                    ],
                },
            ]
        )
        return facets

    # ---------- LoCC (hierarchical) ----------
    @cherrypy.expose
    @cherrypy.tools.json_out()  # type: ignore[attr-defined]
    def loccs(
        self,
        parent: str = "",
        page: int = 1,
        limit: int = 28,
        query: str = "",
        lang: str = "",
        copyrighted: str = "",
        audiobook: str = "",
        sort: str = "",
        sort_order: str = "",
    ):
        """
        LoCC hierarchical navigation (simplified).

        - Without parent: list top-level LoCC classes (navigation)
        - With parent:
          - If children exist: return navigation of those children (no groups)
          - If no children: treat as leaf and return books (publications) with facets/pagination
        """
        parent = (parent or "").strip().upper()
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28

        # Get children via FullTextSearch helper (delegates to helpers.get_locc_children)
        try:
            children = self.fts.get_locc_children(parent)
        except Exception as e:
            cherrypy.log(f"LoCC children error: {e}")
            children = []

        # If there are children, return navigation entries for them directly.
        # The helper `get_locc_children` already returns descendant rows (pk/locc/has_children),
        # so there's no need to expand via BFS here. Use the returned list so the
        # client can display navigation links and click through to either more nav
        # or to a leaf (books) when appropriate.
        if children:
            # Ensure deterministic ordering similar to previous behavior.
            children.sort(key=lambda x: (len(x.get("code", "")), x.get("code", "")))

            navigation = [
                {
                    "href": f"(




                                    )

                                    /opds/loccs?parent={child['code']}",
                    "title": child.get("label", child["code"]),
                    "type": "application/opds+json",
                    "rel": "subsection"
                    if child.get("has_children")
                    else "h
                                        t
                                    tp://opds-spec.org/acquisition",
                }
                for child in children
            ]

            links = [
                {
                    "rel": "self",
                    "href": f"/opds/loccs?parent={parent}" if parent else "/opds/loccs",
                    "type": "application/opds+json",
                },
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {
                    "rel": "up",
                    "href": "/opds/loccs" if parent else "/opds/",
                    "type": "application/opds+json",
                },
            ]

            return {
                "metadata": {
                    "title": parent or "Subject Classification",
                },
                "links": links,
                "navigation": navigation,
            }

        # No children -> leaf node: return books with filtering/sorting/facets (similar to other endpoints)
        try:
            q = self.fts.query(crosswalk=Crosswalk.OPDS)
            q.locc(parent)

            if query.strip():
                sf, st = _parse_field("keyword")
                q.search(query, field=sf, search_type=st)

            if lang:
                q.lang(lang)
            if copyrighted == "true":
                q.copyrighted()
            elif copyrighted == "false":
                q.public_domain()
            if audiobook == "true":
                q.audiobook()
            elif audiobook == "false":
                q.text_only()

            if sort in _VALID_SORTS:
                direction = (
                    SortDirection.ASC
                    if sort_order == "asc"
                    else SortDirection.DESC
                    if sort_order == "desc"
                    else None
                )
                q.order_by(OrderBy(sort), direction)
            elif query.strip():
                q.order_by(OrderBy.RELEVANCE)
            else:
                q.order_by(OrderBy.DOWNLOADS)

            q[page, limit]
            result = self.fts.execute(q)
        except Exception as e:
            cherrypy.log(f"LoCC browse error: {e}")
            raise cherrypy.HTTPError(500, "Browse failed")

        # URL builder for pagination/facets
        def build_url(p: int) -> str:
            params = {
                "parent": parent,
                "query": query,
                "page": p,
                "limit": limit,
                "lang": lang,
                "copyrighted": copyrighted,
                "audiobook": audiobook,
                "sort": sort,
                "sort_order": sort_order,
            }
            return _url_with_params("/opds/loccs", params)

        self_href = build_url(result["page"])

        # dynamic top subjects for facets (optional)
        top_subjects = None
        try:
            q_sub = self.fts.query()
            q_sub.locc(parent)
            if query.strip():
                sf, st = _parse_field("keyword")
                q_sub.search(query, field=sf, search_type=st)
            if lang:
                q_sub.lang(lang)
            if copyrighted == "true":
                q_sub.copyrighted()
            elif copyrighted == "false":
                q_sub.public_domain()
            if audiobook == "true":
                q_sub.audiobook()
            elif audiobook == "false":
                q_sub.text_only()
            top_subjects = self.fts.get_top_subjects_for_query(
                q_sub, limit=15, max_books=500
            )
        except Exception as e:
            cherrypy.log(f"Top subjects error: {e}")

        facets = self._build_locc_facets(
            parent,
            query,
            limit,
            lang,
            copyrighted,
            audiobook,
            sort,
            sort_order,
            top_subjects,
        )

        feed = {
            "metadata": {
                "title": parent,
                "numberOfItems": result["total"],
                "itemsPerPage": result["page_size"],
                "currentPage": result["page"],
            },
            "links": [
                {"rel": "self", "href": self_href, "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/loccs", "type": "application/opds+json"},
                {
                    "rel": "search",
                    "href": f"/opds/loccs?parent={parent}{{&query,lang,sort,copyrighted,audiobook}}",
                    "type": "application/opds+json",
                    "templated": True,
                },
            ],
            "publications": result["results"],
            "facets": facets,
        }

        # Add pagination links
        self._append_pagination_links(feed["links"], build_url, result)
        return feed

    def _build_locc_facets(
        self,
        parent,
        query,
        limit,
        lang,
        copyrighted,
        audiobook,
        sort,
        sort_order,
        top_subjects=None,
    ):
        """Build facets for LoCC leaf node."""

        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "parent": parent,
                "query": q,
                "page": 1,
                "limit": limit,
                "lang": lng,
                "copyrighted": cr,
                "audiobook": ab,
                "sort": srt,
                "sort_order": srt_ord,
            }
            return _url_with_params("/opds/loccs", params)

        facets = [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "downloads", "desc"),
                        "Most Popular",
                        sort == "downloads" or not sort,
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "relevance", ""),
                        "Relevance",
                        sort == "relevance",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "title", "asc"),
                        "Title (A-Z)",
                        sort == "title",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "author", "asc"),
                        "Author (A-Z)",
                        sort == "author",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "random", ""),
                        "Random",
                        sort == "random",
                    ),
                ],
            },
        ]

        if top_subjects:
            facets.append(
                {
                    "metadata": {"title": "Top Subjects in Results"},
                    "links": [
                        {
                            "href": f"/opds/subjects?id={s['id']}",
                            "type": "application/opds+json",
                            "title": f"{s['name']} ({s['count']})",
                        }
                        for s in top_subjects
                    ],
                }
            )

        facets.extend(
            [
                {
                    "metadata": {"title": "Copyright Status"},
                    "links": [
                        _facet_link(
                            url(query, lang, "", audiobook, sort, sort_order),
                            "Any",
                            not copyrighted,
                        ),
                        _facet_link(
                            url(query, lang, "false", audiobook, sort, sort_order),
                            "Public Domain",
                            copyrighted == "false",
                        ),
                        _facet_link(
                            url(query, lang, "true", audiobook, sort, sort_order),
                            "Copyrighted",
                            copyrighted == "true",
                        ),
                    ],
                },
                {
                    "metadata": {"title": "Format"},
                    "links": [
                        _facet_link(
                            url(query, lang, copyrighted, "", sort, sort_order),
                            "Any",
                            not audiobook,
                        ),
                        _facet_link(
                            url(query, lang, copyrighted, "false", sort, sort_order),
                            "Text",
                            audiobook == "false",
                        ),
                        _facet_link(
                            url(query, lang, copyrighted, "true", sort, sort_order),
                            "Audiobook",
                            audiobook == "true",
                        ),
                    ],
                },
                {
                    "metadata": {"title": "Language"},
                    "links": [
                        _facet_link(
                            url(query, "", copyrighted, audiobook, sort, sort_order),
                            "Any",
                            not lang,
                        )
                    ]
                    + [
                        _facet_link(
                            url(
                                query,
                                item["code"],
                                copyrighted,
                                audiobook,
                                sort,
                                sort_order,
                            ),
                            item["label"],
                            lang == item["code"],
                        )
                        for item in LANGUAGE_LIST
                    ],
                },
            ]
        )
        return facets

    # ---------- Subjects ----------
    @cherrypy.expose
    @cherrypy.tools.json_out()  # type: ignore[attr-defined]
    def subjects(
        self,
        id: int | None = None,
        page: int = 1,
        limit: int = 28,
        query: str = "",
        lang: str = "",
        copyrighted: str = "",
        audiobook: str = "",
        sort: str = "",
        sort_order: str = "",
    ):
        """Subject navigation and detail (subject -> books)."""
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28

        # Detail view for a subject id -> books + facets
        if id is not None:
            try:
                subject_id = int(id)
            except (ValueError, TypeError):
                raise cherrypy.HTTPError(400, "Invalid subject ID")

            subject_name = (
                self.fts.get_subject_name(subject_id) or f"Subject {subject_id}"
            )
            try:
                q = self.fts.query(crosswalk=Crosswalk.OPDS)
                q.subject_id(subject_id)

                if query.strip():
                    sf, st = _parse_field("keyword")
                    q.search(query, field=sf, search_type=st)

                if lang:
                    q.lang(lang)
                if copyrighted == "true":
                    q.copyrighted()
                elif copyrighted == "false":
                    q.public_domain()
                if audiobook == "true":
                    q.audiobook()
                elif audiobook == "false":
                    q.text_only()

                if sort in _VALID_SORTS:
                    direction = (
                        SortDirection.ASC
                        if sort_order == "asc"
                        else SortDirection.DESC
                        if sort_order == "desc"
                        else None
                    )
                    q.order_by(OrderBy(sort), direction)
                elif query.strip():
                    q.order_by(OrderBy.RELEVANCE)
                else:
                    q.order_by(OrderBy.DOWNLOADS)

                q[page, limit]
                result = self.fts.execute(q)
            except Exception as e:
                cherrypy.log(f"Subject browse error: {e}")
                raise cherrypy.HTTPError(500, "Browse failed")

            def build_url(p: int) -> str:
                params = {
                    "id": subject_id,
                    "query": query,
                    "page": p,
                    "limit": limit,
                    "lang": lang,
                    "copyrighted": copyrighted,
                    "audiobook": audiobook,
                    "sort": sort,
                    "sort_order": sort_order,
                }
                return _url_with_params("/opds/subjects", params)

            self_href = build_url(result["page"])
            facets = self._build_subject_facets(
                subject_id, query, limit, lang, copyrighted, audiobook, sort, sort_order
            )

            feed = {
                "metadata": {
                    "title": subject_name,
                    "numberOfItems": result["total"],
                    "itemsPerPage": result["page_size"],
                    "currentPage": result["page"],
                },
                "links": [
                    {"rel": "self", "href": self_href, "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {
                        "rel": "up",
                        "href": "/opds/subjects",
                        "type": "application/opds+json",
                    },
                    {
                        "rel": "search",
                        "href": f"/opds/subjects?id={subject_id}{{&query,lang,sort,copyrighted,audiobook}}",
                        "type": "application/opds+json",
                        "templated": True,
                    },
                ],
                "publications": result["results"],
                "facets": facets,
            }
            self._append_pagination_links(feed["links"], build_url, result)
            return feed

        # No id -> list top subjects for navigation
        subjects = self.fts.list_subjects()
        subjects.sort(key=lambda x: x["book_count"], reverse=True)
        navigation = [
            {
                "href": f"/opds/subjects?id={s['id']}",
                "title": f"{s['name']} ({s['book_count']})",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/acquisition",
            }
            for s in subjects[:100]
        ]
        return {
            "metadata": {"title": "Subjects", "numberOfItems": len(subjects)},
            "links": [
                {
                    "rel": "self",
                    "href": "/opds/subjects",
                    "type": "application/opds+json",
                },
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/", "type": "application/opds+json"},
            ],
            "navigation": navigation,
        }

    def _build_subject_facets(
        self, subject_id, query, limit, lang, copyrighted, audiobook, sort, sort_order
    ):
        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "id": subject_id,
                "query": q,
                "page": 1,
                "limit": limit,
                "lang": lng,
                "copyrighted": cr,
                "audiobook": ab,
                "sort": srt,
                "sort_order": srt_ord,
            }
            return _url_with_params("/opds/subjects", params)

        return [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "downloads", "desc"),
                        "Most Popular",
                        sort == "downloads" or not sort,
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "relevance", ""),
                        "Relevance",
                        sort == "relevance",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "title", "asc"),
                        "Title (A-Z)",
                        sort == "title",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "author", "asc"),
                        "Author (A-Z)",
                        sort == "author",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, audiobook, "random", ""),
                        "Random",
                        sort == "random",
                    ),
                ],
            },
            {
                "metadata": {"title": "Copyright Status"},
                "links": [
                    _facet_link(
                        url(query, lang, "", audiobook, sort, sort_order),
                        "Any",
                        not copyrighted,
                    ),
                    _facet_link(
                        url(query, lang, "false", audiobook, sort, sort_order),
                        "Public Domain",
                        copyrighted == "false",
                    ),
                    _facet_link(
                        url(query, lang, "true", audiobook, sort, sort_order),
                        "Copyrighted",
                        copyrighted == "true",
                    ),
                ],
            },
            {
                "metadata": {"title": "Format"},
                "links": [
                    _facet_link(
                        url(query, lang, copyrighted, "", sort, sort_order),
                        "Any",
                        not audiobook,
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, "false", sort, sort_order),
                        "Text",
                        audiobook == "false",
                    ),
                    _facet_link(
                        url(query, lang, copyrighted, "true", sort, sort_order),
                        "Audiobook",
                        audiobook == "true",
                    ),
                ],
            },
            {
                "metadata": {"title": "Language"},
                "links": [
                    _facet_link(
                        url(query, "", copyrighted, audiobook, sort, sort_order),
                        "Any",
                        not lang,
                    )
                ]
                + [
                    _facet_link(
                        url(
                            query,
                            item["code"],
                            copyrighted,
                            audiobook,
                            sort,
                            sort_order,
                        ),
                        item["label"],
                        lang == item["code"],
                    )
                    for item in LANGUAGE_LIST
                ],
            },
        ]

    # ---------- Search ----------
    @cherrypy.expose
    @cherrypy.tools.json_out()  # type: ignore[attr-defined]
    def search(
        self,
        query: str = "",
        page: int = 1,
        limit: int = 28,
        field: str = "keyword",
        lang: str = "",
        copyrighted: str = "",
        audiobook: str = "",
        sort: str = "",
        sort_order: str = "",
        locc: str = "",
    ):
        """
        Full-text search with facets. Keeps behavior consistent with existing endpoints.
        """
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28

        search_field, search_type = _parse_field(field)
        try:
            q = self.fts.query(crosswalk=Crosswalk.OPDS)

            if query.strip():
                q.search(query, field=search_field, search_type=search_type)

            if sort in _VALID_SORTS:
                direction = (
                    SortDirection.ASC
                    if sort_order == "asc"
                    else SortDirection.DESC
                    if sort_order == "desc"
                    else None
                )
                q.order_by(OrderBy(sort), direction)
            elif query.strip():
                q.order_by(OrderBy.RELEVANCE)
            else:
                q.order_by(OrderBy.DOWNLOADS)

            if lang:
                q.lang(lang)
            if copyrighted == "true":
                q.copyrighted()
            elif copyrighted == "false":
                q.public_domain()
            if audiobook == "true":
                q.audiobook()
            elif audiobook == "false":
                q.text_only()
            if locc:
                q.locc(locc)

            q[page, limit]
            result = self.fts.execute(q)

            # dynamic top subjects for facets if filters/search applied
            top_subjects = None
            if query.strip() or locc or lang:
                q_sub = self.fts.query()
                if query.strip():
                    q_sub.search(query, field=search_field, search_type=search_type)
                if lang:
                    q_sub.lang(lang)
                if copyrighted == "true":
                    q_sub.copyrighted()
                elif copyrighted == "false":
                    q_sub.public_domain()
                if audiobook == "true":
                    q_sub.audiobook()
                elif audiobook == "false":
                    q_sub.text_only()
                if locc:
                    q_sub.locc(locc)
                top_subjects = self.fts.get_top_subjects_for_query(
                    q_sub, limit=15, max_books=500
                )
        except Exception as e:
            cherrypy.log(f"Search error: {e}")
            raise cherrypy.HTTPError(500, "Search failed")

        def url(p: int) -> str:
            return self._build_url(
                query,
                p,
                limit,
                field,
                lang,
                copyrighted,
                audiobook,
                sort,
                sort_order,
                locc,
            )

        self_href = url(result["page"])

        feed = {
            "metadata": {
                "title": "Gutenberg Search Results",
                "numberOfItems": result["total"],
                "itemsPerPage": result["page_size"],
                "currentPage": result["page"],
            },
            "links": [
                {"rel": "self", "href": self_href, "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/", "type": "application/opds+json"},
                {
                    "rel": "search",
                    "href": "/opds/search{?query,field,lang,sort,copyrighted,audiobook,locc}",
                    "type": "application/opds+json",
                    "templated": True,
                },
            ],
            "publications": result["results"],
            "facets": self._build_facets(
                query,
                limit,
                field,
                lang,
                copyrighted,
                audiobook,
                sort,
                sort_order,
                locc,
                top_subjects,
            ),
        }

        self._append_pagination_links(feed["links"], url, result)
        return feed

    # ---------- URL / facet helpers for search ----------
    def _build_url(
        self,
        query,
        page,
        limit,
        field,
        lang,
        copyrighted,
        audiobook,
        sort,
        sort_order,
        locc,
        **overrides,
    ):
        params = {
            "query": query,
            "page": page,
            "limit": limit,
            "field": field,
            "lang": lang,
            "copyrighted": copyrighted,
            "audiobook": audiobook,
            "sort": sort,
            "sort_order": sort_order,
            "locc": locc,
        }
        params.update(overrides)
        return _url_with_params("/opds/search", params)

    def _build_facets(
        self,
        query,
        limit,
        field,
        lang,
        copyrighted,
        audiobook,
        sort,
        sort_order,
        locc,
        top_subjects=None,
    ):
        url = self._build_url
        facets = [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            "downloads",
                            "desc",
                            locc,
                        ),
                        "Most Popular",
                        sort == "downloads" or not sort,
                    ),
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            "relevance",
                            "",
                            locc,
                        ),
                        "Relevance",
                        sort == "relevance",
                    ),
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            "title",
                            "asc",
                            locc,
                        ),
                        "Title (A-Z)",
                        sort == "title",
                    ),
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            "author",
                            "asc",
                            locc,
                        ),
                        "Author (A-Z)",
                        sort == "author",
                    ),
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            "random",
                            "",
                            locc,
                        ),
                        "Random",
                        sort == "random",
                    ),
                ],
            },
        ]

        if top_subjects:
            facets.append(
                {
                    "metadata": {"title": "Top Subjects in Results"},
                    "links": [
                        {
                            "href": f"/opds/subjects?id={s['id']}",
                            "type": "application/opds+json",
                            "title": f"{s['name']} ({s['count']})",
                        }
                        for s in top_subjects
                    ],
                }
            )

        facets.append(
            {
                "metadata": {"title": "Broad Genre"},
                "links": [
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            sort,
                            sort_order,
                            "",
                        ),
                        "Any",
                        not locc,
                    )
                ]
                + [
                    _facet_link(
                        url(
                            query,
                            1,
                            limit,
                            field,
                            lang,
                            copyrighted,
                            audiobook,
                            sort,
                            sort_order,
                            item.code,
                        ),
                        item.label,
                        locc == item.code,
                    )
                    for item in LoCCMainClass
                ],
            }
        )

        facets.extend(
            [
                {
                    "metadata": {"title": "Copyright Status"},
                    "links": [
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                lang,
                                "",
                                audiobook,
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Any",
                            not copyrighted,
                        ),
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                lang,
                                "false",
                                audiobook,
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Public Domain",
                            copyrighted == "false",
                        ),
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                lang,
                                "true",
                                audiobook,
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Copyrighted",
                            copyrighted == "true",
                        ),
                    ],
                },
                {
                    "metadata": {"title": "Format"},
                    "links": [
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                lang,
                                copyrighted,
                                "",
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Any",
                            not audiobook,
                        ),
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                lang,
                                copyrighted,
                                "false",
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Text",
                            audiobook == "false",
                        ),
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                lang,
                                copyrighted,
                                "true",
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Audiobook",
                            audiobook == "true",
                        ),
                    ],
                },
                {
                    "metadata": {"title": "Language"},
                    "links": [
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                "",
                                copyrighted,
                                audiobook,
                                sort,
                                sort_order,
                                locc,
                            ),
                            "Any",
                            not lang,
                        )
                    ]
                    + [
                        _facet_link(
                            url(
                                query,
                                1,
                                limit,
                                field,
                                item["code"],
                                copyrighted,
                                audiobook,
                                sort,
                                sort_order,
                                locc,
                            ),
                            item["label"],
                            lang == item["code"],
                        )
                        for item in LANGUAGE_LIST
                    ],
                },
            ]
        )
        return facets


if __name__ == "__main__":
    cherrypy.config.update(
        {"server.socket_host": "127.0.0.1", "server.socket_port": 8080}
    )
    api = API()
    cherrypy.tree.mount(api, "/opds", {"/": {}})
    try:
        cherrypy.engine.start()
        cherrypy.engine.block()
    except KeyboardInterrupt:
        cherrypy.engine.exit()
