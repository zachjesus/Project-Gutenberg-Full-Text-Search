"""OPDS 2.0 API for Gutenberg catalog."""
import cherrypy
from urllib.parse import quote, unquote, urlencode
from FullTextSearch import (
    FullTextSearch, SearchField, SearchType, OrderBy, SortDirection, Crosswalk,
    LANGUAGE_LIST, LOCC_TOP, LOCC_HIERARCHY, CURATED_BOOKSHELVES,
    get_locc_children, get_locc_path, get_broad_genres
)

def _parse_field(field: str) -> tuple[SearchField, SearchType]:
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

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        """Root catalog - groups with featured publications and navigation links."""
        navigation = [
            {
                "href": "/opds/search?field=fuzzy_keyword",
                "title": "Search Fuzzy (Typo-Tolerant, Slower)",
                "type": "application/opds+json",
                "rel": "subsection"
            },
            {
                "href": "/opds/search?field=fts_keyword",
                "title": "Search FTS (Strict, Faster, Operators: \"quotes for exact\", or, and, - for negate)",
                "type": "application/opds+json",
                "rel": "subsection"
            },
            {
                "href": "/opds/loccs",
                "title": "Browse by LoCC (Subject Classification)",
                "type": "application/opds+json",
                "rel": "subsection"
            },
            {
                "href": "/opds/search?sort=downloads&sort_order=desc",
                "title": "Most Popular",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/sort/popular"
            },
            {
                "href": "/opds/search?sort=release_date&sort_order=desc",
                "title": "Recently Added",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/sort/new"
            },
            {
                "href": "/opds/search?sort=random",
                "title": "Random",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/sort/random"
            },
        ]
        
        # Build groups for ALL curated bookshelves with sample publications
        # Uses MN table batch query for speed (single query for all bookshelves)
        SAMPLE_LIMIT = 20
        
        # Collect ALL bookshelf IDs from all categories (no cap)
        shelves_to_show = []
        for category, shelves in CURATED_BOOKSHELVES.items():
            shelves_to_show.extend(shelves)
        
        # Batch fetch all samples in one query (uses MN table - very fast!)
        try:
            bookshelf_ids = [s['id'] for s in shelves_to_show]
            batch_results = self.fts.get_bookshelf_samples_batch(bookshelf_ids, sample_limit=SAMPLE_LIMIT, crosswalk=Crosswalk.OPDS)
        except Exception as e:
            cherrypy.log(f"Error fetching bookshelf samples: {e}")
            batch_results = {}
        
        # Build groups from batch results
        groups = []
        for shelf in shelves_to_show:
            shelf_data = batch_results.get(shelf['id'], {"results": [], "total": 0})
            if shelf_data["results"]:  # Only show shelves with books
                groups.append({
                    "metadata": {
                        "title": shelf['name'],
                        "numberOfItems": shelf_data["total"]
                    },
                    "links": [
                        {"href": f"/opds/bookshelves?id={shelf['id']}", "rel": "self", "type": "application/opds+json"}
                    ],
                    "publications": shelf_data["results"]
                })
        
        return {
            "metadata": {
                "title": "Project Gutenberg Catalog"
            },
            "links": [
                {"rel": "self", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "search", "href": "/opds/search{?query,field,lang,sort,copyrighted,audiobook,locc}", "type": "application/opds+json", "templated": True}
            ],
            "navigation": navigation,
            "groups": groups
        }

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def genres(self, code="", page=1, limit=28, query="", lang="", copyrighted="", audiobook="", sort="", sort_order=""):
        """
        Broad Genre navigation and browsing.
        
        Without code: shows broad genres (Literature, Science, History, etc.)
        With code: browse books in that genre with full search/filtering support
        """
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28
        
        code = (code or "").strip().upper()
        
        # If no code, show the genre listing
        if not code:
            # Get broad genres from base tables
            with self.fts.Session() as session:
                broad_genres = get_broad_genres(session=session)
            
            # Build navigation for broad genres
            navigation = [
                {
                    "href": f"/opds/genres?code={g['code']}",
                    "title": f"{g['label']} ({g['book_count']})",
                    "type": "application/opds+json",
                    "rel": "subsection"
                }
                for g in broad_genres
            ]
            
            return {
                "metadata": {
                    "title": "Broad Genres",
                    "numberOfItems": len(broad_genres)
                },
                "links": [
                    {"rel": "self", "href": "/opds/genres", "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {"rel": "up", "href": "/opds/", "type": "application/opds+json"}
                ],
                "navigation": navigation
            }
        
        # Browse books within a specific broad genre
        genre_label = LOCC_HIERARCHY.get(code, code)
        
        # Build query with filtering
        try:
            q = self.fts.query(crosswalk=Crosswalk.OPDS)
            
            # Filter by broad genre (LoCC prefix)
            q.locc(code)
            
            # Apply optional search
            if query.strip():
                search_field, search_type = _parse_field("keyword")
                q.search(query, field=search_field, search_type=search_type)
            
            # Apply filters
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
            
            # Apply sorting
            if sort in OrderBy._value2member_map_:
                direction = SortDirection.ASC if sort_order == "asc" else SortDirection.DESC if sort_order == "desc" else None
                q.order_by(OrderBy(sort), direction)
            elif query.strip():
                q.order_by(OrderBy.RELEVANCE)
            else:
                q.order_by(OrderBy.DOWNLOADS)
            
            q[page, limit]
            result = self.fts.execute(q)
            
        except Exception as e:
            cherrypy.log(f"Genre browse error: {e}")
            raise cherrypy.HTTPError(500, "Browse failed")
        
        # Build URL helper for this endpoint
        def build_url(q_param, pg, lim, lng, cr, ab, srt, srt_ord):
            params = {
                "code": code, "query": q_param, "page": pg, "limit": lim,
                "lang": lng, "copyrighted": cr, "audiobook": ab,
                "sort": srt, "sort_order": srt_ord
            }
            return _url_with_params("/opds/genres", params)
        
        self_href = build_url(query, page, limit, lang, copyrighted, audiobook, sort, sort_order)
        
        # Build facets for filtering within this genre
        facets = self._build_genre_facets(code, query, limit, lang, copyrighted, audiobook, sort, sort_order)
        
        feed = {
            "metadata": {
                "title": f"Browse: {genre_label}",
                "numberOfItems": result["total"],
                "itemsPerPage": result["page_size"],
                "currentPage": result["page"]
            },
            "links": [
                {"rel": "self", "href": self_href, "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/genres", "type": "application/opds+json"},
                {"rel": "search", "href": f"/opds/genres?code={code}{{&query,lang,sort,copyrighted,audiobook}}", "type": "application/opds+json", "templated": True}
            ],
            "publications": result["results"],
            "facets": facets
        }
        
        # Add pagination links
        if result["page"] > 1:
            feed["links"].append({"rel": "first", "href": build_url(query, 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            feed["links"].append({"rel": "previous", "href": build_url(query, result['page'] - 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
        if result["page"] < result["total_pages"]:
            feed["links"].append({"rel": "next", "href": build_url(query, result['page'] + 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            feed["links"].append({"rel": "last", "href": build_url(query, result['total_pages'], limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
        
        return feed
    
    def _build_genre_facets(self, code, query, limit, lang, copyrighted, audiobook, sort, sort_order):
        """Build facets for genre browse (excludes broad genre selector)."""
        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "code": code, "query": q, "page": 1, "limit": limit,
                "lang": lng, "copyrighted": cr, "audiobook": ab,
                "sort": srt, "sort_order": srt_ord
            }
            return _url_with_params("/opds/genres", params)

        return [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, audiobook, "downloads", "desc"), "Most Popular", sort == "downloads" or not sort),
                    _facet_link(url(query, lang, copyrighted, audiobook, "relevance", ""), "Relevance", sort == "relevance"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "title", "asc"), "Title (A-Z)", sort == "title"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "author", "asc"), "Author (A-Z)", sort == "author"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "random", ""), "Random", sort == "random"),
                ]
            },
            {
                "metadata": {"title": "Copyright Status"},
                "links": [
                    _facet_link(url(query, lang, "", audiobook, sort, sort_order), "Any", not copyrighted),
                    _facet_link(url(query, lang, "false", audiobook, sort, sort_order), "Public Domain", copyrighted == "false"),
                    _facet_link(url(query, lang, "true", audiobook, sort, sort_order), "Copyrighted", copyrighted == "true"),
                ]
            },
            {
                "metadata": {"title": "Format"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, "", sort, sort_order), "Any", not audiobook),
                    _facet_link(url(query, lang, copyrighted, "false", sort, sort_order), "Text", audiobook == "false"),
                    _facet_link(url(query, lang, copyrighted, "true", sort, sort_order), "Audiobook", audiobook == "true"),
                ]
            },
            {
                "metadata": {"title": "Language"},
                "links": [_facet_link(url(query, "", copyrighted, audiobook, sort, sort_order), "Any", not lang)] + [
                    _facet_link(url(query, item['code'], copyrighted, audiobook, sort, sort_order), item['label'], lang == item['code'])
                    for item in LANGUAGE_LIST
                ]
            }
        ]
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def loccs(self, parent="", page=1, limit=28, query="", lang="", copyrighted="", audiobook="", sort="", sort_order=""):
        """
        Subject Classification (LoCC) hierarchical navigation with full filtering.
        
        Without parent: shows top-level classes (A, B, C...)
        With parent: shows children of that class, or books if leaf node
        At any level with books: supports search and filtering via facets
        """
        parent = (parent or "").strip().upper()
        
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28
        
        children = get_locc_children(parent)
        
        # Build breadcrumb path
        path = get_locc_path(parent) if parent else []
        
        # If no children, this is a leaf - return books with facets
        if not children and parent:
            try:
                q = self.fts.query(crosswalk=Crosswalk.OPDS)
                q.locc(parent)
                
                # Apply optional search
                if query.strip():
                    search_field, search_type = _parse_field("keyword")
                    q.search(query, field=search_field, search_type=search_type)
                
                # Apply filters
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
                
                # Apply sorting
                if sort in OrderBy._value2member_map_:
                    direction = SortDirection.ASC if sort_order == "asc" else SortDirection.DESC if sort_order == "desc" else None
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
            
            # Build URL helper
            def build_url(q_param, pg, lim, lng, cr, ab, srt, srt_ord):
                params = {
                    "parent": parent, "query": q_param, "page": pg, "limit": lim,
                    "lang": lng, "copyrighted": cr, "audiobook": ab,
                    "sort": srt, "sort_order": srt_ord
                }
                return _url_with_params("/opds/loccs", params)
            
            self_href = build_url(query, page, limit, lang, copyrighted, audiobook, sort, sort_order)
            up_parent = path[-2]['code'] if len(path) > 1 else ""
            
            # Get top subjects for this LoCC (for dynamic facets)
            top_subjects = None
            try:
                q_for_subjects = self.fts.query()
                q_for_subjects.locc(parent)
                if query.strip():
                    search_field, search_type = _parse_field("keyword")
                    q_for_subjects.search(query, field=search_field, search_type=search_type)
                if lang:
                    q_for_subjects.lang(lang)
                if copyrighted == "true":
                    q_for_subjects.copyrighted()
                elif copyrighted == "false":
                    q_for_subjects.public_domain()
                if audiobook == "true":
                    q_for_subjects.audiobook()
                elif audiobook == "false":
                    q_for_subjects.text_only()
                top_subjects = self.fts.get_top_subjects_for_query(q_for_subjects, limit=15, max_books=500)
            except Exception as e:
                cherrypy.log(f"Top subjects error: {e}")
            
            # Build facets for filtering
            facets = self._build_locc_facets(parent, query, limit, lang, copyrighted, audiobook, sort, sort_order, top_subjects)
            
            feed = {
                "metadata": {
                    "title": LOCC_HIERARCHY.get(parent, parent),
                    "subtitle": " > ".join(p['label'] for p in path) if path else None,
                    "numberOfItems": result["total"],
                    "itemsPerPage": result["page_size"],
                    "currentPage": result["page"]
                },
                "links": [
                    {"rel": "self", "href": self_href, "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {"rel": "up", "href": f"/opds/loccs?parent={up_parent}" if up_parent else "/opds/loccs", "type": "application/opds+json"},
                    {"rel": "search", "href": f"/opds/loccs?parent={parent}{{&query,lang,sort,copyrighted,audiobook}}", "type": "application/opds+json", "templated": True}
                ],
                "publications": result["results"],
                "facets": facets
            }
            
            # Add pagination links
            if result["page"] > 1:
                feed["links"].append({"rel": "first", "href": build_url(query, 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
                feed["links"].append({"rel": "previous", "href": build_url(query, result['page'] - 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            if result["page"] < result["total_pages"]:
                feed["links"].append({"rel": "next", "href": build_url(query, result['page'] + 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
                feed["links"].append({"rel": "last", "href": build_url(query, result['total_pages'], limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            
            return feed
        
        # Build navigation feed for children (no facets at navigation level)
        navigation = []
        for child in children:
            nav_item = {
                "href": f"/opds/loccs?parent={child['code']}",
                "title": child['label'],
                "type": "application/opds+json",
                "rel": "subsection"  # All LoCC items use subsection (they link to feeds, not files)
            }
            navigation.append(nav_item)
        
        links = [
            {"rel": "self", "href": f"/opds/loccs?parent={parent}" if parent else "/opds/loccs", "type": "application/opds+json"},
            {"rel": "start", "href": "/opds/", "type": "application/opds+json"}
        ]
        
        if parent and path:
            # Add "up" link to parent
            up_parent = path[-2]['code'] if len(path) > 1 else ""
            links.append({"rel": "up", "href": f"/opds/loccs?parent={up_parent}" if up_parent else "/opds/loccs", "type": "application/opds+json"})
        elif parent:
            links.append({"rel": "up", "href": "/opds/loccs", "type": "application/opds+json"})
        else:
            links.append({"rel": "up", "href": "/opds/", "type": "application/opds+json"})
        
        return {
            "metadata": {
                "title": LOCC_HIERARCHY.get(parent, "Subject Classification") if parent else "Subject Classification",
                "subtitle": " > ".join(p['label'] for p in path) if path else None
            },
            "links": links,
            "navigation": navigation
        }
    
    def _build_locc_facets(self, parent, query, limit, lang, copyrighted, audiobook, sort, sort_order, top_subjects=None):
        """Build facets for LoCC leaf node (excludes LoCC selector since we're already deep in hierarchy)."""
        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "parent": parent, "query": q, "page": 1, "limit": limit,
                "lang": lng, "copyrighted": cr, "audiobook": ab,
                "sort": srt, "sort_order": srt_ord
            }
            return _url_with_params("/opds/loccs", params)
        
        facets = [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, audiobook, "downloads", "desc"), "Most Popular", sort == "downloads" or not sort),
                    _facet_link(url(query, lang, copyrighted, audiobook, "relevance", ""), "Relevance", sort == "relevance"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "title", "asc"), "Title (A-Z)", sort == "title"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "author", "asc"), "Author (A-Z)", sort == "author"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "random", ""), "Random", sort == "random"),
                ]
            },
        ]
        
        # Add top subjects if available
        if top_subjects:
            facets.append({
                "metadata": {"title": "Top Subjects in Results"},
                "links": [
                    {
                        "href": f"/opds/subjects?id={s['id']}",
                        "type": "application/opds+json",
                        "title": f"{s['name']} ({s['count']})"
                    }
                    for s in top_subjects
                ]
            })
        
        facets.extend([
            {
                "metadata": {"title": "Copyright Status"},
                "links": [
                    _facet_link(url(query, lang, "", audiobook, sort, sort_order), "Any", not copyrighted),
                    _facet_link(url(query, lang, "false", audiobook, sort, sort_order), "Public Domain", copyrighted == "false"),
                    _facet_link(url(query, lang, "true", audiobook, sort, sort_order), "Copyrighted", copyrighted == "true"),
                ]
            },
            {
                "metadata": {"title": "Format"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, "", sort, sort_order), "Any", not audiobook),
                    _facet_link(url(query, lang, copyrighted, "false", sort, sort_order), "Text", audiobook == "false"),
                    _facet_link(url(query, lang, copyrighted, "true", sort, sort_order), "Audiobook", audiobook == "true"),
                ]
            },
            {
                "metadata": {"title": "Language"},
                "links": [_facet_link(url(query, "", copyrighted, audiobook, sort, sort_order), "Any", not lang)] + [
                    _facet_link(url(query, item['code'], copyrighted, audiobook, sort, sort_order), item['label'], lang == item['code'])
                    for item in LANGUAGE_LIST
                ]
            }
        ])
        
        return facets

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def bookshelves(self, id=None, category=None, page=1, limit=28, query="", lang="", copyrighted="", audiobook="", sort="", sort_order=""):
        """
        Bookshelf navigation using curated bookshelves with search/filtering support.
        
        Without id or category: lists bookshelf categories
        With category: lists bookshelves in that category with sample publications
        With id: returns books in that bookshelf with full search/filtering
        """
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28
        
        if id is not None:
            # Return books in this bookshelf with search/filtering support
            try:
                bookshelf_id = int(id)
            except (ValueError, TypeError):
                raise cherrypy.HTTPError(400, "Invalid bookshelf ID")
            
            # Find bookshelf name from curated list
            bookshelf_name = f"Bookshelf {bookshelf_id}"
            parent_category = None
            for cat, shelves in CURATED_BOOKSHELVES.items():
                for shelf in shelves:
                    if shelf['id'] == bookshelf_id:
                        bookshelf_name = shelf['name']
                        parent_category = cat
                        break
                if parent_category:
                    break
            
            # Build query with filtering
            try:
                q = self.fts.query(crosswalk=Crosswalk.OPDS)
                q.bookshelf_id(bookshelf_id)
                
                # Apply optional search
                if query.strip():
                    search_field, search_type = _parse_field("keyword")
                    q.search(query, field=search_field, search_type=search_type)
                
                # Apply filters
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
                
                # Apply sorting
                if sort in _VALID_SORTS:
                    direction = SortDirection.ASC if sort_order == "asc" else SortDirection.DESC if sort_order == "desc" else None
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
            
            up_href = f"/opds/bookshelves?category={quote(parent_category)}" if parent_category else "/opds/bookshelves"
            
            # Build URL helper
            def build_url(q_param, pg, lim, lng, cr, ab, srt, srt_ord):
                params = {
                    "id": bookshelf_id, "query": q_param, "page": pg, "limit": lim,
                    "lang": lng, "copyrighted": cr, "audiobook": ab,
                    "sort": srt, "sort_order": srt_ord
                }
                return _url_with_params("/opds/bookshelves", params)
            
            self_href = build_url(query, page, limit, lang, copyrighted, audiobook, sort, sort_order)
            
            # Get top subjects for this bookshelf (for dynamic facets)
            top_subjects = None
            try:
                q_for_subjects = self.fts.query()
                q_for_subjects.bookshelf_id(bookshelf_id)
                if query.strip():
                    search_field, search_type = _parse_field("keyword")
                    q_for_subjects.search(query, field=search_field, search_type=search_type)
                if lang:
                    q_for_subjects.lang(lang)
                if copyrighted == "true":
                    q_for_subjects.copyrighted()
                elif copyrighted == "false":
                    q_for_subjects.public_domain()
                if audiobook == "true":
                    q_for_subjects.audiobook()
                elif audiobook == "false":
                    q_for_subjects.text_only()
                top_subjects = self.fts.get_top_subjects_for_query(q_for_subjects, limit=15, max_books=500)
            except Exception as e:
                cherrypy.log(f"Top subjects error: {e}")
            
            # Build facets for filtering
            facets = self._build_bookshelf_facets(bookshelf_id, query, limit, lang, copyrighted, audiobook, sort, sort_order, top_subjects)
            
            feed = {
                "metadata": {
                    "title": bookshelf_name,
                    "numberOfItems": result["total"],
                    "itemsPerPage": result["page_size"],
                    "currentPage": result["page"]
                },
                "links": [
                    {"rel": "self", "href": self_href, "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {"rel": "up", "href": up_href, "type": "application/opds+json"},
                    {"rel": "search", "href": f"/opds/bookshelves?id={bookshelf_id}{{&query,lang,sort,copyrighted,audiobook}}", "type": "application/opds+json", "templated": True}
                ],
                "publications": result["results"],
                "facets": facets
            }
            
            # Add pagination links
            if result["page"] > 1:
                feed["links"].append({"rel": "first", "href": build_url(query, 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
                feed["links"].append({"rel": "previous", "href": build_url(query, result['page'] - 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            if result["page"] < result["total_pages"]:
                feed["links"].append({"rel": "next", "href": build_url(query, result['page'] + 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
                feed["links"].append({"rel": "last", "href": build_url(query, result['total_pages'], limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            
            return feed
        
        if category is not None:
            # List bookshelves in this category (navigation format - no sample publications to avoid N+1)
            # Decode URL-encoded category name
            category = unquote(category)
            if category not in CURATED_BOOKSHELVES:
                raise cherrypy.HTTPError(404, "Category not found")
            
            shelves = CURATED_BOOKSHELVES[category]
            
            # Build navigation links for each shelf
            navigation = [
                {
                    "href": f"/opds/bookshelves?id={shelf['id']}",
                    "title": shelf["name"],
                    "type": "application/opds+json",
                    "rel": "http://opds-spec.org/acquisition"
                }
                for shelf in shelves
            ]
            
            return {
                "metadata": {
                    "title": category,
                    "numberOfItems": len(shelves)
                },
                "links": [
                    {"rel": "self", "href": f"/opds/bookshelves?category={quote(category)}", "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {"rel": "up", "href": "/opds/bookshelves", "type": "application/opds+json"}
                ],
                "navigation": navigation
            }
        
        # List all categories (navigation format - no DB queries needed)
        navigation = [
            {
                "href": f"/opds/bookshelves?category={quote(cat)}",
                "title": f"{cat} ({len(shelves)} shelves)",
                "type": "application/opds+json",
                "rel": "subsection"
            }
            for cat, shelves in CURATED_BOOKSHELVES.items()
        ]
        
        return {
            "metadata": {
                "title": "Bookshelves",
                "numberOfItems": len(CURATED_BOOKSHELVES)
            },
            "links": [
                {"rel": "self", "href": "/opds/bookshelves", "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/", "type": "application/opds+json"}
            ],
            "navigation": navigation
        }
    
    def _build_bookshelf_facets(self, bookshelf_id, query, limit, lang, copyrighted, audiobook, sort, sort_order, top_subjects=None):
        """Build facets for bookshelf browse."""
        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "id": bookshelf_id, "query": q, "page": 1, "limit": limit,
                "lang": lng, "copyrighted": cr, "audiobook": ab,
                "sort": srt, "sort_order": srt_ord
            }
            return _url_with_params("/opds/bookshelves", params)
        
        facets = [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, audiobook, "downloads", "desc"), "Most Popular", sort == "downloads" or not sort),
                    _facet_link(url(query, lang, copyrighted, audiobook, "relevance", ""), "Relevance", sort == "relevance"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "title", "asc"), "Title (A-Z)", sort == "title"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "author", "asc"), "Author (A-Z)", sort == "author"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "random", ""), "Random", sort == "random"),
                ]
            },
        ]
        
        # Add top subjects if available
        if top_subjects:
            facets.append({
                "metadata": {"title": "Top Subjects in Results"},
                "links": [
                    {
                        "href": f"/opds/subjects?id={s['id']}",
                        "type": "application/opds+json",
                        "title": f"{s['name']} ({s['count']})"
                    }
                    for s in top_subjects
                ]
            })
        
        facets.extend([
            {
                "metadata": {"title": "Copyright Status"},
                "links": [
                    _facet_link(url(query, lang, "", audiobook, sort, sort_order), "Any", not copyrighted),
                    _facet_link(url(query, lang, "false", audiobook, sort, sort_order), "Public Domain", copyrighted == "false"),
                    _facet_link(url(query, lang, "true", audiobook, sort, sort_order), "Copyrighted", copyrighted == "true"),
                ]
            },
            {
                "metadata": {"title": "Format"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, "", sort, sort_order), "Any", not audiobook),
                    _facet_link(url(query, lang, copyrighted, "false", sort, sort_order), "Text", audiobook == "false"),
                    _facet_link(url(query, lang, copyrighted, "true", sort, sort_order), "Audiobook", audiobook == "true"),
                ]
            },
            {
                "metadata": {"title": "Language"},
                "links": [_facet_link(url(query, "", copyrighted, audiobook, sort, sort_order), "Any", not lang)] + [
                    _facet_link(url(query, item['code'], copyrighted, audiobook, sort, sort_order), item['label'], lang == item['code'])
                    for item in LANGUAGE_LIST
                ]
            }
        ])
        
        return facets

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def subjects(self, id=None, page=1, limit=28, query="", lang="", copyrighted="", audiobook="", sort="", sort_order=""):
        """
        Subject navigation with search/filtering support.
        
        Without id: lists top subjects by book count
        With id: returns books with that subject with full search/filtering
        """
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 28
        
        if id is not None:
            # Return books with this subject with search/filtering support
            try:
                subject_id = int(id)
            except (ValueError, TypeError):
                raise cherrypy.HTTPError(400, "Invalid subject ID")
            
            # Get subject name (fast single lookup instead of loading all subjects)
            subject_name = self.fts.get_subject_name(subject_id) or f"Subject {subject_id}"
            
            # Build query with filtering
            try:
                q = self.fts.query(crosswalk=Crosswalk.OPDS)
                q.subject_id(subject_id)
                
                # Apply optional search
                if query.strip():
                    search_field, search_type = _parse_field("keyword")
                    q.search(query, field=search_field, search_type=search_type)
                
                # Apply filters
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
                
                # Apply sorting
                if sort in _VALID_SORTS:
                    direction = SortDirection.ASC if sort_order == "asc" else SortDirection.DESC if sort_order == "desc" else None
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
            
            # Build URL helper
            def build_url(q_param, pg, lim, lng, cr, ab, srt, srt_ord):
                params = {
                    "id": subject_id, "query": q_param, "page": pg, "limit": lim,
                    "lang": lng, "copyrighted": cr, "audiobook": ab,
                    "sort": srt, "sort_order": srt_ord
                }
                return _url_with_params("/opds/subjects", params)
            
            self_href = build_url(query, page, limit, lang, copyrighted, audiobook, sort, sort_order)
            
            # Build facets for filtering
            facets = self._build_subject_facets(subject_id, query, limit, lang, copyrighted, audiobook, sort, sort_order)
            
            feed = {
                "metadata": {
                    "title": subject_name,
                    "numberOfItems": result["total"],
                    "itemsPerPage": result["page_size"],
                    "currentPage": result["page"]
                },
                "links": [
                    {"rel": "self", "href": self_href, "type": "application/opds+json"},
                    {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                    {"rel": "up", "href": "/opds/subjects", "type": "application/opds+json"},
                    {"rel": "search", "href": f"/opds/subjects?id={subject_id}{{&query,lang,sort,copyrighted,audiobook}}", "type": "application/opds+json", "templated": True}
                ],
                "publications": result["results"],
                "facets": facets
            }
            
            # Add pagination links
            if result["page"] > 1:
                feed["links"].append({"rel": "first", "href": build_url(query, 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
                feed["links"].append({"rel": "previous", "href": build_url(query, result['page'] - 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            if result["page"] < result["total_pages"]:
                feed["links"].append({"rel": "next", "href": build_url(query, result['page'] + 1, limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
                feed["links"].append({"rel": "last", "href": build_url(query, result['total_pages'], limit, lang, copyrighted, audiobook, sort, sort_order), "type": "application/opds+json"})
            
            return feed
        
        # List top subjects
        subjects = self.fts.list_subjects()
        # Sort by book count descending
        subjects.sort(key=lambda x: x['book_count'], reverse=True)
        
        navigation = [
            {
                "href": f"/opds/subjects?id={s['id']}",
                "title": f"{s['name']} ({s['book_count']})",
                "type": "application/opds+json",
                "rel": "http://opds-spec.org/acquisition"
            }
            for s in subjects[:100]  # Limit to top 100 for navigation
        ]
        
        return {
            "metadata": {
                "title": "Subjects",
                "numberOfItems": len(subjects)
            },
            "links": [
                {"rel": "self", "href": "/opds/subjects", "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/", "type": "application/opds+json"}
            ],
            "navigation": navigation
        }
    
    def _build_subject_facets(self, subject_id, query, limit, lang, copyrighted, audiobook, sort, sort_order):
        """Build facets for subject browse."""
        def url(q, lng, cr, ab, srt, srt_ord):
            params = {
                "id": subject_id, "query": q, "page": 1, "limit": limit,
                "lang": lng, "copyrighted": cr, "audiobook": ab,
                "sort": srt, "sort_order": srt_ord
            }
            return _url_with_params("/opds/subjects", params)

        return [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, audiobook, "downloads", "desc"), "Most Popular", sort == "downloads" or not sort),
                    _facet_link(url(query, lang, copyrighted, audiobook, "relevance", ""), "Relevance", sort == "relevance"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "title", "asc"), "Title (A-Z)", sort == "title"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "author", "asc"), "Author (A-Z)", sort == "author"),
                    _facet_link(url(query, lang, copyrighted, audiobook, "random", ""), "Random", sort == "random"),
                ]
            },
            {
                "metadata": {"title": "Copyright Status"},
                "links": [
                    _facet_link(url(query, lang, "", audiobook, sort, sort_order), "Any", not copyrighted),
                    _facet_link(url(query, lang, "false", audiobook, sort, sort_order), "Public Domain", copyrighted == "false"),
                    _facet_link(url(query, lang, "true", audiobook, sort, sort_order), "Copyrighted", copyrighted == "true"),
                ]
            },
            {
                "metadata": {"title": "Format"},
                "links": [
                    _facet_link(url(query, lang, copyrighted, "", sort, sort_order), "Any", not audiobook),
                    _facet_link(url(query, lang, copyrighted, "false", sort, sort_order), "Text", audiobook == "false"),
                    _facet_link(url(query, lang, copyrighted, "true", sort, sort_order), "Audiobook", audiobook == "true"),
                ]
            },
            {
                "metadata": {"title": "Language"},
                "links": [_facet_link(url(query, "", copyrighted, audiobook, sort, sort_order), "Any", not lang)] + [
                    _facet_link(url(query, item['code'], copyrighted, audiobook, sort, sort_order), item['label'], lang == item['code'])
                    for item in LANGUAGE_LIST
                ]
            }
        ]

    def _build_url(self, query, page, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc, **overrides):
        """Build filter URL with optional parameter overrides."""
        params = {
            "query": query, "page": page, "limit": limit, "field": field,
            "lang": lang, "copyrighted": copyrighted, "audiobook": audiobook,
            "sort": sort, "sort_order": sort_order, "locc": locc,
        }
        params.update(overrides)
        return _url_with_params("/opds/search", params)

    def _build_facets(self, query, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc, top_subjects=None):
        """Build facet navigation links per OPDS 2.0 spec."""
        url = self._build_url
        
        facets = [
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "downloads", "desc", locc), "Most Popular", sort == "downloads" or not sort),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "relevance", "", locc), "Relevance", sort == "relevance"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "title", "asc", locc), "Title (A-Z)", sort == "title"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "author", "asc", locc), "Author (A-Z)", sort == "author"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "random", "", locc), "Random", sort == "random"),
                ]
            },
        ]
        
        # Add dynamic top subjects facet ABOVE broad genre (if we have search results)
        if top_subjects:
            facets.append({
                "metadata": {"title": "Top Subjects in Results"},
                "links": [
                    {
                        "href": f"/opds/subjects?id={s['id']}",
                        "type": "application/opds+json",
                        "title": f"{s['name']} ({s['count']})"
                    }
                    for s in top_subjects
                ]
            })
        
        # Broad Genre after Top Subjects
        facets.append({
            "metadata": {"title": "Broad Genre"},
            "links": [_facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, ""), "Any", not locc)] + [
                _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, item['code']), item['label'], locc == item['code'])
                for item in LOCC_TOP
            ]
        })
        
        facets.extend([
            {
                "metadata": {"title": "Copyright Status"},
                "links": [
                    _facet_link(url(query, 1, limit, field, lang, "", audiobook, sort, sort_order, locc), "Any", not copyrighted),
                    _facet_link(url(query, 1, limit, field, lang, "false", audiobook, sort, sort_order, locc), "Public Domain", copyrighted == "false"),
                    _facet_link(url(query, 1, limit, field, lang, "true", audiobook, sort, sort_order, locc), "Copyrighted", copyrighted == "true"),
                ]
            },
            {
                "metadata": {"title": "Format"},
                "links": [
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, "", sort, sort_order, locc), "Any", not audiobook),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, "false", sort, sort_order, locc), "Text", audiobook == "false"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, "true", sort, sort_order, locc), "Audiobook", audiobook == "true"),
                ]
            },
            {
                "metadata": {"title": "Language"},
                "links": [_facet_link(url(query, 1, limit, field, "", copyrighted, audiobook, sort, sort_order, locc), "Any", not lang)] + [
                    _facet_link(url(query, 1, limit, field, item['code'], copyrighted, audiobook, sort, sort_order, locc), item['label'], lang == item['code'])
                    for item in LANGUAGE_LIST
                ]
            }
        ])
        
        return facets

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def search(self, query="", page=1, limit=28, field="keyword", lang="", copyrighted="", audiobook="", sort="", sort_order="", locc=""):
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
                direction = SortDirection.ASC if sort_order == "asc" else SortDirection.DESC if sort_order == "desc" else None
                q.order_by(OrderBy(sort), direction)
            elif query.strip():
                # Both FTS and Fuzzy use relevance ranking for searches
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
            
            # Get top subjects for this query (for dynamic facets)
            top_subjects = None
            if query.strip() or locc or lang:  # Only fetch for filtered queries
                # Build a fresh query without pagination for subject counting
                q_for_subjects = self.fts.query()
                if query.strip():
                    q_for_subjects.search(query, field=search_field, search_type=search_type)
                if lang:
                    q_for_subjects.lang(lang)
                if copyrighted == "true":
                    q_for_subjects.copyrighted()
                elif copyrighted == "false":
                    q_for_subjects.public_domain()
                if audiobook == "true":
                    q_for_subjects.audiobook()
                elif audiobook == "false":
                    q_for_subjects.text_only()
                if locc:
                    q_for_subjects.locc(locc)
                
                top_subjects = self.fts.get_top_subjects_for_query(q_for_subjects, limit=15, max_books=500)
            
        except Exception as e:
            cherrypy.log(f"Search error: {e}")
            raise cherrypy.HTTPError(500, "Search failed")

        url = self._build_url
        self_href = url(query, page, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc)
        
        feed = {
            "metadata": {
                "title": "Gutenberg Search Results",
                "numberOfItems": result["total"],
                "itemsPerPage": result["page_size"],
                "currentPage": result["page"]
            },
            "links": [
                {"rel": "self", "href": self_href, "type": "application/opds+json"},
                {"rel": "start", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "up", "href": "/opds/", "type": "application/opds+json"},
                {"rel": "search", "href": "/opds/search{?query,field,lang,sort,copyrighted,audiobook,locc}", "type": "application/opds+json", "templated": True}
            ],
            "publications": result["results"],
            "facets": self._build_facets(query, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc, top_subjects)
        }

        if result["page"] > 1:
            feed["links"].append({"rel": "first", "href": url(query, 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc), "type": "application/opds+json"})
            feed["links"].append({"rel": "previous", "href": url(query, result['page'] - 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc), "type": "application/opds+json"})
        
        if result["page"] < result["total_pages"]:
            feed["links"].append({"rel": "next", "href": url(query, result['page'] + 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc), "type": "application/opds+json"})
            feed["links"].append({"rel": "last", "href": url(query, result['total_pages'], limit, field, lang, copyrighted, audiobook, sort, sort_order, locc), "type": "application/opds+json"})

        return feed


if __name__ == "__main__":
    cherrypy.quickstart(API(), "/opds")
