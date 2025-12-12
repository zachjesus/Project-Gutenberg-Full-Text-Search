"""OPDS 2.0 API for Gutenberg catalog."""
import cherrypy
from FullTextSearch import FullTextSearch, SearchField, SearchType, OrderBy, SortDirection, Crosswalk


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

FIELD_MAP = {
    "keyword": (SearchField.BOOK, SearchType.FTS),
    "title": (SearchField.TITLE, SearchType.FTS),
    "author": (SearchField.AUTHOR, SearchType.FTS),
    "fuzzy_keyword": (SearchField.BOOK, SearchType.FUZZY),
    "fuzzy_title": (SearchField.TITLE, SearchType.FUZZY),
    "fuzzy_author": (SearchField.AUTHOR, SearchType.FUZZY),
}

SORT_MAP = {
    "downloads": OrderBy.DOWNLOADS,
    "title": OrderBy.TITLE,
    "author": OrderBy.AUTHOR,
    "release_date": OrderBy.RELEASE_DATE,
    "random": OrderBy.RANDOM,
}


def _facet_link(href: str, title: str, is_active: bool) -> dict:
    """Build a facet link. Only includes 'rel' if active (per OPDS 2.0 spec)."""
    link = {"href": href, "type": "application/opds+json", "title": title}
    if is_active:
        link["rel"] = "self"
    return link


class API:
    def __init__(self):
        self.fts = FullTextSearch()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        raise cherrypy.HTTPRedirect("/opds/search")

    def _build_url(self, query, page, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc, **overrides):
        """Build filter URL with optional parameter overrides."""
        params = {
            "query": query, "page": page, "limit": limit, "field": field,
            "lang": lang, "copyrighted": copyrighted, "audiobook": audiobook,
            "sort": sort, "sort_order": sort_order, "locc": locc,
        }
        params.update(overrides)
        param_str = "&".join(f"{k}={v}" for k, v in params.items() if v not in ("", None))
        return f"/opds/search?{param_str}" if param_str else "/opds/search"

    def _build_facets(self, query, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc):
        """Build facet navigation links per OPDS 2.0 spec."""
        url = self._build_url
        
        return [
            {
                "metadata": {"title": "Search Field"},
                "links": [
                    _facet_link(url(query, 1, limit, "keyword", lang, copyrighted, audiobook, sort, sort_order, locc), "All Fields", field == "keyword"),
                    _facet_link(url(query, 1, limit, "title", lang, copyrighted, audiobook, sort, sort_order, locc), "Title", field == "title"),
                    _facet_link(url(query, 1, limit, "author", lang, copyrighted, audiobook, sort, sort_order, locc), "Author", field == "author"),
                    _facet_link(url(query, 1, limit, "fuzzy_keyword", lang, copyrighted, audiobook, sort, sort_order, locc), "All Fields (Fuzzy)", field == "fuzzy_keyword"),
                    _facet_link(url(query, 1, limit, "fuzzy_title", lang, copyrighted, audiobook, sort, sort_order, locc), "Title (Fuzzy)", field == "fuzzy_title"),
                    _facet_link(url(query, 1, limit, "fuzzy_author", lang, copyrighted, audiobook, sort, sort_order, locc), "Author (Fuzzy)", field == "fuzzy_author"),
                ]
            },
            {
                "metadata": {"title": "Sort By"},
                "links": [
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "", "", locc), "Relevance", not sort),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "downloads", "desc", locc), "Downloads (High to Low)", sort == "downloads" and sort_order != "asc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "downloads", "asc", locc), "Downloads (Low to High)", sort == "downloads" and sort_order == "asc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "title", "asc", locc), "Title (A-Z)", sort == "title" and sort_order != "desc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "title", "desc", locc), "Title (Z-A)", sort == "title" and sort_order == "desc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "author", "asc", locc), "Author (A-Z)", sort == "author" and sort_order != "desc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "author", "desc", locc), "Author (Z-A)", sort == "author" and sort_order == "desc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "release_date", "desc", locc), "Release Date (Newest)", sort == "release_date" and sort_order != "asc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "release_date", "asc", locc), "Release Date (Oldest)", sort == "release_date" and sort_order == "asc"),
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, "random", "", locc), "Random", sort == "random"),
                ]
            },
            {
                "metadata": {"title": "Subject"},
                "links": [_facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, ""), "Any", not locc)] + [
                    _facet_link(url(query, 1, limit, field, lang, copyrighted, audiobook, sort, sort_order, item['code']), item['label'], locc == item['code'])
                    for item in LOCC_LIST
                ]
            },
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
        ]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def search(self, query="", page=1, limit=20, field="keyword", lang="", copyrighted="", audiobook="", sort="", sort_order="", locc=""):
        try:
            page = max(1, int(page))
            limit = max(1, min(100, int(limit)))
        except (ValueError, TypeError):
            page, limit = 1, 20

        search_field, search_type = FIELD_MAP.get(field, (SearchField.BOOK, SearchType.FTS))
        
        try:
            q = self.fts.query(crosswalk=Crosswalk.OPDS)
            
            if query.strip():
                q.search(query, field=search_field, type=search_type)
            
            if sort in SORT_MAP:
                direction = SortDirection.ASC if sort_order == "asc" else SortDirection.DESC if sort_order == "desc" else None
                q.order_by(SORT_MAP[sort], direction)
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
                {"rel": "search", "href": "/opds/search{?query}", "type": "application/opds+json", "templated": True}
            ],
            "publications": result["results"],
            "facets": self._build_facets(query, limit, field, lang, copyrighted, audiobook, sort, sort_order, locc)
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
