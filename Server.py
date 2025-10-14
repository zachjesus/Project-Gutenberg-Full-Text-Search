import cherrypy
from FullTextSearch import FullTextSearch

class API:
    def __init__(self):
        self.fts = FullTextSearch()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def search(self, query="", page=1, limit=10, fields=None):
        try:
            page = max(1, int(page))
            limit = max(1, int(limit))
        except Exception:
            page, limit = 1, 10

        search_fields = None
        if fields:
            search_fields = [f.strip() for f in str(fields).split(",") if f.strip()]

        results = self.fts.ranked_fulltext_search(
            query_text=query,
            limit=limit,
            search_fields=search_fields,
            page=page
        )

        def row_to_dict(r):
            return {
                "pk": getattr(r, "pk", None),
                "title": getattr(r, "title", None),
                "downloads": getattr(r, "downloads", 0),
                "release_date": str(getattr(r, "release_date", "")),
                "text_rank": getattr(r, "text_rank", 0.0),
                "total_rank": getattr(r, "total_rank", 0.0),
                "match_types": getattr(r, "match_types", []),
            }

        return {
            "query": query,
            "page": page,
            "limit": limit,
            "fields": search_fields or [],
            "results": [row_to_dict(r) for r in results]
        }

if __name__ == "__main__":
    cherrypy.quickstart(API(), "/")