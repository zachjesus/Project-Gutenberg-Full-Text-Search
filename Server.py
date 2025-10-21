import cherrypy
from FullTextSearchRewrite import FullTextSearch

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

        return results

if __name__ == "__main__":
    cherrypy.quickstart(API(), "/")