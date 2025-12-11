import cherrypy
from FullTextSearch import FullTextSearch, SearchField, Crosswalk


class API:
    def __init__(self):
        self.fts = FullTextSearch()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        return {
            "metadata": {"title": "Gutenberg OPDS"},
            "links": [
                {"rel": "self", "href": "/opds", "type": "application/opds+json"},
                {"rel": "search", "href": "/opds/search{?query}", "type": "application/opds+json", "templated": True}
            ],
            "publications": []
        }

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def search(self, query="", page=1, limit=10, field="book"):
        try:
            page = max(1, int(page))
            limit = max(1, int(limit))
        except Exception:
            page, limit = 1, 10

        field_map = {
            'book': SearchField.BOOK,
            'title': SearchField.TITLE,
            'author': SearchField.AUTHOR,
            'subject': SearchField.SUBJECT,
            'bookshelf': SearchField.BOOKSHELF,
            'attribute': SearchField.ATTRIBUTE,
        }
        search_field = field_map.get(field, SearchField.BOOK)

        q = (self.fts.query(crosswalk=Crosswalk.OPDS)
             .search(query, field=search_field)
             [page, limit])
        
        result = self.fts.execute(q)

        feed = {
            "metadata": {
                "title": "Gutenberg Search Results",
                "itemsPerPage": result["page_size"],
                "currentPage": result["page"],
                "numberOfItems": result["total"]
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"/opds/search?query={query}&page={page}&limit={limit}&field={field}",
                    "type": "application/opds+json"
                }
            ],
            "publications": result["results"]
        }

        feed["links"].append({
            "rel": "search",
            "href": "/opds/search{?query}",
            "type": "application/opds+json",
            "templated": True
        })

        if result["page"] > 1:
            feed["links"].append({
                "rel": "first",
                "href": f"/opds/search?query={query}&page=1&limit={limit}&field={field}",
                "type": "application/opds+json"
            })
            feed["links"].append({
                "rel": "previous",
                "href": f"/opds/search?query={query}&page={result['page'] - 1}&limit={limit}&field={field}",
                "type": "application/opds+json"
            })
        
        if result["page"] < result["total_pages"]:
            feed["links"].append({
                "rel": "next",
                "href": f"/opds/search?query={query}&page={result['page'] + 1}&limit={limit}&field={field}",
                "type": "application/opds+json"
            })
            feed["links"].append({
                "rel": "last",
                "href": f"/opds/search?query={query}&page={result['total_pages']}&limit={limit}&field={field}",
                "type": "application/opds+json"
            })

        return feed


if __name__ == "__main__":
    cherrypy.quickstart(API(), "/opds")