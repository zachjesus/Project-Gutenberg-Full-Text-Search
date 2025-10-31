import cherrypy
from FullTextSearchRewrite import FullTextSearch

def format_authors(book):
    authors = book.get("authors") or []
    result = []
    for a in authors:
        if not a or not a.get("name"):
            continue
        result.append({"name": a.get("name", "")})
    return result

def format_languages(book):
    langs = book.get("languages") or []
    return langs[0] if langs else "en"


def format_release_date(book):
    rd = book.get("release_date")
    return rd if rd else ""

def format_description(book):
    for attr in book.get("attributes", []):
        if not attr:
            continue
        if attr.get("type", "").startswith("520"):
            return attr.get("text", "")
    return ""

def format_images(book):
    images = []
    for f in book.get("files", []):
        if not f or not f.get("mediatype"):
            continue
        if isinstance(f.get("mediatype"), str) and f["mediatype"].startswith("image/"):
            images.append({
                "href": f.get("download_url"),
                "type": f["mediatype"],
                "rel": "cover"
            })
    return images

def format_download_links(book):
    links = []
    for f in book.get("files", []):
        if not f or not f.get("download_url") or not f.get("mediatype"):
            continue
        if isinstance(f.get("mediatype"), str) and f["mediatype"].startswith("image/"):
            continue  
        links.append({
            "rel": "alternate",
            "href": f["download_url"],
            "type": f["mediatype"],
            "title": f.get("filetype", ""),
            "length": f.get("size", None)
        })
    return links

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

        feed = {
            "metadata": {
                "title": "Gutenberg Search Results",
                "itemsPerPage": limit,
                "currentPage": page,
                "numberOfItems": len(results)
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"/search?query={query}&page={page}&limit={limit}",
                    "type": "application/opds+json"
                }
            ],
            "publications": []
        }

        for book in results:
            pub = {
                "metadata": {
                    "@type": "http://schema.org/Book",
                    "identifier": f"urn:gutenberg:{book.get('pk')}",
                    "title": book.get("title", ""),
                    "author": format_authors(book),
                    "language": format_languages(book),
                    "description": format_description(book),
                    "datePublished": format_release_date(book)
                },
                "links": [
                    {
                        "rel": "self",
                        "href": f"https://www.gutenberg.org/ebooks/{book.get('pk')}",
                        "type": "application/opds-publication+json"
                    }
                ] + format_download_links(book),
                "images": format_images(book)
            }
            feed["publications"].append(pub)

        return feed

if __name__ == "__main__":
    cherrypy.quickstart(API(), "/")