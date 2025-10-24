Ensure Project Gutenberg postgres database is running and connected to via config settings found in the FullTextSearch.

To make venv for packages:
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

Then you can run the server or command prompt to interface with the full text search.

The full text search supports some [basic web operators](https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-PARSING-QUERIES).

Server can be accessed like http://127.0.0.1:8080/search?query=shakespeare&page=2&limit=5&fields=title,author if running locally.

Next features will be (1) updating the search to allow facet searching (2) Then making the server OPDS spec. 

Other long term goals are adding support for truncation and typo tolerance.
