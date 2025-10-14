import curses
import locale
import re
from FullTextSearch import FullTextSearch

locale.setlocale(locale.LC_ALL, "")

ALLOWED_FIELDS = {"book", "author", "subject", "attribute", "bookshelf"}

class ConsoleSearch:
    def __init__(self):
        self.fts = FullTextSearch()
        self.limit = 10
        self.query_input = ""
        self.page = 1
        self.fields = None
        self.results = []
        self.status = ""

    def parse_query(self, s):
        s = s.strip()
        if not s:
            return None, None
        # Parse queries as "book: 'Guide' subject: 'Philosophy' etc..."
        tokens = re.findall(r'(\w+):\s*([^\s][^:]*?)(?=\s+\w+:|$)', s)
        fields = []
        query_parts = []
        for field, value in tokens:
            field = field.lower()
            if field in ALLOWED_FIELDS:
                fields.append(field)
                query_parts.append(value.strip())
        if fields and query_parts:
            return fields, " ".join(query_parts)
        # Allows for queries like "book, subject, attribute: "
        if ":" in s:
            left, right = s.split(":", 1)
            tokens = [t.strip().lower() for t in left.split(",") if t.strip()]
            fields = [t for t in tokens if t in ALLOWED_FIELDS]
            query = right.strip()
            return (fields if fields else None), query
        return None, s

    def fetch(self, page):
        try:
            r = self.fts.ranked_fulltext_search(
                query_text=self.query_text,
                limit=self.limit,
                search_fields=self.fields,
                page=page,
            )
            return r
        except Exception as e:
            self.status = f"error: {e}"
            return None

    def render(self, stdscr):
        stdscr.clear()
        h, w = stdscr.getmaxyx()

        title = "Gutenberg Search (Enter to search | ←/→ to page | ESC to quit)"
        stdscr.addnstr(0, 0, title, w)

        prompt = "Search (prefix fields like 'author: Herman subject: scifi'): "
        stdscr.addnstr(1, 0, prompt, w)
        stdscr.addnstr(1, len(prompt), self.query_input, max(0, w - len(prompt)))

        info = f"page {self.page} | limit {self.limit}"
        if self.fields:
            info += " | fields: " + ", ".join(self.fields)
        stdscr.addnstr(2, 0, info, w)

        y = 4
        if self.results:
            for i, row in enumerate(self.results, start=1):
                if y >= h - 1:
                    break
                raw_title = getattr(row, "title", "") or ""
                clean_title = re.sub(r'[\r\n\t]+', ' ', raw_title)
                clean_title = ''.join(ch for ch in clean_title if ch.isprintable())
                clean_title = ' '.join(clean_title.split())
                downloads = int(getattr(row, "downloads", 0) or 0)
                total_rank = float(getattr(row, "total_rank", 0.0) or 0.0)
                line = f"{i}. {clean_title}  [downloads: {downloads} | score: {total_rank:.3f}]"
                stdscr.addnstr(y, 0, line[:max(0, w - 1)], w)
                y += 1
        else:
            stdscr.addnstr(y, 0, "(no results)", w)

        if self.status:
            stdscr.addnstr(h - 1, 0, self.status[: w - 1], w)

        try:
            curses.curs_set(1)
        except Exception:
            pass
        stdscr.move(1, min(len(prompt) + len(self.query_input), max(0, w - 1)))
        stdscr.refresh()

    def main(self, stdscr):
        stdscr.keypad(True)
        curses.noecho()
        curses.cbreak()
        self.query_text = ""
        self.results = []
        self.status = ""

        while True:
            self.render(stdscr)
            ch = stdscr.getch()

            if ch in (27,):  # ESC
                break
            elif ch in (curses.KEY_ENTER, 10, 13):
                self.fields, self.query_text = self.parse_query(self.query_input)
                if not self.query_text:
                    self.status = "enter a query"
                    continue
                self.page = 1
                res = self.fetch(self.page)
                if res is not None:
                    self.results = res
                    self.status = f"{len(self.results)} results"
            elif ch == curses.KEY_LEFT:
                if self.page > 1:
                    new_page = self.page - 1
                    res = self.fetch(new_page)
                    if res is not None:
                        self.page = new_page
                        self.results = res
                        self.status = f"{len(self.results)} results"
            elif ch == curses.KEY_RIGHT:
                new_page = self.page + 1
                res = self.fetch(new_page)
                if res is not None and len(res) > 0:
                    self.page = new_page
                    self.results = res
                    self.status = f"{len(self.results)} results"
                else:
                    self.status = "no more pages"
            elif ch in (curses.KEY_BACKSPACE, 127, 8):
                if self.query_input:
                    self.query_input = self.query_input[:-1]
            elif ch == curses.KEY_RESIZE:
                pass
            else:
                if 32 <= ch <= 126:
                    self.query_input += chr(ch)

def run():
    curses.wrapper(ConsoleSearch().main)

if __name__ == "__main__":
    run()