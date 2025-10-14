class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'

from libgutenberg import GutenbergDatabase, CommonOptions
from libgutenberg.Models import (
    Book, Author, Subject, Attribute, Bookshelf, BookAuthor,
    t_mn_books_subjects, t_mn_books_bookshelves
)
from sqlalchemy import func, desc, distinct, literal, asc

class FullTextSearch:
    def __init__(self):
        options = CommonOptions.Options()
        options.config = Config()
        self.ob = GutenbergDatabase.Objectbase(False)

    def ranked_fulltext_search(self, query_text, limit=100, search_fields=None, page=1):
        session = self.ob.get_session()
        try:
            fields = set(search_fields or ['book', 'author', 'subject', 'attribute', 'bookshelf'])
            tsquery = func.websearch_to_tsquery('english', query_text)
            download_boost = 1.50
            page = max(1, int(page))
            offset = (page - 1) * int(limit)

            def make_q(base_query, tsvec_col, weight_letter, match_type):
                return (
                    base_query
                    .with_entities(
                        Book.pk.label('book_pk'),
                        Book.title.label('title'),
                        func.coalesce(Book.downloads, 0).label('downloads'),
                        Book.release_date.label('release_date'),
                        func.ts_rank_cd(func.setweight(tsvec_col, weight_letter), tsquery).label('rank'),
                        literal(match_type).label('match_type'),
                    )
                    .filter(tsvec_col.op('@@')(tsquery))
                )

            specs = [
                ('book',     session.query(Book), Book.tsvec, 'B'),
                ('author',   session.query(Book)
                                  .join(BookAuthor, Book.pk == BookAuthor.fk_books)
                                  .join(Author, Author.id == BookAuthor.fk_authors), Author.tsvec,   'A'),
                ('subject',  session.query(Book)
                                  .join(t_mn_books_subjects, Book.pk == t_mn_books_subjects.c.fk_books)
                                  .join(Subject, Subject.id == t_mn_books_subjects.c.fk_subjects), Subject.tsvec, 'C'),
                ('attribute',session.query(Book)
                                  .join(Attribute, Attribute.fk_books == Book.pk), Attribute.tsvec,'C'),
                ('bookshelf',session.query(Book)
                                  .join(t_mn_books_bookshelves, Book.pk == t_mn_books_bookshelves.c.fk_books)
                                  .join(Bookshelf, Bookshelf.id == t_mn_books_bookshelves.c.fk_bookshelves), Bookshelf.tsvec, 'D'),
            ]

            result_queries = [make_q(base, tsvec, w, name) for name, base, tsvec, w in specs if name in fields]
            if not result_queries:
                return []

            union_q = result_queries[0] if len(result_queries) == 1 else result_queries[0].union_all(*result_queries[1:])
            combined = union_q.subquery('combined')

            book_pk, title, downloads, release_date, rank, match_type = [combined.c[i] for i in range(6)]
            download_score = func.ln(1 + downloads) * download_boost

            final_results = (
                session.query(
                    book_pk.label('pk'),
                    title.label('title'),
                    downloads.label('downloads'),
                    release_date.label('release_date'),
                    func.sum(rank).label('text_rank'),
                    func.array_agg(distinct(match_type)).label('match_types'),
                    (func.sum(rank) + download_score).label('total_rank'),
                )
                .group_by(book_pk, title, downloads, release_date)
                .order_by(desc('total_rank'), desc(downloads), asc(title), asc(book_pk))
                .offset(offset)
                .limit(limit)
            )

            return final_results.all()
        finally:
            session.close()