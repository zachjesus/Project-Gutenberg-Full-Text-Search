
class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'

from libgutenberg import GutenbergDatabase, CommonOptions
from libgutenberg.Models import (
    Book, Author, Subject, Attribute, Bookshelf, BookAuthor, File,
    t_mn_books_subjects, t_mn_books_bookshelves
)
from sqlalchemy import func, desc, distinct, literal, asc

def apply_filter(query, filter_dict, already_joined=None):
    already_joined = already_joined or set()
    col_map = {
        'title': Book.title,
        'downloads': Book.downloads,
        'downloads_min': Book.downloads,
        'downloads_max': Book.downloads,
        'release_date': Book.release_date,
        'release_date_min': Book.release_date,
        'release_date_max': Book.release_date,
        'copyrighted': Book.copyrighted,
    }
    tsvec_map = {
        'author': (Author.tsvec, 'author'),
        'subject': (Subject.tsvec, 'subject'),
        'bookshelf': (Bookshelf.tsvec, 'bookshelf'),
        'attribute': (Attribute.tsvec, 'attribute'),
        'title': (Book.tsvec, 'book'),
    }
    if 'author' in filter_dict and 'author' not in already_joined:
        query = query.join(BookAuthor, Book.pk == BookAuthor.fk_books).join(Author, Author.id == BookAuthor.fk_authors)
    if 'subject' in filter_dict and 'subject' not in already_joined:
        query = query.join(t_mn_books_subjects, Book.pk == t_mn_books_subjects.c.fk_books).join(Subject, Subject.id == t_mn_books_subjects.c.fk_subjects)
    if 'bookshelf' in filter_dict and 'bookshelf' not in already_joined:
        query = query.join(t_mn_books_bookshelves, Book.pk == t_mn_books_bookshelves.c.fk_books).join(Bookshelf, Bookshelf.id == t_mn_books_bookshelves.c.fk_bookshelves)
    if 'attribute' in filter_dict and 'attribute' not in already_joined:
        query = query.join(Attribute, Attribute.fk_books == Book.pk)
    for key, value in filter_dict.items():
        if value is None:
            continue
        if key.endswith('_min'):
            query = query.filter(col_map[key] >= value)
        elif key.endswith('_max'):
            query = query.filter(col_map[key] <= value)
        elif key in tsvec_map:
            tsvec_col, _ = tsvec_map[key]
            tsquery = func.websearch_to_tsquery('english', value)
            query = query.filter(tsvec_col.op('@@')(tsquery))
        else:
            query = query.filter(col_map[key] == value)
    return query

class FullTextSearch:
    def __init__(self):
        options = CommonOptions.Options()
        options.config = Config()
        self.ob = GutenbergDatabase.Objectbase(False)

    def ranked_fulltext_search(self, query_text, limit=100, search_fields=None, page=1, filters=None):
        session = self.ob.get_session()
        try:
            limit = int(limit)
            page = max(1, int(page))
            offset = (page - 1) * limit
            filters = filters or {}
            download_boost = 1.5
            query_text = (query_text or '').strip()

            if not query_text:
                base_query = session.query(Book).select_from(Book)
                filtered_query = apply_filter(base_query, filters)
                pk_q = (
                    filtered_query
                    .with_entities(Book.pk)
                    .order_by(desc(Book.downloads), asc(Book.title))
                    .offset(offset)
                    .limit(limit)
                )
                pk_list = [r.pk for r in pk_q.all()]
                if not pk_list:
                    return []
                q = (
                    session.query(
                        Book.pk.label('pk'),
                        Book.title.label('title'),
                        func.coalesce(Book.downloads, 0).label('downloads'),
                        Book.release_date.label('release_date'),
                        Book.copyrighted.label('copyrighted'),
                        (session.query(func.array_agg(distinct(Author.name)))
                            .select_from(Author.__table__.join(BookAuthor.__table__, Author.id == BookAuthor.fk_authors))
                            .filter(BookAuthor.fk_books == Book.pk)
                            .correlate(Book)
                            .scalar_subquery()
                        ).label('authors'),
                        (session.query(func.array_agg(distinct(Subject.subject)))
                            .select_from(Subject.__table__.join(t_mn_books_subjects, Subject.id == t_mn_books_subjects.c.fk_subjects))
                            .filter(t_mn_books_subjects.c.fk_books == Book.pk)
                            .correlate(Book)
                            .scalar_subquery()
                        ).label('subjects'),
                        (session.query(func.array_agg(distinct(Attribute.text)))
                            .filter(Attribute.fk_books == Book.pk)
                            .correlate(Book)
                            .scalar_subquery()
                        ).label('attributes'),
                        (session.query(func.array_agg(distinct(Bookshelf.bookshelf)))
                            .select_from(Bookshelf.__table__.join(t_mn_books_bookshelves, Bookshelf.id == t_mn_books_bookshelves.c.fk_bookshelves))
                            .filter(t_mn_books_bookshelves.c.fk_books == Book.pk)
                            .correlate(Book)
                            .scalar_subquery()
                        ).label('bookshelves'),
                        (session.query(func.min(File.archive_path))
                            .filter(File.fk_books == Book.pk)
                            .correlate(Book)
                            .scalar_subquery()
                        ).label('download_path'),
                        literal(0.0).label('text_rank'),
                        literal(['filter']).label('match_types'),
                        (func.ln(1 + func.coalesce(Book.downloads, 0)) * download_boost).label('total_rank'),
                    )
                    .filter(Book.pk.in_(pk_list))
                    .order_by(desc('total_rank'))
                )
                rows = []
                for r in q.all():
                    link = None
                    if r.download_path:
                        link = 'https://www.gutenberg.org/' + r.download_path
                    rows.append({
                        'pk': r.pk,
                        'title': r.title,
                        'downloads': r.downloads,
                        'release_date': r.release_date,
                        'copyrighted': r.copyrighted,
                        'authors': r.authors or [],
                        'subjects': r.subjects or [],
                        'attributes': r.attributes or [],
                        'bookshelves': r.bookshelves or [],
                        'download_link': link,
                        'text_rank': r.text_rank,
                        'match_types': r.match_types,
                        'total_rank': r.total_rank,
                    })
                return rows

            fields = set(search_fields or ['book', 'author', 'subject', 'attribute', 'bookshelf'])
            tsquery = func.websearch_to_tsquery('english', query_text)

            def make_q(base_query, tsvec_col, weight_letter, match_type):
                joined = {match_type} if match_type != 'book' else set()
                filtered_query = apply_filter(base_query, filters, already_joined=joined)
                return (
                    filtered_query
                    .with_entities(
                        Book.pk.label('book_pk'),
                        literal(match_type).label('match_type'),
                        func.ts_rank_cd(func.setweight(tsvec_col, weight_letter), tsquery).label('rank')
                    )
                    .filter(tsvec_col.op('@@')(tsquery))
                )

            specs = [
                ('book', session.query(Book).select_from(Book), Book.tsvec, 'B'),
                ('author', session.query(Book).select_from(Book)
                    .join(BookAuthor, Book.pk == BookAuthor.fk_books)
                    .join(Author, Author.id == BookAuthor.fk_authors), Author.tsvec, 'A'),
                ('subject', session.query(Book).select_from(Book)
                    .join(t_mn_books_subjects, Book.pk == t_mn_books_subjects.c.fk_books)
                    .join(Subject, Subject.id == t_mn_books_subjects.c.fk_subjects), Subject.tsvec, 'C'),
                ('attribute', session.query(Book).select_from(Book)
                    .join(Attribute, Attribute.fk_books == Book.pk), Attribute.tsvec, 'C'),
                ('bookshelf', session.query(Book).select_from(Book)
                    .join(t_mn_books_bookshelves, Book.pk == t_mn_books_bookshelves.c.fk_books)
                    .join(Bookshelf, Bookshelf.id == t_mn_books_bookshelves.c.fk_bookshelves), Bookshelf.tsvec, 'D'),
            ]

            result_queries = [make_q(base, tsvec, w, name) for name, base, tsvec, w in specs if name in fields]
            if not result_queries:
                return []

            union_q = result_queries[0] if len(result_queries) == 1 else result_queries[0].union_all(*result_queries[1:])
            combined = union_q.subquery('combined')

            ranked_books_cte = (
                session.query(
                    combined.c.book_pk,
                    func.sum(combined.c.rank).label('text_rank'),
                    func.array_agg(distinct(combined.c.match_type)).label('match_types')
                )
                .select_from(combined)
                .group_by(combined.c.book_pk)
            ).cte('ranked_books')

            top_books_cte = (
                session.query(
                    ranked_books_cte.c.book_pk,
                    ranked_books_cte.c.text_rank,
                    ranked_books_cte.c.match_types,
                    (ranked_books_cte.c.text_rank +
                     func.ln(1 + func.coalesce(Book.downloads, 0)) * download_boost).label('total_rank')
                )
                .select_from(ranked_books_cte)
                .join(Book, Book.pk == ranked_books_cte.c.book_pk)
                .order_by(desc('total_rank'))
                .limit(limit)
                .offset(offset)
            ).cte('top_books')

            final_results = (
                session.query(
                    Book.pk.label('pk'),
                    Book.title.label('title'),
                    func.coalesce(Book.downloads, 0).label('downloads'),
                    Book.release_date.label('release_date'),
                    Book.copyrighted.label('copyrighted'),
                    func.array_agg(distinct(Author.name)).label('authors'),
                    func.array_agg(distinct(Subject.subject)).label('subjects'),
                    func.array_agg(distinct(Attribute.text)).label('attributes'),
                    func.array_agg(distinct(Bookshelf.bookshelf)).label('bookshelves'),
                    func.concat('https://www.gutenberg.org/', func.min(File.archive_path)).label('download_link'),
                    top_books_cte.c.text_rank,
                    top_books_cte.c.match_types,
                    top_books_cte.c.total_rank,
                )
                .select_from(top_books_cte)
                .join(Book, Book.pk == top_books_cte.c.book_pk)
                .outerjoin(BookAuthor, BookAuthor.fk_books == Book.pk)
                .outerjoin(Author, Author.id == BookAuthor.fk_authors)
                .outerjoin(t_mn_books_subjects, t_mn_books_subjects.c.fk_books == Book.pk)
                .outerjoin(Subject, Subject.id == t_mn_books_subjects.c.fk_subjects)
                .outerjoin(Attribute, Attribute.fk_books == Book.pk)
                .outerjoin(t_mn_books_bookshelves, t_mn_books_bookshelves.c.fk_books == Book.pk)
                .outerjoin(Bookshelf, Bookshelf.id == t_mn_books_bookshelves.c.fk_bookshelves)
                .outerjoin(File, File.fk_books == Book.pk)
                .group_by(
                    Book.pk, Book.title, Book.downloads, Book.release_date,
                    Book.copyrighted, top_books_cte.c.text_rank,
                    top_books_cte.c.match_types, top_books_cte.c.total_rank
                )
                .order_by(desc(top_books_cte.c.total_rank))
            )
            return final_results.all()
        finally:
            session.close()

if __name__ == '__main__':
    fts = FullTextSearch()
    results = fts.ranked_fulltext_search('', limit=10, page=1, filters={'downloads_min': 10000})
    print(results)