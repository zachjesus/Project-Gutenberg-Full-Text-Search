from libgutenberg import GutenbergDatabase, CommonOptions
from libgutenberg.Models import (
    Book, Author, Subject, Attribute, Bookshelf, BookAuthor, Category, Filetype, Locc, Lang, File,
    t_mn_books_subjects, t_mn_books_bookshelves
)
from sqlalchemy import func, desc, asc, literal, distinct
from libgutenberg import DublinCoreMapping

class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'

FACETS = {
    'downloads': Book.downloads,
    'downloads_min': Book.downloads,
    'downloads_max': Book.downloads,
    'release_date': Book.release_date,
    'release_date_min': Book.release_date,
    'release_date_max': Book.release_date,
    'copyrighted': Book.copyrighted,
    'birthdate': Book.authors,
    'birthdate_min': Book.authors,
    'birthdate_max': Book.authors,
    'langs': Book.langs,
    'filetype': Book.files,
    'dcmitype': Book.categories,
    'audiobook': Book.is_audiobook,
    'locc': Book.loccs,
}

TSVEC_FACETS = {
    'author': (Author.tsvec, 'author'),
    'subject': (Subject.tsvec, 'subject'),
    'bookshelf': (Bookshelf.tsvec, 'bookshelf'),
    'attribute': (Attribute.tsvec, 'attribute'),
    'title': (Book.tsvec, 'book'),
}

def apply_filter(query, filter_dict):
    col_map = FACETS
    tsvec_map = TSVEC_FACETS

    invalid_keys = set(filter_dict.keys()) - set(FACETS.keys()) - set(TSVEC_FACETS.keys())
    if invalid_keys:
        raise ValueError(f"Invalid filter keys: {', '.join(invalid_keys)}")

    for key, value in filter_dict.items():
        if value is None:
            continue
        try:
            if key in ['downloads_min', 'release_date_min']:
                query = query.filter(col_map[key] >= value)
            elif key in ['downloads_max', 'release_date_max']:
                query = query.filter(col_map[key] <= value)
            elif key in tsvec_map:
                tsvec_col, _ = tsvec_map[key]
                tsquery = func.websearch_to_tsquery('english', value)
                query = query.filter(tsvec_col.op('@@')(tsquery))
            elif key == 'audiobook':
                query = query.filter(col_map[key] == value)
            elif key == 'langs':
                if isinstance(value, (list, tuple, set)):
                    query = query.filter(Book.langs.any(Lang.language.in_(value)))
                else:
                    query = query.filter(Book.langs.any(Lang.language == value))
            elif key == 'locc':
                if isinstance(value, (list, tuple, set)):
                    query = query.filter(Book.loccs.any(Locc.locc.in_(value)))
                else:
                    query = query.filter(Book.loccs.any(Locc.locc == value))
            elif key == 'filetype':
                if isinstance(value, (list, tuple, set)):
                    query = query.filter(Book.files.any(File.file_type.has(Filetype.filetype.in_(value))))
                else:
                    query = query.filter(Book.files.any(File.file_type.has(Filetype.filetype == value)))
            elif key == 'dcmitype':
                if isinstance(value, (list, tuple, set)):
                    query = query.filter(Book.categories.any(Category.dcmitype.in_(value)))
                else:
                    query = query.filter(Book.categories.any(Category.dcmitype == value))
            elif key == 'birthdate':
                if isinstance(value, (list, tuple, set)):
                    query = query.filter(Book.authors.any(Author.birthdate.in_(value)))
                else:
                    query = query.filter(Book.authors.any(Author.birthdate == value))
            elif key == 'birthdate_min':
                query = query.filter(Book.authors.any(Author.birthdate >= value))
            elif key == 'birthdate_max':
                query = query.filter(Book.authors.any(Author.birthdate <= value))
            else:
                if isinstance(value, (list, tuple, set)):
                    query = query.filter(col_map[key].in_(value))
                else:
                    query = query.filter(col_map[key] == value)
        except Exception:
            continue

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
                results = []
                for pk in pk_list:
                    dc = DublinCoreMapping.DublinCoreObject(session=session)
                    dc.load_from_database(pk)
                    results.append(dc.__dict__)
                return results

            fields = set(search_fields or ['book', 'author', 'subject', 'attribute', 'bookshelf'])
            tsquery = func.websearch_to_tsquery('english', query_text)

            def make_q(base_query, tsvec_col, weight_letter, match_type):
                filtered_query = apply_filter(base_query, filters)
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

            pk_list = [r.book_pk for r in session.query(top_books_cte.c.book_pk).all()]
            if not pk_list:
                return []

            results = []
            for pk in pk_list:
                dc = DublinCoreMapping.DublinCoreObject(session=session)
                dc.load_from_database(pk)
                results.append(dc.__dict__)
            return results
        finally:
            session.close()

if __name__ == '__main__':
    from pprint import pprint
    fts = FullTextSearch()

    pprint(fts.ranked_fulltext_search("Shakespeare", limit=10, page=2))
