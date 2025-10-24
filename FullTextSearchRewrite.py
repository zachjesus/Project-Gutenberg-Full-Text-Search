from collections import namedtuple
from libgutenberg import GutenbergDatabase, CommonOptions
from libgutenberg.Models import (
    Book, Author, Subject, Attribute, Bookshelf, BookAuthor, Category, Filetype, Locc, Lang, File, Dcmitype,
    t_mn_books_subjects, t_mn_books_bookshelves
)
from sqlalchemy import func, desc, asc, or_, literal

class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'

BookFacet = namedtuple('BookFacet', ['type', 'relation', 'column'])

FACETS = {
    'downloads': BookFacet('exact', None, Book.downloads),
    'downloads_gte': BookFacet('range_min', None, Book.downloads),
    'downloads_lte': BookFacet('range_max', None, Book.downloads),
    'release_date': BookFacet('exact', None, Book.release_date),
    'release_date_gte': BookFacet('range_min', None, Book.release_date),
    'release_date_lte': BookFacet('range_max', None, Book.release_date),
    'copyrighted': BookFacet('exact', None, Book.copyrighted),
    'audiobook': BookFacet('exact', None, Book.is_audiobook),
    'languages': BookFacet('m2m', 'langs', 'language'),
    'loc_class': BookFacet('m2m', 'loccs', 'locc'),
    'file_type': BookFacet('m2m', 'files', 'file_type'),
    'dcmi_type': BookFacet('m2m', 'categories', 'dcmitype'),
    'author_birthdate': BookFacet('o2m', 'authors', 'birthdate'),
    'author_birthdate_gte': BookFacet('range_min_o2m', 'authors', 'birthdate'),
    'author_birthdate_lte': BookFacet('range_max_o2m', 'authors', 'birthdate'),
    'author_deathdate': BookFacet('o2m', 'authors', 'deathdate'),
    'author_deathdate_gte': BookFacet('range_min_o2m', 'authors', 'deathdate'),
    'author_deathdate_lte': BookFacet('range_max_o2m', 'authors', 'deathdate'),
}

TSVEC_FACETS = {
    'book': BookFacet('tsvec', None, Book.tsvec),
    'author': BookFacet('tsvec', 'authors', Author.tsvec),
    'subject': BookFacet('tsvec', 'subjects', Subject.tsvec),
    'bookshelf': BookFacet('tsvec', 'bookshelves', Bookshelf.tsvec),
    'attribute': BookFacet('tsvec', 'attributes', Attribute.tsvec),
}

TGRM_FACETS = {
    'fuzzy_title': BookFacet('trgm', None, Book.title),
}

SEARCH_FIELD_WEIGHTS = {
    'book': 'B',
    'author': 'A',
    'subject': 'C',
    'attribute': 'C',
    'bookshelf': 'D',
}

def apply_exact_filter(query, facet, value):
    try:
        if isinstance(value, (list, tuple, set)):
            return query.filter(facet.column.in_(value))
        else:
            return query.filter(facet.column == value)
    except Exception as e:
        print(f"Error in exact filter: {e}")
        return query


def apply_range_filter(query, facet, value, op):
    """Only allows 'gte' or 'lte' for range operations."""
    try:
        if op not in ('gte', 'lte'):
            print(f"Invalid range op: {op}")
            return query
        if facet.relation:
            rel = getattr(Book, facet.relation)
            col = getattr(rel.property.mapper.class_, facet.column)
            if op == 'gte':
                return query.filter(rel.any(col >= value))
            else:
                return query.filter(rel.any(col <= value))
        else:
            if op == 'gte':
                return query.filter(facet.column >= value)
            else:
                return query.filter(facet.column <= value)
    except Exception as e:
        print(f"Error in range filter: {e}")
        return query


def apply_relation_filter(query, facet, value):
    """Handles both m2m and o2m filters."""
    try:
        rel = getattr(Book, facet.relation)
        col = getattr(rel.property.mapper.class_, facet.column)
        if isinstance(value, (list, tuple, set)):
            return query.filter(rel.any(col.in_(value)))
        else:
            return query.filter(rel.any(col == value))
    except Exception as e:
        print(f"Error in relation filter: {e}")
        return query


def apply_tsvec_filter(query, facet, value):
    try:
        tsquery = func.websearch_to_tsquery('english', value)
        if facet.relation:
            rel = getattr(Book, facet.relation)
            col = facet.column
            return query.filter(rel.any(col.op('@@')(tsquery)))
        else:
            return query.filter(facet.column.op('@@')(tsquery))
    except Exception as e:
        print(f"Error in tsvec filter: {e}")
        return query


def apply_trgm_filter(query, facet, value):
    try:
        return query.filter(facet.column.op('%')(value))
    except Exception as e:
        print(f"Error in trgm filter: {e}")
        return query


def apply_filters(query, filter_dict):
    """Apply all filters from filter_dict to the query."""
    for filter_key, filter_value in filter_dict.items():
        if filter_value is None:
            continue
        facet = FACETS.get(filter_key) or TSVEC_FACETS.get(filter_key) or TGRM_FACETS.get(filter_key)
        if not facet:
            print(f"Unknown filter key: {filter_key}")
            continue

        if facet.type == 'exact':
            query = apply_exact_filter(query, facet, filter_value)
        elif facet.type == 'range_min':
            query = apply_range_filter(query, facet, filter_value, 'gte')
        elif facet.type == 'range_max':
            query = apply_range_filter(query, facet, filter_value, 'lte')
        elif facet.type in ('m2m', 'o2m'):
            query = apply_relation_filter(query, facet, filter_value)
        elif facet.type == 'range_min_o2m':
            query = apply_range_filter(query, facet, filter_value, 'gte')
        elif facet.type == 'range_max_o2m':
            query = apply_range_filter(query, facet, filter_value, 'lte')
        elif facet.type == 'tsvec':
            query = apply_tsvec_filter(query, facet, filter_value)
        elif facet.type == 'trgm':
            query = apply_trgm_filter(query, facet, filter_value)

    return query


def get_base_query_for_field(session, field_name):
    """Get the base query for a specific search field."""
    base_query = session.query(Book).select_from(Book)
    
    if field_name == 'author':
        return base_query.join(BookAuthor, Book.pk == BookAuthor.fk_books).join(Author, Author.id == BookAuthor.fk_authors)
    elif field_name == 'subject':
        return base_query.join(t_mn_books_subjects, Book.pk == t_mn_books_subjects.c.fk_books).join(Subject, Subject.id == t_mn_books_subjects.c.fk_subjects)
    elif field_name == 'attribute':
        return base_query.join(Attribute, Attribute.fk_books == Book.pk)
    elif field_name == 'bookshelf':
        return base_query.join(t_mn_books_bookshelves, Book.pk == t_mn_books_bookshelves.c.fk_books).join(Bookshelf, Bookshelf.id == t_mn_books_bookshelves.c.fk_bookshelves)
    
    return base_query


class FullTextSearch:
    def __init__(self):
        options = CommonOptions.Options()
        options.config = Config()
        self.ob = GutenbergDatabase.Objectbase(False)

    def get_facets_for_book(self, book):
        """Return a dict of all facet values for a Book instance."""
        result = {}
        for key, facet in FACETS.items():
            try:
                if facet.relation:
                    rel = getattr(book, facet.relation)
                    if isinstance(rel, list):
                        result[key] = [getattr(obj, facet.column) for obj in rel]
                    else:
                        result[key] = getattr(rel, facet.column)
                else:
                    result[key] = getattr(book, facet.column.key)
            except Exception as e:
                result[key] = None

        result['pk'] = book.pk
        result['title'] = book.title
        
        return result

    def ranked_fulltext_search(self, query_text, limit=100, search_fields=None, page=1, filters=None):
        """Perform ranked full-text search with filtering and pagination."""
        session = self.ob.get_session()
        try:
            limit = int(limit)
            page = max(1, int(page))
            offset = (page - 1) * limit
            filters = filters or {}
            query_text = (query_text or '').strip()
            download_boost = 1.5

            # Handle empty query - return by downloads and title
            if not query_text:
                base_query = session.query(Book).select_from(Book)
                filtered_query = apply_filters(base_query, filters)
                books = filtered_query \
                    .order_by(desc(Book.downloads), asc(Book.title)) \
                    .offset(offset) \
                    .limit(limit) \
                    .all()
                return [self.get_facets_for_book(book) for book in books]

            # Full-text search with ranking
            tsquery = func.websearch_to_tsquery('english', query_text)
            fields = set(search_fields or ['book', 'author', 'subject', 'attribute', 'bookshelf'])

            # Build ranking queries for each field
            rank_queries = []
            for field in fields:
                if field not in SEARCH_FIELD_WEIGHTS:
                    continue
                    
                base_query = get_base_query_for_field(session, field)
                filtered_query = apply_filters(base_query, filters)
                weight = SEARCH_FIELD_WEIGHTS[field]
                tsvec_col = TSVEC_FACETS[field].column if field in TSVEC_FACETS else Book.tsvec

                rank_q = (
                    filtered_query
                    .with_entities(
                        Book.pk.label('book_pk'),
                        literal(field).label('match_type'),
                        func.ts_rank_cd(func.setweight(tsvec_col, weight), tsquery).label('rank')
                    )
                    .filter(tsvec_col.op('@@')(tsquery))
                )
                rank_queries.append(rank_q)

            if not rank_queries:
                return []

            # Combine all rank queries
            union_q = rank_queries[0] if len(rank_queries) == 1 else rank_queries[0].union_all(*rank_queries[1:])
            combined = union_q.subquery('combined')

            # Aggregate ranks by book
            ranked_books_cte = (
                session.query(
                    combined.c.book_pk,
                    func.sum(combined.c.rank).label('text_rank'),
                    func.array_agg(func.distinct(combined.c.match_type)).label('match_types')
                )
                .select_from(combined)
                .group_by(combined.c.book_pk)
            ).cte('ranked_books')

            # Combine text rank with download boost
            top_books_cte = (
                session.query(
                    ranked_books_cte.c.book_pk,
                    (ranked_books_cte.c.text_rank +
                     func.ln(1 + func.coalesce(Book.downloads, 0)) * download_boost).label('total_rank')
                )
                .select_from(ranked_books_cte)
                .join(Book, Book.pk == ranked_books_cte.c.book_pk)
                .order_by(desc('total_rank'))
                .offset(offset)
                .limit(limit)
            ).cte('top_books')

            books = session.query(Book).filter(Book.pk.in_(
                session.query(top_books_cte.c.book_pk)
            )).all()
            return [self.get_facets_for_book(book) for book in books]

        finally:
            session.close()

import pprint
if __name__ == '__main__':
    fts = FullTextSearch()
    pprint.pprint(fts.ranked_fulltext_search('Computer', limit=10, search_fields=['book'], page=1, filters={'copyrighted': 1}))