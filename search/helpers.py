import re
from functools import wraps
from typing import Any, Callable

from constants import LoCCMainClass
from sqlalchemy import text
from sqlalchemy.orm import Session

_RE_MARC_SUBFIELD = re.compile(r"\$[a-z]")
_RE_MARC_SPSEP = re.compile(r"[\n ](,|:)([A-Za-z0-9])")
_RE_CURLY_SINGLE = re.compile("[\u2018\u2019]")  # ' '
_RE_CURLY_DOUBLE = re.compile("[\u201c\u201d]")  # " "
_RE_TITLE_SPLITTER = re.compile(r"\s*[;:]\s*")


_FIELDS_TO_FORMAT = frozenset(
    {
        "title",
        "subtitle",
        "alt_title",
        "author",
        "name",
        "publisher",
        "subject",
        "bookshelf",
        "subjects",
        "bookshelves",
    }
)


def strip_marc_subfields(text: str) -> str:
    """
    Based on libgutenberg.DublinCore.strip_marc_subfields.
    """
    if not text or not isinstance(text, str):
        return ""
    text = _RE_MARC_SUBFIELD.sub("", text)
    text = _RE_MARC_SPSEP.sub(r"\1 \2", text)
    return text.strip()


def normalize_text(text: str) -> str:
    """
    Based on libgutenberg.DublinCore.format_title.
    """
    if not text or not isinstance(text, str):
        return ""
    text = _RE_CURLY_SINGLE.sub("'", text)
    text = _RE_CURLY_DOUBLE.sub('"', text)
    text = _RE_TITLE_SPLITTER.sub(": ", text)
    return text.rstrip(": ").strip()


def format_field(
    key: str, value: str, fields_to_format: frozenset = _FIELDS_TO_FORMAT
) -> str:
    if not value or not isinstance(value, str):
        return ""
    text = value
    if key in fields_to_format:
        text = strip_marc_subfields(text)
        text = normalize_text(text)
    return text.strip()


def format_dict(d: dict, fields_to_format: frozenset = _FIELDS_TO_FORMAT) -> dict:
    result = {}
    for key, value in d.items():
        if isinstance(value, str):
            result[key] = format_field(key, value, fields_to_format)
        elif isinstance(value, dict):
            result[key] = format_dict(value, fields_to_format)
        elif isinstance(value, list):
            result[key] = format_list(key, value, fields_to_format)
        else:
            result[key] = value
    return result


def format_list(
    parent_key: str, lst: list, fields_to_format: frozenset = _FIELDS_TO_FORMAT
) -> list:
    result = []
    for item in lst:
        if isinstance(item, dict):
            result.append(format_dict(item, fields_to_format))
        elif isinstance(item, str):
            result.append(format_field(parent_key, item, fields_to_format))
        elif isinstance(item, list):
            result.append(format_list(parent_key, item, fields_to_format))
        else:
            result.append(item)
    return result


def format_dict_result(
    fn: Callable | None = None, *, fields_to_format: frozenset = _FIELDS_TO_FORMAT
) -> Callable:
    """
    Decorator that formats dict results using the module helpers.

    Usage:
      @format_dict_result
      def f(...): ...

      @format_dict_result(fields_to_format=['title', 'author'])
      def f(...): ...

    `fields_to_format` (if provided) overrides the module default `_FIELDS_TO_FORMAT`.
    """
    fields_fs = frozenset(fields_to_format)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            result = func(*args, **kwargs)
            if isinstance(result, dict):
                return format_dict(result, fields_fs)
            return result

        return wrapper

    if fn is None:
        return decorator
    return decorator(fn)


def get_locc_children(parent: LoCCMainClass | str, session: Session) -> list[dict]:
    """
    Get LoCC children for `parent` using the provided SQLAlchemy ORM Session.
    """
    if isinstance(parent, LoCCMainClass):
        parent = parent.code
    else:
        parent = (parent or "").strip().upper()

    if not parent:
        sorted_classes = sorted(LoCCMainClass, key=lambda x: x.code)
        return [
            {"code": item.code, "label": item.label, "has_children": True}
            for item in sorted_classes
        ]

    sql = text(
        """
        SELECT lc.pk AS code, lc.label AS label,
                EXISTS (
                    SELECT 1 FROM loccs lc2 WHERE lc2.pk LIKE lc.pk || '%' AND lc2.pk != lc.pk
                ) AS has_children
        FROM loccs lc
        WHERE lc.pk LIKE :pattern AND lc.pk != :parent
        ORDER BY char_length(lc.pk), lc.pk
        """
    )

    params = {"pattern": f"{parent}%", "parent": parent}
    rows = session.execute(sql, params).mappings().all()

    return [
        {
            "code": r["code"],
            "label": r["label"],
            "has_children": bool(r["has_children"]),
        }
        for r in rows
    ]
