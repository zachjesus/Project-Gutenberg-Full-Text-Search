import html
from typing import Any

from .constants import Crosswalk
from .helpers import format_dict_result


@format_dict_result
def crosswalk_full(row) -> dict[str, Any]:
    return {
        "book_id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads,
        "dc": row.dc,
    }


@format_dict_result
def crosswalk_mini(row) -> dict[str, Any]:
    return {
        "id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads,
    }


@format_dict_result
def crosswalk_pg(row) -> dict[str, Any]:
    dc = row.dc or {}
    return {
        "ebook_no": row.book_id,
        "title": row.title,
        "contributors": [
            {"name": c.get("name"), "role": c.get("role", "Author")}
            for c in dc.get("creators", [])
        ],
        "language": dc.get("language"),
        "subjects": [s["subject"] for s in dc.get("subjects", []) if s.get("subject")],
        "bookshelves": [
            b["bookshelf"] for b in dc.get("bookshelves", []) if b.get("bookshelf")
        ],
        "release_date": dc.get("date"),
        "downloads_last_30_days": row.downloads,
        "files": [
            {
                "filename": f.get("filename"),
                "type": f.get("mediatype"),
                "size": f.get("extent"),
            }
            for f in dc.get("format", [])
            if f.get("filename")
        ],
        "cover_url": (dc.get("coverpage") or [None])[0],
    }


@format_dict_result
def crosswalk_opds(row) -> dict[str, Any]:
    """Transform row to OPDS 2.0 publication format per spec."""
    dc = row.dc or {}

    metadata = {
        "@type": "http://schema.org/Book",
        "identifier": f"urn:gutenberg:{row.book_id}",
        "title": row.title,
        "language": (dc.get("language") or [{}])[0].get("code") or "en",
    }

    creators = dc.get("creators", [])
    if creators and creators[0].get("name"):
        p = creators[0]
        author = {"name": p["name"], "sortAs": p["name"]}
        if p.get("id"):
            author["identifier"] = f"https://www.gutenberg.org/ebooks/author/{p['id']}"
        metadata["author"] = author

    if dc.get("date"):
        metadata["published"] = dc["date"]

    for m in dc.get("marc", []):
        if m.get("code") == 508 and "Updated:" in (m.get("text") or ""):
            try:
                modified = m["text"].split("Updated:")[1].strip().split()[0].rstrip(".")
                if modified:
                    metadata["modified"] = modified
            except (IndexError, AttributeError):
                pass
            break

    desc_parts = []
    if summary := (dc.get("summary") or [None])[0]:
        desc_parts.append(summary)
    if notes := dc.get("description"):
        desc_parts.append(f"Notes: {'; '.join(notes)}")
    if credits := (dc.get("credits") or [None])[0]:
        desc_parts.append(f"Credits: {credits}")
    for m in dc.get("marc", []):
        if m.get("code") == 908 and m.get("text"):
            desc_parts.append(f"Reading Level: {m['text']}")
            break
    if dcmitype := [t["dcmitype"] for t in dc.get("type", []) if t.get("dcmitype")]:
        desc_parts.append(f"Category: {', '.join(dcmitype)}")
    if rights := dc.get("rights"):
        desc_parts.append(f"Rights: {rights}")
    desc_parts.append(f"Downloads: {row.downloads}")

    if desc_parts:
        metadata["description"] = (
            "<p>" + "</p><p>".join(html.escape(p) for p in desc_parts) + "</p>"
        )

    subjects = [s["subject"] for s in dc.get("subjects", []) if s.get("subject")]
    subjects += [c["locc"] for c in dc.get("coverage", []) if c.get("locc")]
    if subjects:
        metadata["subject"] = subjects

    if pub_raw := (dc.get("publisher") or {}).get("raw"):
        metadata["publisher"] = pub_raw

    collections = []
    for b in dc.get("bookshelves", []):
        if b.get("bookshelf"):
            collections.append(
                {
                    "name": b["bookshelf"],
                    "identifier": f"https://www.gutenberg.org/ebooks/bookshelf/{b.get('id', '')}",
                }
            )
    for c in dc.get("coverage", []):
        if c.get("locc"):
            collections.append(
                {
                    "name": c["locc"],
                    "identifier": f"https://www.gutenberg.org/ebooks/locc/{c.get('id', '')}",
                }
            )
    if collections:
        metadata["belongsTo"] = {"collection": collections}

    links = []

    # Audiobooks: use HTML index | Text books: prefer EPUB3 with images
    target_format = "index" if row.is_audio else "epub3.images"
    fallback_formats = ["epub.images", "epub.noimages", "kindle.images", "pdf.images", "pdf.noimages", "html"] if not row.is_audio else ["html"]

    # Try target format first, then fallbacks
    for try_format in [target_format] + fallback_formats:
        for f in dc.get("format", []):
            fn = f.get("filename")
            if not fn:
                continue
            ftype = (f.get("filetype") or "").strip().lower()
            if ftype != try_format:
                continue

            href = (
                fn
                if fn.startswith(("http://", "https://"))
                else f"https://www.gutenberg.org/{fn.lstrip('/')}"
            )
            mtype = (f.get("mediatype") or "").strip()

            link = {
                "rel": "http://opds-spec.org/acquisition/open-access",
                "href": href,
                "type": mtype or "application/epub+zip",
            }
            if f.get("extent") is not None and f["extent"] > 0:
                link["length"] = f["extent"]
            if f.get("hr_filetype"):
                link["title"] = f["hr_filetype"]
            links.append(link)
            break
        if links:
            break

    # OPDS 2.0 requires at least one acquisition link - fallback to readable HTML page
    if not links:
        links.append({
            "rel": "http://opds-spec.org/acquisition/open-access",
            "href": f"https://www.gutenberg.org/ebooks/{row.book_id}",
            "type": "text/html",
        })

    result = {"metadata": metadata, "links": links}

    images = []
    for f in dc.get("format", []):
        ft = f.get("filetype") or ""
        fn = f.get("filename")
        if fn and ("cover.medium" in ft or ("cover" in ft and not images)):
            href = (
                fn
                if fn.startswith(("http://", "https://"))
                else f"https://www.gutenberg.org/{fn.lstrip('/')}"
            )
            img = {"href": href, "type": "image/jpeg"}
            images.append(img)
            if "cover.medium" in ft:
                break
    if images:
        result["images"] = images

    return result


CROSSWALK_MAP = {
    Crosswalk.FULL: crosswalk_full,
    Crosswalk.MINI: crosswalk_mini,
    Crosswalk.PG: crosswalk_pg,
    Crosswalk.OPDS: crosswalk_opds,
}
