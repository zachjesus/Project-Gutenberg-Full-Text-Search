"""Full-text search using mv_books_dc materialized view with query builder."""
from __future__ import annotations
import html
import mimetypes
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker


__all__ = [
    "Config",
    "FullTextSearch",
    "SearchQuery",
    "SearchField",
    "SearchType",
    "OrderBy",
    "SortDirection",
    "FileType",
    "Encoding",
    "Crosswalk",
    "LanguageCode",
    "LoccClass",
    "LANGUAGE_LIST",
    "LOCC_LIST",
    "LOCC_HIERARCHY",
    "LANGUAGE_LABELS",
    "LOCC_LABELS",
    "CURATED_BOOKSHELVES",
    "get_locc_children",
    "get_locc_path",
    "get_broad_genres",
]


class Config:
    PGHOST = 'localhost'
    PGPORT = '5432'
    PGDATABASE = 'gutendb'
    PGUSER = 'postgres'


# =============================================================================
# Catalog vocabulary (OPDS facet labels)
# =============================================================================

LANGUAGE_LIST = [
    {'code': 'en', 'label': 'English'},
    {'code': 'af', 'label': 'Afrikaans'},
    {'code': 'ale', 'label': 'Aleut'},
    {'code': 'ang', 'label': 'Old English'},
    {'code': 'ar', 'label': 'Arabic'},
    {'code': 'arp', 'label': 'Arapaho'},
    {'code': 'bg', 'label': 'Bulgarian'},
    {'code': 'bgs', 'label': 'Basa Banyumasan'},
    {'code': 'bo', 'label': 'Tibetan'},
    {'code': 'br', 'label': 'Breton'},
    {'code': 'brx', 'label': 'Bodo'},
    {'code': 'ca', 'label': 'Catalan'},
    {'code': 'ceb', 'label': 'Cebuano'},
    {'code': 'cs', 'label': 'Czech'},
    {'code': 'csb', 'label': 'Kashubian'},
    {'code': 'cy', 'label': 'Welsh'},
    {'code': 'da', 'label': 'Danish'},
    {'code': 'de', 'label': 'German'},
    {'code': 'el', 'label': 'Greek'},
    {'code': 'enm', 'label': 'Middle English'},
    {'code': 'eo', 'label': 'Esperanto'},
    {'code': 'es', 'label': 'Spanish'},
    {'code': 'et', 'label': 'Estonian'},
    {'code': 'fa', 'label': 'Persian'},
    {'code': 'fi', 'label': 'Finnish'},
    {'code': 'fr', 'label': 'French'},
    {'code': 'fur', 'label': 'Friulian'},
    {'code': 'fy', 'label': 'Western Frisian'},
    {'code': 'ga', 'label': 'Irish'},
    {'code': 'gl', 'label': 'Galician'},
    {'code': 'gla', 'label': 'Scottish Gaelic'},
    {'code': 'grc', 'label': 'Ancient Greek'},
    {'code': 'hai', 'label': 'Haida'},
    {'code': 'he', 'label': 'Hebrew'},
    {'code': 'hu', 'label': 'Hungarian'},
    {'code': 'ia', 'label': 'Interlingua'},
    {'code': 'ilo', 'label': 'Iloko'},
    {'code': 'is', 'label': 'Icelandic'},
    {'code': 'it', 'label': 'Italian'},
    {'code': 'iu', 'label': 'Inuktitut'},
    {'code': 'ja', 'label': 'Japanese'},
    {'code': 'kha', 'label': 'Khasi'},
    {'code': 'kld', 'label': 'Klamath-Modoc'},
    {'code': 'ko', 'label': 'Korean'},
    {'code': 'la', 'label': 'Latin'},
    {'code': 'lt', 'label': 'Lithuanian'},
    {'code': 'mi', 'label': 'Māori'},
    {'code': 'myn', 'label': 'Mayan Languages'},
    {'code': 'nah', 'label': 'Nahuatl'},
    {'code': 'nai', 'label': 'North American Indian'},
    {'code': 'nap', 'label': 'Neapolitan'},
    {'code': 'nav', 'label': 'Navajo'},
    {'code': 'nl', 'label': 'Dutch'},
    {'code': 'no', 'label': 'Norwegian'},
    {'code': 'oc', 'label': 'Occitan'},
    {'code': 'oji', 'label': 'Ojibwa'},
    {'code': 'pl', 'label': 'Polish'},
    {'code': 'pt', 'label': 'Portuguese'},
    {'code': 'rmq', 'label': 'Romani'},
    {'code': 'ro', 'label': 'Romanian'},
    {'code': 'ru', 'label': 'Russian'},
    {'code': 'sa', 'label': 'Sanskrit'},
    {'code': 'sco', 'label': 'Scots'},
    {'code': 'sl', 'label': 'Slovenian'},
    {'code': 'sr', 'label': 'Serbian'},
    {'code': 'sv', 'label': 'Swedish'},
    {'code': 'te', 'label': 'Telugu'},
    {'code': 'tl', 'label': 'Tagalog'},
    {'code': 'yi', 'label': 'Yiddish'},
    {'code': 'zh', 'label': 'Chinese'},
]

LOCC_LIST = [
    {'code': 'A', 'label': 'General Works'},
    {'code': 'B', 'label': 'Philosophy, Psychology, Religion'},
    {'code': 'C', 'label': 'History: Auxiliary Sciences'},
    {'code': 'D', 'label': 'History: General and Eastern Hemisphere'},
    {'code': 'E', 'label': 'History: America'},
    {'code': 'F', 'label': 'History: America (Local)'},
    {'code': 'G', 'label': 'Geography, Anthropology, Recreation'},
    {'code': 'H', 'label': 'Social Sciences'},
    {'code': 'J', 'label': 'Political Science'},
    {'code': 'K', 'label': 'Law'},
    {'code': 'L', 'label': 'Education'},
    {'code': 'M', 'label': 'Music'},
    {'code': 'N', 'label': 'Fine Arts'},
    {'code': 'P', 'label': 'Language and Literature'},
    {'code': 'Q', 'label': 'Science'},
    {'code': 'R', 'label': 'Medicine'},
    {'code': 'S', 'label': 'Agriculture'},
    {'code': 'T', 'label': 'Technology'},
    {'code': 'U', 'label': 'Military Science'},
    {'code': 'V', 'label': 'Naval Science'},
    {'code': 'Z', 'label': 'Bibliography, Library Science'},
]

# Complete LOCC hierarchy with all subclasses for drill-down navigation
LOCC_HIERARCHY = {
    # A - General Works
    'A': 'General Works',
    'AC': 'Collections, Series, Collected works',
    'AE': 'Encyclopedias',
    'AG': 'Dictionaries and other general reference books',
    'AI': 'Indexes',
    'AM': 'Museums, Collectors and collecting',
    'AN': 'Newspapers',
    'AP': 'Periodicals',
    'AS': 'Academies and International Associations',
    'AY': 'Yearbooks, Almanacs, Directories',
    'AZ': 'History of scholarship and learning',
    # B - Philosophy, Psychology, Religion
    'B': 'Philosophy, Psychology, Religion',
    'BC': 'Logic',
    'BD': 'Speculative Philosophy',
    'BF': 'Psychology, Psychoanalysis',
    'BH': 'Aesthetics',
    'BJ': 'Ethics, Social usages, Etiquette',
    'BL': 'Religion: General, Miscellaneous',
    'BM': 'Judaism',
    'BP': 'Islam, Bahaism, Theosophy',
    'BQ': 'Buddhism',
    'BR': 'Christianity',
    'BS': 'The Bible',
    'BT': 'Doctrinal theology, Christology',
    'BV': 'Practical theology, Worship',
    'BX': 'Churches, Church movements',
    # C - History: Auxiliary Sciences
    'C': 'History: Auxiliary Sciences',
    'CB': 'History of civilization',
    'CC': 'Archaeology',
    'CD': 'Diplomatics, Archives, Seals',
    'CE': 'Technical Chronology, Calendar',
    'CJ': 'Numismatics',
    'CN': 'Inscriptions, Epigraphy',
    'CR': 'Heraldry',
    'CS': 'Genealogy',
    'CT': 'Biography',
    # D - History: General and Eastern Hemisphere
    'D': 'History: General and Eastern Hemisphere',
    'DA': 'Great Britain, Ireland',
    'DB': 'Austria, Hungary, Czech Republic',
    'DC': 'France, Andorra, Monaco',
    'DD': 'Germany',
    'DE': 'The Mediterranean, Greco-Roman World',
    'DF': 'Greece',
    'DG': 'Italy, Vatican City, Malta',
    'DH': 'Netherlands, Belgium, Luxemburg',
    'DJ': 'Netherlands',
    'DJK': 'Eastern Europe',
    'DK': 'Russia, Soviet Republics, Poland',
    'DL': 'Northern Europe, Scandinavia',
    'DP': 'Spain, Portugal',
    'DQ': 'Switzerland',
    'DR': 'Balkan Peninsula, Turkey',
    'DS': 'Asia',
    'DT': 'Africa',
    'DU': 'Oceania (South Seas)',
    'DX': 'History of Romanies',
    'D501': 'World War I (1914-1918)',
    'D731': 'World War II (1939-1945)',
    # E - History: America
    'E': 'History: America',
    'E011': 'America (General)',
    'E151': 'United States',
    'E186': 'Colonial History (1607-1775)',
    'E201': 'Revolution (1775-1783)',
    'E300': 'Revolution to Civil War (1783-1861)',
    'E456': 'Civil War period (1861-1865)',
    'E660': 'Late nineteenth century (1865-1900)',
    'E740': 'Twentieth century',
    'E838': 'Later twentieth century (1961-)',
    'E895': 'Twenty-first century',
    # F - United States and Americas Local History
    'F': 'United States and Americas Local History',
    'F001': 'New England',
    'F106': 'Atlantic coast, Middle Atlantic',
    'F206': 'The South, South Atlantic',
    'F296': 'Gulf States, West Florida',
    'F350.5': 'Mississippi River and Valley',
    'F396': 'Old Southwest, Lower Mississippi',
    'F476': 'Old Northwest, Northwest Territory',
    'F516': 'Ohio River and Valley',
    'F590.3': 'The West, Great Plains',
    'F721': 'Rocky Mountains, Yellowstone',
    'F786': 'New Southwest, Colorado River',
    'F850.5': 'Pacific States',
    'F965': 'Territories of the United States',
    'F970': 'Insular possessions',
    'F975': 'Central American affiliations',
    'F1001': 'Canada',
    'F1201': 'Mexico',
    'F1401': 'Latin America: General',
    'F1461': 'Guatemala',
    'F1481': 'El Salvador',
    'F1501': 'Honduras',
    'F1521': 'Nicaragua',
    'F1541': 'Costa Rica',
    'F1561': 'Panama',
    'F1601': 'West Indies',
    'F1751': 'Cuba',
    'F1861': 'Jamaica',
    'F1900': 'Hispaniola (Haiti, Dominican Republic)',
    'F1951': 'Puerto Rico',
    'F2001': 'Lesser Antilles',
    'F2131': 'British West Indies',
    'F2155': 'Caribbean area',
    'F2201': 'South America: General',
    'F2251': 'Colombia',
    'F2301': 'Venezuela',
    'F2351': 'Guiana',
    'F2501': 'Brazil',
    'F2661': 'Paraguay',
    'F2701': 'Uruguay',
    'F2801': 'Argentina',
    'F3051': 'Chile',
    'F3301': 'Bolivia',
    'F3401': 'Peru',
    'F3701': 'Ecuador',
    # G - Geography, Anthropology, Recreation
    'G': 'Geography, Anthropology, Recreation',
    'GA': 'Mathematical geography, Cartography',
    'GB': 'Physical geography',
    'GC': 'Oceanography',
    'GE': 'Environmental Sciences',
    'GF': 'Human ecology, Anthropogeography',
    'GN': 'Anthropology',
    'GR': 'Folklore',
    'GT': 'Manners and customs',
    'GV': 'Recreation, Leisure',
    # H - Social Sciences
    'H': 'Social Sciences',
    'HA': 'Statistics',
    'HB': 'Economic theory, Demography',
    'HC': 'Economic history and conditions',
    'HD': 'Economic history, Production',
    'HE': 'Transportation and communications',
    'HF': 'Commerce',
    'HG': 'Finance',
    'HJ': 'Public finance',
    'HM': 'Sociology',
    'HN': 'Social history, Social problems',
    'HQ': 'Family, Marriage, Sex and Gender',
    'HS': 'Societies: secret, benevolent, etc.',
    'HT': 'Communities, Classes, Races',
    'HV': 'Social pathology, Public Welfare',
    'HX': 'Socialism, Communism, Anarchism',
    # J - Political Science
    'J': 'Political Science',
    'JA': 'Political science (General)',
    'JC': 'Political theory',
    'JF': 'Political institutions, Public admin',
    'JK': 'Political inst.: United States',
    'JL': 'Political inst.: America',
    'JN': 'Political inst.: Europe',
    'JQ': 'Political inst.: Asia, Africa, Oceania',
    'JS': 'Local government, Municipal',
    'JV': 'Colonies, International migration',
    'JX': 'International law',
    'JZ': 'International relations',
    # K - Law
    'K': 'Law',
    'KBM': 'Jewish law',
    'KBP': 'Islamic law',
    'KBR': 'History of canon law',
    'KBU': 'Roman Catholic Church law',
    'KD': 'United Kingdom and Ireland',
    'KDZ': 'America, North America',
    'KE': 'Canada',
    'KF': 'United States',
    'KG': 'Latin America, Mexico, Central America',
    'KH': 'South America',
    'KJ': 'Europe',
    'KL': 'Asia, Eurasia, Africa, Pacific',
    'KLA': 'Russia, Soviet Union',
    'KM': 'Asia',
    'KN': 'South Asia, Southeast Asia, East Asia',
    'KNN': 'China',
    'KNX': 'Japan',
    'KP': 'South Asia, Southeast Asia',
    'KQ': 'Africa',
    'KR': 'Africa (continued)',
    'KS': 'Africa (continued)',
    'KT': 'Africa (continued)',
    'KU': 'Pacific area, Australia',
    'KV': 'Pacific area jurisdictions',
    'KWX': 'Antarctica',
    'KZ': 'Law of nations',
    'KZA': 'Law of the sea',
    'KZD': 'Space law',
    # L - Education
    'L': 'Education',
    'LA': 'History of education',
    'LB': 'Theory and practice',
    'LC': 'Special aspects of education',
    'LD': 'Institutions: United States',
    'LE': 'Institutions: America (except US)',
    'LF': 'Institutions: Europe',
    'LG': 'Institutions: Asia, Africa, Oceania',
    'LH': 'College magazines and papers',
    'LJ': 'Student fraternities, United States',
    'LT': 'Textbooks',
    # M - Music
    'M': 'Music',
    'ML': 'Literature of music',
    'MT': 'Musical instruction and study',
    # N - Fine Arts
    'N': 'Fine Arts',
    'NA': 'Architecture',
    'NB': 'Sculpture',
    'NC': 'Drawing, Design, Illustration',
    'ND': 'Painting',
    'NE': 'Print media',
    'NK': 'Decorative and Applied Arts',
    'NX': 'Arts in general',
    # P - Language and Literature
    'P': 'Language and Literature',
    'PA': 'Classical Languages and Literature',
    'PB': 'General works',
    'PC': 'Romance languages',
    'PD': 'Germanic and Scandinavian',
    'PE': 'English',
    'PF': 'West Germanic',
    'PG': 'Slavic, Russian',
    'PH': 'Finno-Ugrian and Basque',
    'PJ': 'Oriental languages',
    'PK': 'Indo-Iranian literatures',
    'PL': 'Eastern Asia, Africa, Oceania',
    'PM': 'Indigenous and Artificial Languages',
    'PN': 'Literature: General, Criticism',
    'PQ': 'Romance literatures',
    'PR': 'English literature',
    'PS': 'American and Canadian literature',
    'PT': 'Germanic, Scandinavian, Icelandic',
    'PZ': 'Juvenile belles lettres',
    # Q - Science
    'Q': 'Science',
    'QA': 'Mathematics',
    'QB': 'Astronomy',
    'QC': 'Physics',
    'QD': 'Chemistry',
    'QE': 'Geology',
    'QH': 'Natural history',
    'QH301': 'Biology',
    'QK': 'Botany',
    'QL': 'Zoology',
    'QM': 'Human anatomy',
    'QP': 'Physiology',
    'QR': 'Microbiology',
    # R - Medicine
    'R': 'Medicine',
    'RA': 'Public aspects of medicine',
    'RB': 'Pathology',
    'RC': 'Internal medicine',
    'RD': 'Surgery',
    'RE': 'Ophthalmology',
    'RF': 'Otorhinolaryngology',
    'RG': 'Gynecology and obstetrics',
    'RJ': 'Pediatrics',
    'RK': 'Dentistry',
    'RL': 'Dermatology',
    'RM': 'Therapeutics, Pharmacology',
    'RS': 'Pharmacy and materia medica',
    'RT': 'Nursing',
    'RV': 'Botanic, Thomsonian medicine',
    'RX': 'Homeopathy',
    'RZ': 'Other systems of medicine',
    # S - Agriculture
    'S': 'Agriculture',
    'SB': 'Plant culture',
    'SD': 'Forestry',
    'SF': 'Animal culture',
    'SH': 'Aquaculture, Fisheries, Angling',
    'SK': 'Hunting sports',
    # T - Technology
    'T': 'Technology',
    'TA': 'Engineering, Civil engineering',
    'TC': 'Ocean engineering',
    'TD': 'Environmental technology',
    'TE': 'Highway engineering',
    'TF': 'Railroad engineering',
    'TG': 'Bridge engineering',
    'TH': 'Building construction',
    'TJ': 'Mechanical engineering',
    'TK': 'Electrical, Electronics, Nuclear',
    'TL': 'Motor vehicles, Aeronautics',
    'TN': 'Mining engineering, Metallurgy',
    'TP': 'Chemical technology',
    'TR': 'Photography',
    'TS': 'Manufactures',
    'TT': 'Handicrafts, Arts and crafts',
    'TX': 'Home economics',
    # U - Military Science
    'U': 'Military Science',
    'UA': 'Armies: Organization, distribution',
    'UB': 'Military administration',
    'UC': 'Maintenance and transportation',
    'UD': 'Infantry',
    'UE': 'Cavalry, Armor',
    'UF': 'Artillery',
    'UG': 'Military engineering',
    'UH': 'Other services',
    # V - Naval Science
    'V': 'Naval Science',
    'VA': 'Navies: Organization, distribution',
    'VB': 'Naval administration',
    'VC': 'Naval maintenance',
    'VD': 'Naval seamen',
    'VE': 'Marines',
    'VF': 'Naval ordnance',
    'VG': 'Minor services of navies',
    'VK': 'Navigation, Merchant marine',
    'VM': 'Naval architecture, Shipbuilding',
    # Z - Bibliography, Library Science
    'Z': 'Bibliography, Library Science',
    'ZA': 'Information resources',
}

LANGUAGE_LABELS = {i["code"]: i["label"] for i in LANGUAGE_LIST}
LOCC_LABELS = {i["code"]: i["label"] for i in LOCC_LIST}

# Curated bookshelves organized by category (from Gutenberg.org)
CURATED_BOOKSHELVES = {
    "Literature": [
        {"id": 644, "name": "Adventure"},
        {"id": 654, "name": "American Literature"},
        {"id": 653, "name": "British Literature"},
        {"id": 652, "name": "French Literature"},
        {"id": 651, "name": "German Literature"},
        {"id": 650, "name": "Russian Literature"},
        {"id": 649, "name": "Classics of Literature"},
        {"id": 643, "name": "Biographies"},
        {"id": 645, "name": "Novels"},
        {"id": 634, "name": "Short Stories"},
        {"id": 637, "name": "Poetry"},
        {"id": 642, "name": "Plays/Films/Dramas"},
        {"id": 639, "name": "Romance"},
        {"id": 638, "name": "Science-Fiction & Fantasy"},
        {"id": 640, "name": "Crime, Thrillers & Mystery"},
        {"id": 646, "name": "Mythology, Legends & Folklore"},
        {"id": 641, "name": "Humour"},
        {"id": 636, "name": "Children & Young Adult Reading"},
        {"id": 633, "name": "Literature - Other"},
    ],
    "Science & Technology": [
        {"id": 671, "name": "Engineering & Technology"},
        {"id": 672, "name": "Mathematics"},
        {"id": 667, "name": "Science - Physics"},
        {"id": 668, "name": "Science - Chemistry/Biochemistry"},
        {"id": 669, "name": "Science - Biology"},
        {"id": 670, "name": "Science - Earth/Agricultural/Farming"},
        {"id": 673, "name": "Research Methods/Statistics/Info Sys"},
        {"id": 685, "name": "Environmental Issues"},
    ],
    "History": [
        {"id": 656, "name": "History - American"},
        {"id": 657, "name": "History - British"},
        {"id": 658, "name": "History - European"},
        {"id": 659, "name": "History - Ancient"},
        {"id": 660, "name": "History - Medieval/Middle Ages"},
        {"id": 661, "name": "History - Early Modern (c. 1450-1750)"},
        {"id": 662, "name": "History - Modern (1750+)"},
        {"id": 663, "name": "History - Religious"},
        {"id": 664, "name": "History - Royalty"},
        {"id": 665, "name": "History - Warfare"},
        {"id": 666, "name": "History - Schools & Universities"},
        {"id": 655, "name": "History - Other"},
        {"id": 686, "name": "Archaeology & Anthropology"},
    ],
    "Social Sciences & Society": [
        {"id": 695, "name": "Business/Management"},
        {"id": 696, "name": "Economics"},
        {"id": 689, "name": "Law & Criminology"},
        {"id": 690, "name": "Gender & Sexuality Studies"},
        {"id": 688, "name": "Psychiatry/Psychology"},
        {"id": 693, "name": "Sociology"},
        {"id": 694, "name": "Politics"},
        {"id": 701, "name": "Parenthood & Family Relations"},
        {"id": 700, "name": "Old Age & the Elderly"},
    ],
    "Arts & Culture": [
        {"id": 675, "name": "Art"},
        {"id": 674, "name": "Architecture"},
        {"id": 677, "name": "Music"},
        {"id": 676, "name": "Fashion"},
        {"id": 698, "name": "Journalism/Media/Writing"},
        {"id": 687, "name": "Language & Communication"},
        {"id": 647, "name": "Essays, Letters & Speeches"},
    ],
    "Religion & Philosophy": [
        {"id": 692, "name": "Religion/Spirituality"},
        {"id": 691, "name": "Philosophy & Ethics"},
    ],
    "Lifestyle & Hobbies": [
        {"id": 678, "name": "Cooking & Drinking"},
        {"id": 680, "name": "Sports/Hobbies"},
        {"id": 679, "name": "How To ..."},
        {"id": 648, "name": "Travel Writing"},
        {"id": 683, "name": "Nature/Gardening/Animals"},
        {"id": 703, "name": "Sexuality & Erotica"},
    ],
    "Health & Medicine": [
        {"id": 681, "name": "Health & Medicine"},
        {"id": 682, "name": "Drugs/Alcohol/Pharmacology"},
        {"id": 684, "name": "Nutrition"},
    ],
    "Education & Reference": [
        {"id": 697, "name": "Encyclopedias/Dictionaries/Reference"},
        {"id": 704, "name": "Teaching & Education"},
        {"id": 702, "name": "Reports & Conference Proceedings"},
        {"id": 699, "name": "Journals"},
    ],
}


def get_locc_children(parent: str = "") -> list[dict]:
    """
    Get immediate children of a LOCC code for hierarchical navigation.
    
    Args:
        parent: Parent LOCC code (empty string for top-level classes)
        
    Returns:
        List of dicts with 'code', 'label', and 'has_children' keys
        
    Hierarchy structure:
    - Level 0 (root): Single letters (A, B, C, ...)
    - Level 1: Two-letter codes (AC, AE, BC, ...) - immediate children of single letters
    - Level 2+: Letter codes with numbers (E011, D501, QH301, F350.5, ...)
    """
    parent = (parent or "").strip().upper()
    
    if not parent:
        # Root level: return all single-letter codes
        children = [c for c in LOCC_HIERARCHY.keys() if len(c) == 1 and c.isalpha()]
        return [
            {
                'code': code,
                'label': LOCC_HIERARCHY[code],
                'has_children': any(c.startswith(code) and c != code for c in LOCC_HIERARCHY.keys())
            }
            for code in sorted(children)
        ]
    
    # Extract letter prefix from parent
    parent_match = re.match(r'^([A-Z]+)', parent)
    if not parent_match:
        return []
    
    parent_letters = parent_match.group(1)
    parent_has_numbers = len(parent) > len(parent_letters)
    
    # Find all candidates that start with parent but aren't the parent itself
    candidates = [c for c in LOCC_HIERARCHY.keys() if c.startswith(parent) and c != parent]
    
    if not candidates:
        return []  # Leaf node - no children
    
    children = set()
    
    for code in candidates:
        # Extract letter prefix from this code
        code_match = re.match(r'^([A-Z]+)', code)
        if not code_match:
            continue
        
        code_letters = code_match.group(1)
        code_has_numbers = len(code) > len(code_letters)
        
        if parent_has_numbers:
            # Parent has numbers (e.g., E011) - not typical, but handle it
            # Direct children would be longer numbered codes
            children.add(code)
        elif len(parent_letters) == 1:
            # Parent is a single letter (e.g., A, B, E)
            if not code_has_numbers:
                # Two-letter codes are immediate children (e.g., AC, AE under A)
                if len(code_letters) == 2 and code_letters.startswith(parent):
                    children.add(code)
            else:
                # Numbered codes: only add if letter prefix matches exactly
                # E.g., E011 is under E, not EA
                if code_letters == parent:
                    children.add(code)
        else:
            # Parent is a multi-letter code (e.g., AC, DJK)
            if not code_has_numbers:
                # Longer letter codes are children (e.g., DJK under D if DJ doesn't exist)
                if code_letters.startswith(parent_letters) and len(code_letters) == len(parent_letters) + 1:
                    children.add(code)
            else:
                # Numbered codes under this letter prefix
                if code_letters == parent_letters:
                    children.add(code)
    
    # For numbered codes, filter to only show "top-level" numbered codes
    # (i.e., don't show F350.5 if we should show F350 first)
    numbered_children = [c for c in children if any(ch.isdigit() for ch in c)]
    if numbered_children:
        # Sort by length, then alphabetically
        numbered_children.sort(key=lambda x: (len(x), x))
        # Keep only those that aren't prefixed by another numbered child
        filtered_numbered = []
        for code in numbered_children:
            is_sub = False
            for other in numbered_children:
                if other != code and code.startswith(other):
                    is_sub = True
                    break
            if not is_sub:
                filtered_numbered.append(code)
        # Replace numbered children with filtered set
        children = {c for c in children if not any(ch.isdigit() for ch in c)}
        children.update(filtered_numbered)
    
    # Build result with has_children flag
    result = []
    for code in sorted(children, key=lambda x: (len(x), x)):
        has_children = any(c.startswith(code) and c != code for c in LOCC_HIERARCHY.keys())
        result.append({
            'code': code,
            'label': LOCC_HIERARCHY.get(code, code),
            'has_children': has_children
        })
    
    return result


def get_locc_path(code: str) -> list[dict]:
    """
    Get the path from root to a LOCC code (breadcrumb navigation).
    
    Args:
        code: LOCC code
        
    Returns:
        List of dicts with 'code' and 'label' from root to the given code
    """
    code = (code or "").strip().upper()
    if not code:
        return []
    
    path = []
    
    # Extract letter prefix
    match = re.match(r'^([A-Z]+)', code)
    if not match:
        return []
    
    letters = match.group(1)
    
    # Add single letter (top level) if it exists
    if letters[0] in LOCC_HIERARCHY:
        path.append({'code': letters[0], 'label': LOCC_HIERARCHY[letters[0]]})
    
    # Add intermediate letter codes (two-letter, three-letter, etc.)
    for i in range(2, len(letters) + 1):
        prefix = letters[:i]
        if prefix in LOCC_HIERARCHY:
            path.append({'code': prefix, 'label': LOCC_HIERARCHY[prefix]})
    
    # Add the full code if it has numbers and is different from the letter-only path
    if code != letters and code in LOCC_HIERARCHY:
        path.append({'code': code, 'label': LOCC_HIERARCHY[code]})
    elif code not in LOCC_HIERARCHY and letters in LOCC_HIERARCHY:
        # Code not in hierarchy but letters are - still valid for books
        # Add the code itself as the final path element
        path.append({'code': code, 'label': code})
    
    return path


def get_broad_genres(session=None) -> list[dict]:
    """
    Get broad genres (top-level LoCC) with book counts from base tables.
    
    Args:
        session: Optional SQLAlchemy session. If not provided, creates a temporary one.
    
    Returns:
        List of dicts with 'code', 'label', and 'book_count' keys
        
    Note: Uses base tables (loccs, mn_books_loccs) directly for accurate counts.
    """
    # Query base tables for accurate counts by top-level LoCC
    sql = """
        SELECT 
            SUBSTRING(lc.pk FROM 1 FOR 1) AS broad_genre,
            COUNT(DISTINCT mblc.fk_books) AS book_count
        FROM loccs lc
        JOIN mn_books_loccs mblc ON lc.pk = mblc.fk_loccs
        GROUP BY SUBSTRING(lc.pk FROM 1 FOR 1)
        ORDER BY broad_genre
    """
    
    # Use provided session or create a temporary one
    if session is not None:
        rows = session.execute(text(sql)).fetchall()
    else:
        # Create a temporary connection using module-level config
        cfg = Config()
        engine = create_engine(
            f"postgresql://{cfg.PGUSER}@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.PGDATABASE}",
            pool_pre_ping=True,
        )
        Session = sessionmaker(bind=engine)
        with Session() as temp_session:
            rows = temp_session.execute(text(sql)).fetchall()
    
    # Map to our labels
    result = []
    for row in rows:
        code = row.broad_genre
        if code in LOCC_HIERARCHY:
            result.append({
                'code': code,
                'label': LOCC_HIERARCHY[code],
                'book_count': row.book_count
            })
    
    return result


# =============================================================================
# Enums
# =============================================================================

class LanguageCode(str, Enum):
    """Gutenberg language codes (mirrors OPDS facet options)."""
    EN = "en"
    AF = "af"
    ALE = "ale"
    ANG = "ang"
    AR = "ar"
    ARP = "arp"
    BG = "bg"
    BGS = "bgs"
    BO = "bo"
    BR = "br"
    BRX = "brx"
    CA = "ca"
    CEB = "ceb"
    CS = "cs"
    CSB = "csb"
    CY = "cy"
    DA = "da"
    DE = "de"
    EL = "el"
    ENM = "enm"
    EO = "eo"
    ES = "es"
    ET = "et"
    FA = "fa"
    FI = "fi"
    FR = "fr"
    FUR = "fur"
    FY = "fy"
    GA = "ga"
    GL = "gl"
    GLA = "gla"
    GRC = "grc"
    HAI = "hai"
    HE = "he"
    HU = "hu"
    IA = "ia"
    ILO = "ilo"
    IS = "is"
    IT = "it"
    IU = "iu"
    JA = "ja"
    KHA = "kha"
    KLD = "kld"
    KO = "ko"
    LA = "la"
    LT = "lt"
    MI = "mi"
    MYN = "myn"
    NAH = "nah"
    NAI = "nai"
    NAP = "nap"
    NAV = "nav"
    NL = "nl"
    NO = "no"
    OC = "oc"
    OJI = "oji"
    PL = "pl"
    PT = "pt"
    RMQ = "rmq"
    RO = "ro"
    RU = "ru"
    SA = "sa"
    SCO = "sco"
    SL = "sl"
    SR = "sr"
    SV = "sv"
    TE = "te"
    TL = "tl"
    YI = "yi"
    ZH = "zh"


class LoccClass(str, Enum):
    """Library of Congress Classification top-level classes (mirrors OPDS facet options)."""
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"
    H = "H"
    J = "J"
    K = "K"
    L = "L"
    M = "M"
    N = "N"
    P = "P"
    Q = "Q"
    R = "R"
    S = "S"
    T = "T"
    U = "U"
    V = "V"
    Z = "Z"


class FileType(str, Enum):
    """MIME types for file filtering."""
    EPUB = "application/epub+zip"
    KINDLE = "application/x-mobipocket-ebook"
    PDF = "application/pdf"
    TXT = "text/plain"
    HTML = "text/html"


class Encoding(str, Enum):
    """Character encodings for file filtering."""
    ASCII = "us-ascii"
    UTF8 = "utf-8"
    LATIN1 = "iso-8859-1"
    WINDOWS1252 = "windows-1252"


class SearchType(str, Enum):
    """Search algorithm types."""
    FTS = "fts"
    FUZZY = "fuzzy"
    CONTAINS = "contains"


class SearchField(str, Enum):
    """Searchable fields."""
    BOOK = "book"
    TITLE = "title"
    AUTHOR = "author"
    SUBJECT = "subject"
    BOOKSHELF = "bookshelf"
    SUBTITLE = "subtitle"
    ATTRIBUTE = "attribute"


class OrderBy(str, Enum):
    """Sort options."""
    RELEVANCE = "relevance"
    DOWNLOADS = "downloads"
    TITLE = "title"
    AUTHOR = "author"
    RELEASE_DATE = "release_date"
    RANDOM = "random"


class SortDirection(str, Enum):
    """Sort direction."""
    ASC = "asc"
    DESC = "desc"


class Crosswalk(str, Enum):
    """Output format transformers."""
    FULL = "full"
    PG = "pg"
    OPDS = "opds"
    CUSTOM = "custom"
    MINI = "mini"


# =============================================================================
# Internal Configuration
# =============================================================================

_FIELD_COLS = {
    SearchField.BOOK:      ("tsvec",           "book_text"),
    SearchField.TITLE:     ("title_tsvec",     "title"),
    SearchField.SUBTITLE:  ("subtitle_tsvec",  "subtitle"),
    SearchField.AUTHOR:    ("author_tsvec",    "all_authors"),
    SearchField.SUBJECT:   ("subject_tsvec",   "all_subjects"),
    SearchField.BOOKSHELF: ("bookshelf_tsvec", "bookshelf_text"),
    SearchField.ATTRIBUTE: ("attribute_tsvec", "attribute_text"),
}

_TRIGRAM_FIELDS = {
    SearchField.BOOK, SearchField.TITLE, SearchField.SUBTITLE,
    SearchField.AUTHOR, SearchField.SUBJECT, SearchField.BOOKSHELF,
}

_ORDER_COLUMNS = {
    OrderBy.DOWNLOADS: ("downloads", SortDirection.DESC, None),
    OrderBy.TITLE: ("title", SortDirection.ASC, None),
    OrderBy.AUTHOR: ("all_authors", SortDirection.ASC, "LAST"),
    OrderBy.RELEASE_DATE: ("release_date", SortDirection.DESC, "LAST"),
    OrderBy.RANDOM: ("RANDOM()", None, None),
}

_SELECT = "book_id, title, all_authors, downloads, dc"

_SUBQUERY = """book_id, title, all_authors, all_subjects, downloads, release_date, dc,
    copyrighted, lang_codes, is_audio,
    max_author_birthyear, min_author_birthyear,
    max_author_deathyear, min_author_deathyear,
    locc_codes,
    tsvec, title_tsvec, subtitle_tsvec, author_tsvec, subject_tsvec, bookshelf_tsvec, attribute_tsvec,
    book_text, bookshelf_text, attribute_text, subtitle"""

# Accepted filetypes for OPDS output
_OPDS_FILETYPES = {
    "epub.images", "epub.noimages", "epub3.images",
    "kf8.images", "kindle.images", "kindle.noimages",
    "pdf", "index",
}


# =============================================================================
# Crosswalk Functions
# =============================================================================

def _crosswalk_full(row) -> dict[str, Any]:
    return {
        "book_id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads,
        "dc": row.dc
    }


def _crosswalk_mini(row) -> dict[str, Any]:
    return {
        "id": row.book_id,
        "title": row.title,
        "author": row.all_authors,
        "downloads": row.downloads
    }


def _crosswalk_pg(row) -> dict[str, Any]:
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
        "bookshelves": [b["bookshelf"] for b in dc.get("bookshelves", []) if b.get("bookshelf")],
        "release_date": dc.get("date"),
        "downloads_last_30_days": row.downloads,
        "files": [
            {"filename": f.get("filename"), "type": f.get("mediatype"), "size": f.get("extent")}
            for f in dc.get("format", []) if f.get("filename")
        ],
        "cover_url": (dc.get("coverpage") or [None])[0],
    }


def _crosswalk_opds(row) -> dict[str, Any]:
    """Transform row to OPDS 2.0 publication format per spec."""
    dc = row.dc or {}
    
    # Build metadata - spec says no blank values (null, "", [], {})
    metadata = {
        "@type": "http://schema.org/Book",
        "identifier": f"urn:gutenberg:{row.book_id}",
        "title": row.title,
        "language": (dc.get("language") or [{}])[0].get("code") or "en",
    }
    
    # Author (can be string or object with name/sortAs)
    creators = dc.get("creators", [])
    if creators and creators[0].get("name"):
        p = creators[0]
        author = {"name": p["name"], "sortAs": p["name"]}
        if p.get("id"):
            author["identifier"] = f"https://www.gutenberg.org/ebooks/author/{p['id']}"
        metadata["author"] = author
    
    # Published date
    if dc.get("date"):
        metadata["published"] = dc["date"]
    
    # Modified date (from MARC 508 field)
    for m in dc.get("marc", []):
        if m.get("code") == 508 and "Updated:" in (m.get("text") or ""):
            try:
                modified = m["text"].split("Updated:")[1].strip().split()[0].rstrip(".")
                if modified:
                    metadata["modified"] = modified
            except (IndexError, AttributeError):
                pass
            break
    
    # Description
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
        metadata["description"] = "<p>" + "</p><p>".join(html.escape(p) for p in desc_parts) + "</p>"
    
    # Subjects
    subjects = [s["subject"] for s in dc.get("subjects", []) if s.get("subject")]
    subjects += [c["locc"] for c in dc.get("coverage", []) if c.get("locc")]
    if subjects:
        metadata["subject"] = subjects
    
    # Publisher
    if pub_raw := (dc.get("publisher") or {}).get("raw"):
        metadata["publisher"] = pub_raw
    
    # Collections (belongsTo)
    collections = []
    for b in dc.get("bookshelves", []):
        if b.get("bookshelf"):
            collections.append({"name": b["bookshelf"], "identifier": f"https://www.gutenberg.org/ebooks/bookshelf/{b.get('id', '')}"})
    for c in dc.get("coverage", []):
        if c.get("locc"):
            collections.append({"name": c["locc"], "identifier": f"https://www.gutenberg.org/ebooks/locc/{c.get('id', '')}"})
    if collections:
        metadata["belongsTo"] = {"collection": collections}
    
    # Links - must have at least one acquisition link
    links = []
    
    # Acquisition links
    for f in dc.get("format", []):
        fn = f.get("filename")
        if not fn:
            continue
        ftype = (f.get("filetype") or "").strip().lower()
        if ftype not in _OPDS_FILETYPES:
            continue
        
        href = fn if fn.startswith(("http://", "https://")) else f"https://www.gutenberg.org/{fn.lstrip('/')}"
        mtype = (f.get("mediatype") or "").strip() or mimetypes.guess_type(fn)[0] or "application/octet-stream"
        
        link = {"rel": "http://opds-spec.org/acquisition/open-access", "href": href, "type": mtype}
        if f.get("extent") is not None and f["extent"] > 0:
            link["length"] = f["extent"]
        if f.get("hr_filetype"):
            link["title"] = f["hr_filetype"]
        links.append(link)
    
    # Build result
    result = {"metadata": metadata, "links": links}
    
    # Images collection (should contain at least one jpeg/png/gif/avif)
    images = []
    for f in dc.get("format", []):
        ft = f.get("filetype") or ""
        fn = f.get("filename")
        if fn and ("cover.medium" in ft or ("cover" in ft and not images)):
            href = fn if fn.startswith(("http://", "https://")) else f"https://www.gutenberg.org/{fn.lstrip('/')}"
            img = {"href": href, "type": "image/jpeg"}
            images.append(img)
            if "cover.medium" in ft:
                break
    if images:
        result["images"] = images
    
    return result


_CROSSWALK_MAP = {
    Crosswalk.FULL: _crosswalk_full,
    Crosswalk.MINI: _crosswalk_mini,
    Crosswalk.PG: _crosswalk_pg,
    Crosswalk.OPDS: _crosswalk_opds,
    Crosswalk.CUSTOM: _crosswalk_full,
}


# =============================================================================
# SearchQuery
# =============================================================================

@dataclass
class SearchQuery:
    """Fluent query builder for full-text search."""
    
    _search: list[tuple[str, dict, str]] = field(default_factory=list)
    _filter: list[tuple[str, dict]] = field(default_factory=list)
    _order: OrderBy = OrderBy.RELEVANCE
    _sort_dir: SortDirection | None = None
    _page: int = 1
    _page_size: int = 28
    _crosswalk: Crosswalk = Crosswalk.FULL
    
    # === Magic Methods ===
    
    def __getitem__(self, key: int | tuple) -> SearchQuery:
        """Set pagination: q[3] for page 3, q[2, 50] for page 2 with 50 results."""
        if isinstance(key, tuple):
            self._page = max(1, int(key[0]))
            self._page_size = max(1, min(100, int(key[1])))
        else:
            self._page = max(1, int(key))
        return self
    
    def __len__(self) -> int:
        return len(self._search) + len(self._filter)
    
    def __bool__(self) -> bool:
        return bool(self._search or self._filter)
    
    # === Configuration ===
    
    def crosswalk(self, cw: Crosswalk) -> SearchQuery:
        self._crosswalk = cw
        return self
    
    def order_by(self, order: OrderBy, direction: SortDirection | None = None) -> SearchQuery:
        self._order = order
        self._sort_dir = direction
        return self
    
    # === Search Methods ===
    
    def search(self, txt: str, field: SearchField = SearchField.BOOK, search_type: SearchType = SearchType.FTS) -> SearchQuery:
        """
        Add search condition. Supports:
        
        FTS mode (default): Uses PostgreSQL websearch_to_tsquery which supports:
          - "exact phrase" for phrase matching
          - word1 word2 for AND (default)
          - word1 or word2 for OR
          - -word for NOT/exclude
          
        FUZZY mode: Uses trigram similarity with basic boolean support:
          - "exact phrase" for exact substring match
          - word1 word2 for AND (all must match)
          - -word for NOT/exclude
          
        CONTAINS mode: Simple ILIKE substring match
        """
        txt = (txt or "").strip()
        if not txt:
            return self
        
        fts_col, text_col = _FIELD_COLS[field]
        use_trigram = field in _TRIGRAM_FIELDS
        
        if search_type == SearchType.FTS or not use_trigram:
            # websearch_to_tsquery handles "phrases", or, and - natively
            sql = f"{fts_col} @@ websearch_to_tsquery('english', :q)"
            self._search.append((sql, {"q": txt}, fts_col))
        elif search_type == SearchType.FUZZY:
            # Parse query for basic boolean support in fuzzy mode
            conditions, params = self._parse_fuzzy_query(txt, text_col)
            if conditions:
                self._search.append((conditions, params, text_col))
        else:  # CONTAINS
            self._search.append((f"{text_col} ILIKE :q", {"q": f"%{txt}%"}, text_col))
        return self
    
    def _parse_fuzzy_query(self, txt: str, text_col: str) -> tuple[str, dict]:
        """
        Parse query string for fuzzy search with basic boolean support.
        
        Supports:
          - "exact phrase" → ILIKE exact match
          - -word → NOT similarity match
          - word1 word2 → AND (all must fuzzy match)
        """
        original_txt = txt  # Keep for ranking
        
        # Extract quoted phrases
        phrases = re.findall(r'"([^"]+)"', txt)
        txt = re.sub(r'"[^"]*"', '', txt)
        
        # Extract negations
        negations = re.findall(r'-(\S+)', txt)
        txt = re.sub(r'-\S+', '', txt)
        
        # Remaining words (AND logic)
        words = txt.split()
        
        conditions = []
        params = {"q": original_txt}  # Keep original for ranking in _order_sql
        param_idx = 0
        
        # Quoted phrases: exact ILIKE match
        for phrase in phrases:
            phrase = phrase.strip()
            if phrase:
                param_name = f"phrase_{param_idx}"
                conditions.append(f"{text_col} ILIKE :{param_name}")
                params[param_name] = f"%{phrase}%"
                param_idx += 1
        
        # Regular words: fuzzy similarity (AND)
        for word in words:
            word = word.strip()
            if word and word.lower() not in ('or', 'and'):
                param_name = f"word_{param_idx}"
                conditions.append(f":{param_name} <% {text_col}")
                params[param_name] = word
                param_idx += 1
        
        # Negations: NOT similarity match
        for neg in negations:
            neg = neg.strip()
            if neg:
                param_name = f"neg_{param_idx}"
                conditions.append(f"NOT ({text_col} ILIKE :{param_name})")
                params[param_name] = f"%{neg}%"
                param_idx += 1
        
        if not conditions:
            # Fallback: simple fuzzy match on original text
            return f":q <% {text_col}", {"q": original_txt}
        
        return " AND ".join(conditions), params
    
    # === Filter Methods ===
    
    def etext(self, nr: int) -> SearchQuery:
        self._filter.append(("book_id = :id", {"id": int(nr)}))
        return self
    
    def etexts(self, nrs: list[int]) -> SearchQuery:
        self._filter.append(("book_id = ANY(:ids)", {"ids": [int(n) for n in nrs]}))
        return self
    
    def downloads_gte(self, n: int) -> SearchQuery:
        self._filter.append(("downloads >= :dl", {"dl": int(n)}))
        return self
    
    def downloads_lte(self, n: int) -> SearchQuery:
        self._filter.append(("downloads <= :dl", {"dl": int(n)}))
        return self
    
    def public_domain(self) -> SearchQuery:
        self._filter.append(("copyrighted = 0", {}))
        return self
    
    def copyrighted(self) -> SearchQuery:
        self._filter.append(("copyrighted = 1", {}))
        return self
    
    def lang(self, code: str | LanguageCode) -> SearchQuery:
        """Filter by language code (matches any language in multi-language books)."""
        code_val = code.value if isinstance(code, Enum) else str(code)
        # Use array containment to leverage the GIN index on lang_codes.
        self._filter.append(("lang_codes @> ARRAY[CAST(:lang AS text)]", {"lang": code_val}))
        return self
    
    def text_only(self) -> SearchQuery:
        self._filter.append(("is_audio = false", {}))
        return self
    
    def audiobook(self) -> SearchQuery:
        self._filter.append(("is_audio = true", {}))
        return self
    
    def author_born_after(self, year: int) -> SearchQuery:
        self._filter.append(("max_author_birthyear >= :y", {"y": int(year)}))
        return self
    
    def author_born_before(self, year: int) -> SearchQuery:
        self._filter.append(("min_author_birthyear <= :y", {"y": int(year)}))
        return self
    
    def author_died_after(self, year: int) -> SearchQuery:
        self._filter.append(("max_author_deathyear >= :y", {"y": int(year)}))
        return self
    
    def author_died_before(self, year: int) -> SearchQuery:
        self._filter.append(("min_author_deathyear <= :y", {"y": int(year)}))
        return self
    
    def released_after(self, date: str) -> SearchQuery:
        self._filter.append(("release_date >= CAST(:d AS date)", {"d": str(date)}))
        return self
    
    def released_before(self, date: str) -> SearchQuery:
        self._filter.append(("release_date <= CAST(:d AS date)", {"d": str(date)}))
        return self
    
    def locc(self, code: str | LoccClass) -> SearchQuery:
        """Filter by LoCC code (prefix match for top-level codes like 'E', 'F').
        Uses MN table (mn_books_loccs) for fast indexed lookups."""
        code_val = code.value if isinstance(code, Enum) else str(code)
        # Use MN table join instead of array unnest - much faster with proper indexes
        self._filter.append((
            "EXISTS (SELECT 1 FROM mn_books_loccs mbl JOIN loccs lc ON lc.pk = mbl.fk_loccs WHERE mbl.fk_books = book_id AND lc.pk LIKE :locc_pattern)",
            {"locc_pattern": f"{code_val}%"}
        ))
        return self
    
    def has_contributor(self, role: str) -> SearchQuery:
        self._filter.append(("dc->'creators' @> CAST(:j AS jsonb)", {"j": f'[{{"role":"{role}"}}]'}))
        return self
    
    def file_type(self, ft: FileType) -> SearchQuery:
        self._filter.append(("dc->'format' @> CAST(:ft AS jsonb)", {"ft": f'[{{"mediatype":"{ft.value}"}}]'}))
        return self
    
    def author_id(self, aid: int) -> SearchQuery:
        self._filter.append(("dc->'creators' @> CAST(:aid AS jsonb)", {"aid": f'[{{"id":{int(aid)}}}]'}))
        return self
    
    def subject_id(self, sid: int) -> SearchQuery:
        """Filter by subject ID using MN table for fast indexed lookup."""
        self._filter.append((
            "EXISTS (SELECT 1 FROM mn_books_subjects mbs WHERE mbs.fk_books = book_id AND mbs.fk_subjects = :sid)",
            {"sid": int(sid)}
        ))
        return self
    
    def bookshelf_id(self, bid: int) -> SearchQuery:
        """Filter by bookshelf ID using MN table for fast indexed lookup."""
        self._filter.append((
            "EXISTS (SELECT 1 FROM mn_books_bookshelves mbb WHERE mbb.fk_books = book_id AND mbb.fk_bookshelves = :bid)",
            {"bid": int(bid)}
        ))
        return self
    
    def encoding(self, enc: Encoding) -> SearchQuery:
        self._filter.append(("dc->'format' @> CAST(:enc AS jsonb)", {"enc": f'[{{"encoding":"{enc.value}"}}]'}))
        return self
    
    def where(self, sql: str, **params) -> SearchQuery:
        """Add raw SQL filter condition."""
        self._filter.append((sql, params))
        return self
    
    # === SQL Building ===
    
    def _params(self) -> dict[str, Any]:
        params = {}
        for _, p, *_ in self._search:
            params.update(p)
        for _, p in self._filter:
            params.update(p)
        return params
    
    def _order_sql(self, params: dict) -> str:
        if self._order == OrderBy.RELEVANCE and self._search:
            sql, p, col = self._search[-1]
            params["rank_q"] = p["q"].replace("%", "")
            if "<%" in sql or "ILIKE" in sql:
                return f"word_similarity(:rank_q, {col}) DESC, downloads DESC"
            return f"ts_rank_cd({col}, websearch_to_tsquery('english', :rank_q)) DESC, downloads DESC"
        
        if self._order == OrderBy.RANDOM:
            return "RANDOM()"
        
        if self._order not in _ORDER_COLUMNS:
            return "downloads DESC"
        
        col, default_dir, nulls = _ORDER_COLUMNS[self._order]
        direction = self._sort_dir or default_dir
        clause = f"{col} {direction.value.upper()}"
        if nulls:
            clause += f" NULLS {nulls}"
        return clause
    
    def build(self) -> tuple[str, dict]:
        params = self._params()
        order = self._order_sql(params)
        limit, offset = self._page_size, (self._page - 1) * self._page_size
        
        search_sql = " AND ".join(s[0] for s in self._search) if self._search else None
        filter_sql = " AND ".join(f[0] for f in self._filter) if self._filter else None
        
        if search_sql and filter_sql:
            sql = f"SELECT {_SELECT} FROM (SELECT {_SUBQUERY} FROM mv_books_dc WHERE {search_sql}) t WHERE {filter_sql} ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        elif search_sql:
            sql = f"SELECT {_SELECT} FROM mv_books_dc WHERE {search_sql} ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        elif filter_sql:
            sql = f"SELECT {_SELECT} FROM mv_books_dc WHERE {filter_sql} ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        else:
            sql = f"SELECT {_SELECT} FROM mv_books_dc ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        
        return sql, params
    
    def build_count(self) -> tuple[str, dict]:
        params = self._params()
        search_sql = " AND ".join(s[0] for s in self._search) if self._search else None
        filter_sql = " AND ".join(f[0] for f in self._filter) if self._filter else None
        
        if search_sql and filter_sql:
            return f"SELECT COUNT(*) FROM (SELECT {_SUBQUERY} FROM mv_books_dc WHERE {search_sql}) t WHERE {filter_sql}", params
        elif search_sql:
            return f"SELECT COUNT(*) FROM mv_books_dc WHERE {search_sql}", params
        elif filter_sql:
            return f"SELECT COUNT(*) FROM mv_books_dc WHERE {filter_sql}", params
        return "SELECT COUNT(*) FROM mv_books_dc", params


# =============================================================================
# FullTextSearch
# =============================================================================

class FullTextSearch:
    """Main search interface."""
    
    def __init__(self, config: Config | None = None):
        cfg = config or Config()
        self.engine = create_engine(
            f"postgresql://{cfg.PGUSER}@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.PGDATABASE}",
            pool_pre_ping=True,
            pool_recycle=300,
        )
        self.Session = sessionmaker(bind=self.engine)
        self._custom_transformer: Callable | None = None
    
    def set_custom_transformer(self, fn: Callable) -> None:
        """Set custom transformer for Crosswalk.CUSTOM."""
        self._custom_transformer = fn
    
    def query(self, crosswalk: Crosswalk = Crosswalk.FULL) -> SearchQuery:
        """Create a new query builder."""
        q = SearchQuery()
        q._crosswalk = crosswalk
        return q
    
    def _transform(self, row, cw: Crosswalk) -> dict:
        if cw == Crosswalk.CUSTOM and self._custom_transformer:
            return self._custom_transformer(row)
        return _CROSSWALK_MAP[cw](row)
    
    def execute(self, q: SearchQuery) -> dict:
        """Execute query and return paginated results."""
        with self.Session() as session:
            count_sql, count_params = q.build_count()
            total = session.execute(text(count_sql), count_params).scalar() or 0
            total_pages = max(1, (total + q._page_size - 1) // q._page_size)
            q._page = max(1, min(q._page, total_pages))
            
            sql, params = q.build()
            rows = session.execute(text(sql), params).fetchall()
        
        return {
            "results": [self._transform(r, q._crosswalk) for r in rows],
            "page": q._page,
            "page_size": q._page_size,
            "total": total,
            "total_pages": total_pages,
        }
    
    def get(self, etext_nr: int, crosswalk: Crosswalk = Crosswalk.FULL) -> dict | None:
        """Get single book by ID."""
        with self.Session() as session:
            row = session.execute(
                text(f"SELECT {_SELECT} FROM mv_books_dc WHERE book_id = :id"),
                {"id": int(etext_nr)}
            ).fetchone()
            return self._transform(row, crosswalk) if row else None
    
    def get_many(self, nrs: list[int], crosswalk: Crosswalk = Crosswalk.FULL) -> list[dict]:
        """Get multiple books by IDs."""
        if not nrs:
            return []
        with self.Session() as session:
            rows = session.execute(
                text(f"SELECT {_SELECT} FROM mv_books_dc WHERE book_id = ANY(:ids)"),
                {"ids": [int(n) for n in nrs]}
            ).fetchall()
            return [self._transform(r, crosswalk) for r in rows]
    
    def count(self, q: SearchQuery) -> int:
        """Count results without fetching."""
        with self.Session() as session:
            sql, params = q.build_count()
            return session.execute(text(sql), params).scalar() or 0
    
    def list_bookshelves(self) -> list[dict]:
        """
        List all bookshelves with book counts.
        
        Returns:
            List of dicts with 'id', 'name', and 'book_count' keys
        """
        sql = """
            SELECT bs.pk AS id, bs.bookshelf AS name, COUNT(mbbs.fk_books) AS book_count
            FROM bookshelves bs
            LEFT JOIN mn_books_bookshelves mbbs ON bs.pk = mbbs.fk_bookshelves
            GROUP BY bs.pk, bs.bookshelf
            ORDER BY bs.bookshelf
        """
        with self.Session() as session:
            rows = session.execute(text(sql)).fetchall()
            return [{'id': r.id, 'name': r.name, 'book_count': r.book_count} for r in rows]
    
    def list_subjects(self) -> list[dict]:
        """
        List all subjects with book counts.
        
        Returns:
            List of dicts with 'id', 'name', and 'book_count' keys
        """
        sql = """
            SELECT s.pk AS id, s.subject AS name, COUNT(mbs.fk_books) AS book_count
            FROM subjects s
            LEFT JOIN mn_books_subjects mbs ON s.pk = mbs.fk_subjects
            GROUP BY s.pk, s.subject
            ORDER BY book_count DESC, s.subject
        """
        with self.Session() as session:
            rows = session.execute(text(sql)).fetchall()
            return [{'id': r.id, 'name': r.name, 'book_count': r.book_count} for r in rows]
    
    def get_subject_name(self, subject_id: int) -> str | None:
        """
        Get a single subject's name by ID (fast lookup).
        
        Args:
            subject_id: Subject primary key
            
        Returns:
            Subject name or None if not found
        """
        sql = "SELECT subject FROM subjects WHERE pk = :id"
        with self.Session() as session:
            result = session.execute(text(sql), {"id": subject_id}).scalar()
            return result
    
    def get_top_subjects_for_query(self, q: SearchQuery, limit: int = 15, max_books: int = 1000) -> list[dict]:
        """
        Get top N subjects from a search result set for dynamic facets.
        
        Args:
            q: SearchQuery to derive subjects from
            limit: Maximum number of subjects to return (default 15)
            max_books: Maximum number of matching books to sample (default 1000)
            
        Returns:
            List of dicts with 'id', 'name', and 'count' keys, sorted by count desc
        """
        max_books = max(1, min(5000, int(max_books)))
        limit = max(1, min(100, int(limit)))

        # Build a limited "matched books" set using the same WHERE + ORDER
        params = q._params()
        order_sql = q._order_sql(params)
        search_sql = " AND ".join(s[0] for s in q._search) if q._search else None
        filter_sql = " AND ".join(f[0] for f in q._filter) if q._filter else None
        where_parts = [p for p in (search_sql, filter_sql) if p]
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # Count subjects from the sampled matching books (JOIN beats IN(subquery) here)
        sql = f"""
            WITH matched_books AS (
                SELECT book_id
                FROM mv_books_dc
                {where_clause}
                ORDER BY {order_sql}
                LIMIT :max_books
            )
            SELECT s.pk AS id, s.subject AS name, COUNT(*) AS count
            FROM matched_books mb
            JOIN mn_books_subjects mbs ON mbs.fk_books = mb.book_id
            JOIN subjects s ON mbs.fk_subjects = s.pk
            GROUP BY s.pk, s.subject
            ORDER BY count DESC
            LIMIT :limit
        """
        params["limit"] = limit
        params["max_books"] = max_books
        
        with self.Session() as session:
            rows = session.execute(text(sql), params).fetchall()
            return [{'id': r.id, 'name': r.name, 'count': r.count} for r in rows]
    
    def get_books_by_locc(self, locc_code: str, page: int = 1, page_size: int = 28, crosswalk: Crosswalk = Crosswalk.OPDS) -> dict:
        """
        Get books filtered by a specific LOCC code.
        
        Args:
            locc_code: LOCC code to filter by
            page: Page number (1-indexed)
            page_size: Results per page
            crosswalk: Output format
            
        Returns:
            Paginated results dict
        """
        q = self.query(crosswalk)
        q.locc(locc_code)
        q.order_by(OrderBy.DOWNLOADS)
        q[page, page_size]
        return self.execute(q)
    
    def get_books_by_bookshelf(self, bookshelf_id: int, page: int = 1, page_size: int = 28, crosswalk: Crosswalk = Crosswalk.OPDS) -> dict:
        """
        Get books in a specific bookshelf.
        
        Args:
            bookshelf_id: Bookshelf primary key
            page: Page number (1-indexed)
            page_size: Results per page
            crosswalk: Output format
            
        Returns:
            Paginated results dict
        """
        q = self.query(crosswalk)
        q.bookshelf_id(bookshelf_id)
        q.order_by(OrderBy.DOWNLOADS)
        q[page, page_size]
        return self.execute(q)
    
    def get_books_by_subject(self, subject_id: int, page: int = 1, page_size: int = 28, crosswalk: Crosswalk = Crosswalk.OPDS) -> dict:
        """
        Get books with a specific subject.
        
        Args:
            subject_id: Subject primary key
            page: Page number (1-indexed)
            page_size: Results per page
            crosswalk: Output format
            
        Returns:
            Paginated results dict
        """
        q = self.query(crosswalk)
        q.subject_id(subject_id)
        q.order_by(OrderBy.DOWNLOADS)
        q[page, page_size]
        return self.execute(q)
    
    def get_bookshelf_samples_batch(self, bookshelf_ids: list[int], sample_limit: int = 20, crosswalk: Crosswalk = Crosswalk.OPDS) -> dict[int, dict]:
        """
        Fetch sample publications from multiple bookshelves in a single optimized query.
        Uses MN table (mn_books_bookshelves) for fast indexed lookups instead of JSONB.
        
        Args:
            bookshelf_ids: List of bookshelf IDs to fetch samples for
            sample_limit: Max books per bookshelf
            crosswalk: Output format
            
        Returns:
            Dict mapping bookshelf_id -> {"results": [...], "total": int}
        """
        if not bookshelf_ids:
            return {}
        
        # Use window function for top-N per bookshelf (works better with SQLAlchemy)
        sql = """
            WITH ranked AS (
                SELECT 
                    mbb.fk_bookshelves AS bs_id,
                    mv.book_id, mv.title, mv.all_authors, mv.downloads, mv.dc,
                    ROW_NUMBER() OVER (PARTITION BY mbb.fk_bookshelves ORDER BY mv.downloads DESC) AS rn
                FROM mn_books_bookshelves mbb
                JOIN mv_books_dc mv ON mv.book_id = mbb.fk_books
                WHERE mbb.fk_bookshelves = ANY(:ids)
            )
            SELECT bs_id, book_id, title, all_authors, downloads, dc
            FROM ranked
            WHERE rn <= :sample_limit
            ORDER BY bs_id, downloads DESC
        """
        
        # Get totals using MN table (fast indexed count)
        count_sql = """
            SELECT fk_bookshelves AS bs_id, COUNT(*) as total
            FROM mn_books_bookshelves
            WHERE fk_bookshelves = ANY(:ids)
            GROUP BY fk_bookshelves
        """
        
        transformer = _CROSSWALK_MAP[crosswalk]
        
        with self.Session() as session:
            # Get counts (fast - uses index on fk_bookshelves)
            count_rows = session.execute(text(count_sql), {"ids": bookshelf_ids}).fetchall()
            totals = {r.bs_id: r.total for r in count_rows}
            
            # Get samples
            rows = session.execute(text(sql), {"ids": bookshelf_ids, "sample_limit": sample_limit}).fetchall()
        
        # Group by bookshelf
        result = {bid: {"results": [], "total": totals.get(bid, 0)} for bid in bookshelf_ids}
        for row in rows:
            result[row.bs_id]["results"].append(transformer(row))
        
        return result