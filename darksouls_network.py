"""
=============================================================
  CHARACTER NETWORK — DARK SOULS SAGA (DS1 + DS2 + DS3)
  Original work

  Methodology:
    1. Downloads the seed pages for Characters/NPCs/Bosses.
    2. Extracts links ONLY from <table> elements inside the
       content block (where character lists are located).
       Fextralife hub pages mix characters with community/
       guide sections; tables contain only characters,
       everything else is discarded.
    3. Visits each found page and downloads its text.
    4. Counts co-mentions between characters on each page.
    5. Generates edges weighted by number of co-mentions.

  Sources (seeds):
    DS1 → darksouls.wiki.fextralife.com/NPCs + /Bosses + /Mini+Bosses + /Expansion+Bosses
    DS2 → darksouls2.wiki.fextralife.com/NPCs + /Bosses
    DS3 → darksouls3.wiki.fextralife.com  (same)

  Usage:
    pip install requests beautifulsoup4 pandas
    python darksouls_network.py

  Output:
    darksouls_nodes.csv
    darksouls_edges.csv
=============================================================
"""

import time
import re
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
import pandas as pd
from collections import defaultdict, Counter

try:
    import cloudscraper
    _SESSION = cloudscraper.create_scraper()
except ImportError:
    import requests as _requests
    _SESSION = _requests.Session()
    print("WARNING: cloudscraper not installed. Install with: pip install cloudscraper")

# ──────────────────────────────────────────────────────────────
#  CONFIGURATION
# ──────────────────────────────────────────────────────────────
MIN_WEIGHT = 2
DELAY      = 0.8
TIMEOUT    = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

WIKIS = {
    "DS1": {
        "base":  "https://darksouls.wiki.fextralife.com",
        "seeds": ["/NPCs", "/Bosses", "/Mini+Bosses", "/Expansion+Bosses"],
    },
    "DS2": {
        "base":  "https://darksouls2.wiki.fextralife.com",
        "seeds": ["/NPCs", "/Bosses"],
    },
    "DS3": {
        "base":  "https://darksouls3.wiki.fextralife.com",
        "seeds": ["/NPCs", "/Bosses"],
    },
}

CONTENT_SELECTORS = [
    {"id":    "wiki-content-block"},
    {"id":    "page-content"},
    {"class": "page-content"},
]

# Canonical locations extracted from the official locations pages:
#   DS1 → darksouls.wiki.fextralife.com/Places
#   DS2 → darksouls2.wiki.fextralife.com/locations
#   DS3 → darksouls3.wiki.fextralife.com/locations
GAME_LOCATIONS = {
    "DS1": [
        "Northern Undead Asylum", "Firelink Shrine", "Undead Burg", "Undead Parish",
        "Depths", "Blighttown", "Quelaag's Domain", "The Great Hollow", "Ash Lake",
        "Sen's Fortress", "Anor Londo", "Painted World of Ariamis", "Darkroot Garden",
        "Darkroot Basin", "New Londo Ruins", "The Duke's Archives", "Crystal Cave",
        "Demon Ruins", "Lost Izalith", "The Catacombs", "Tomb of Giants",
        "The Valley of the Drakes", "The Abyss", "Kiln of the First Flame",
        "Sanctuary Garden", "Oolacile Sanctuary", "Royal Woods", "Oolacile Township",
        "Chasm of the Abyss", "Battle of Stoicism",
    ],
    "DS2": [
        "Things Betwixt", "Majula", "Forest of Fallen Giants", "Heide's Tower of Flame",
        "Cathedral of Blue", "No-man's Wharf", "The Lost Bastille", "Belfry Luna",
        "Sinners' Rise", "Huntsman's Copse", "Undead Purgatory", "Harvest Valley",
        "Earthen Peak", "Iron Keep", "Belfry Sol", "The Pit", "Grave of Saints",
        "The Gutter", "Black Gulch", "Shaded Woods", "Doors of Pharros",
        "Brightstone Cove Tseldora", "Lord's Private Chamber", "Shrine of Winter",
        "Drangleic Castle", "Shrine of Amana", "Undead Crypt", "Aldia's Keep",
        "Dragon Aerie", "Dragon Shrine", "Dark Chasm of Old", "Memory of Vammar",
        "Memory of Orro", "Memory of Jeigh", "Dragon Memories", "Throne Of Want",
        "Shulva - Sanctum City", "Dragon's Sanctum", "Dragon's Rest", "Cave of the Dead",
        "Memory of the King", "Brume Tower", "Iron Passage", "Memory of the Old Iron King",
        "Frozen Eleum Loyce", "Grand Cathedral", "Old Chaos", "Frigid Outskirts",
    ],
    "DS3": [
        "Anor Londo", "Archdragon Peak", "Catacombs of Carthus", "Cathedral of the Deep",
        "Cemetery of Ash", "Church of Yorshka", "Consumed King's Garden", "Farron Keep",
        "Firelink Shrine", "Grand Archives", "High Wall of Lothric", "Irithyll Dungeon",
        "Irithyll of the Boreal Valley", "Kiln of the First Flame", "Lothric Castle",
        "Painted World of Ariandel", "Profaned Capital", "Road of Sacrifices",
        "Smouldering Lake", "The Dreg Heap", "The Ringed City", "Undead Settlement",
        "Untended Graves",
    ],
}

BAD_PATH_WORDS = {
    "weapon", "armor", "armour", "shield", "ring", "spell",
    "sorceri", "miracle", "pyromancy", "item", "consumable",
    "material", "upgrade", "infusion", "bonfire", "ember",
    "location", "area", "walkthrough", "guide", "covenant",
    "achievement", "trophy", "build", "class", "origin",
    "ending", "lore", "gallery", "secret", "illusory",
    "control", "combat", "faq", "humanity", "stat", "attrib",
    "mechanic", "community", "media", "art", "stream", "chat",
    "player", "steam", "xbox", "playstation", "psn",
    "youtube", "partner", "event", "about", "todo", "wiki",
    "general", "patch", "dlc", "update", "download",
    "talk:", "file:", "special:", "template:", "category:",
}


# ──────────────────────────────────────────────────────────────
#  UTILIDADES
# ──────────────────────────────────────────────────────────────

def get_soup(url: str):
    try:
        r = _SESSION.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None, None
        return BeautifulSoup(r.text, "html.parser"), r.url
    except Exception:
        return None, None


def get_content_block(soup):
    for attrs in CONTENT_SELECTORS:
        el = soup.find("div", attrs)
        if el and len(el.get_text()) > 100:
            return el
    return None


_NOISE_SELECTORS = [
    {"id": "tagged-pages-container"},  # list of all NPCs/Bosses in the game
    {"class": "spoiler"},              # spoilers containing said list
    {"class": "comments-section"},
    {"id": "discussions-section"},
]

def _find_location_in_text(text: str, locations: list) -> str:
    """
    Searches the character's text for canonical game locations
    (extracted from the official locations pages).
    Returns the most mentioned location; on a tie, returns all
    tied locations separated by commas. Returns "?" if none found.
    """
    text_lower = text.lower()
    counts = {}
    for loc in locations:
        pattern = r"(?<!\w)" + re.escape(loc.lower()) + r"(?!\w)"
        n = len(re.findall(pattern, text_lower))
        if n > 0:
            counts[loc] = n
    if not counts:
        return "?"
    max_count = max(counts.values())
    best = [loc for loc, n in counts.items() if n == max_count]
    return ", ".join(best)


def extract_location(soup, text: str = "", game: str = "") -> str:
    """
    Finds the character's location in two steps:

    Step 1 — Fextralife infobox (three patterns):
      Pattern A1 (DS1/DS3 NPCs): row label
          Location | Depths, Firelink Shrine, Blighttown
      Pattern A2 (DS2 NPCs/Bosses): column header
          HP | Souls | Location | Drops  →  value in the next row.
      Pattern B (Bosses): <h2/h3/h4> heading "Location"
          followed by <ul><li>...</li></ul> or <p>.

    Step 2 — Text search (fallback):
      If the infobox yielded nothing, searches the page text
      for canonical game locations (GAME_LOCATIONS[game])
      and returns the most mentioned one.
    """
    block = get_content_block(soup)
    if block:
        # Patrones A1 y A2: celda con texto exacto "location" en tabla infobox
        for table in block.find_all("table"):
            rows = table.find_all("tr")
            for i, row in enumerate(rows):
                cells = row.find_all(["td", "th"])
                for j, cell in enumerate(cells):
                    if cell.get_text(strip=True).lower() == "location":
                        # A1: etiqueta de fila → valor en la misma fila, columna j+1
                        if j == 0 and j + 1 < len(cells):
                            loc = cells[j + 1].get_text(separator=", ", strip=True)
                            if loc and loc not in ("-", "–", ""):
                                return loc
                        # A2: cabecera de columna → valor en fila siguiente, columna j
                        if i + 1 < len(rows):
                            value_cells = rows[i + 1].find_all(["td", "th"])
                            if j < len(value_cells):
                                loc = value_cells[j].get_text(separator=", ", strip=True)
                                if loc and loc not in ("-", "–", ""):
                                    return loc

        # Patrón B: encabezado "Location" seguido de lista o párrafo
        for heading in block.find_all(["h2", "h3", "h4"]):
            if heading.get_text(strip=True).lower() == "location":
                sibling = heading.find_next_sibling()
                while sibling and not sibling.get_text(strip=True).replace("\xa0", ""):
                    sibling = sibling.find_next_sibling()
                if sibling:
                    items = sibling.find_all("li")
                    if items:
                        locs = [li.get_text(strip=True) for li in items if li.get_text(strip=True)]
                        if locs:
                            return ", ".join(locs)
                    else:
                        loc = sibling.get_text(strip=True)
                        if loc and loc not in ("-", "–"):
                            return loc

    # Paso 2: fallback — buscar localizaciones canónicas en el texto
    if text and game in GAME_LOCATIONS:
        return _find_location_in_text(text, GAME_LOCATIONS[game])

    return "?"


def _strip_parens(name: str) -> str:
    """Removes content in parentheses and normalizes spaces."""
    return " ".join(re.sub(r"\s*\([^)]*\)\s*", " ", name).split())


def extract_page_name(soup) -> str:
    """
    Extracts the character name from <a id="page-title">.
    Removes the wiki suffix (' | Dark Souls X Wiki...'), truncates
    before the first descriptive comma, and strips parentheses.
      "Aldrich, Devourer of Gods | Dark Souls 3 Wiki | ..."  ->  "Aldrich"
      "Pursuer | Dark Souls 2 Wiki"                          ->  "Pursuer"
      "(Blue) Smelter Demon | ..."                           ->  "Smelter Demon"
      "Undead Merchant (Female) | ..."                       ->  "Undead Merchant"
      "DragonRider(Second Encounter) | ..."                  ->  "DragonRider"
    """
    title_a = soup.find("a", id="page-title")
    if not title_a:
        return ""
    name = title_a.get_text(strip=True).split("|")[0].strip()
    if "," in name:
        name = name.split(",")[0].strip()
    name = _strip_parens(name)
    # Elimina posesivos para fusionar duplicados como
    # "Executioner's Chariot" -> "Executioner Chariot"
    # y que el alias generado sea "Executioner" (sin 's').
    name = re.sub(r"['\u2019]s\b", "", name)
    return name


def extract_text(soup) -> str:
    block = get_content_block(soup)
    if block:
        # Remove navigation widgets that would generate false co-mentions
        for attrs in _NOISE_SELECTORS:
            for el in block.find_all(True, attrs):
                el.decompose()
        return block.get_text(separator=" ").strip()
    paras = soup.find_all("p")
    if paras:
        text = " ".join(p.get_text(separator=" ") for p in paras).strip()
        if len(text) > 200:
            return text
    return ""


def is_valid_character_path(path: str) -> bool:
    """
    True if the path looks like a character page:
      - Single path segment (no subdirectories)
      - No keywords from non-character sections
      - Length between 3 and 80 chars
    """
    path = path.strip("/")
    if "/" in path:
        return False
    if len(path) < 3 or len(path) > 80:
        return False
    path_lower = path.lower()
    for word in BAD_PATH_WORDS:
        if word in path_lower:
            return False
    return True


def _collect_links(anchors, base_url: str, base_domain: str, seen: set) -> list:
    links = []
    for a in anchors:
        href = a.get("href", "").strip()
        if not href:
            continue
        full_url = urljoin(base_url, href)
        parsed   = urlparse(full_url)

        if parsed.netloc != base_domain:
            continue
        if parsed.fragment:
            continue

        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if clean in seen:
            continue
        seen.add(clean)

        if is_valid_character_path(parsed.path):
            links.append(clean)
    return links


def extract_links_from_tables(soup, base_url: str) -> list:
    """
    Extracts character links from the main content block.

    Fextralife has two ways of listing characters on seed pages:
      A) <a class="wiki_link"><img ...><br>Name</a>  (with photo)
      B) <a class="wiki_link">Name</a>               (without photo)
    Additionally, all pages have a #tagged-pages-container with the
    curated complete list of all NPCs/Bosses for the game.

    Strategy: collect (A) + #tagged-pages-container, then add (B)
    that passes the valid path filter. The 'seen' set deduplicates all.
    If nothing works, fall back to the full content block.
    """
    base_domain = urlparse(base_url).netloc
    block = get_content_block(soup)
    if not block:
        return []

    seen  = set()
    links = []

    # A) wiki_link with image (main gallery / Tab Gallery)
    anchors_with_img = [
        a for a in block.find_all("a", class_="wiki_link", href=True)
        if a.find("img")
    ]
    links += _collect_links(anchors_with_img, base_url, base_domain, seen)

    # B) #tagged-pages-container: curated complete list (covers NPCs without photo)
    tpc = block.find(id="tagged-pages-container")
    if tpc:
        links += _collect_links(tpc.find_all("a", href=True), base_url, base_domain, seen)

    # C) wiki_link inside <h4> (pattern from DS2 Bosses "Quick Rundown" tab
    #    and entries without image on other seed pages).
    #    Restricting to <h4> avoids picking up area/item/guide links
    #    that appear in description paragraphs in the page body.
    h4_anchors = [
        a
        for h4 in block.find_all("h4")
        for a in h4.find_all("a", class_="wiki_link", href=True)
    ]
    links += _collect_links(h4_anchors, base_url, base_domain, seen)

    # D) First wiki_link in the first <td> of each table row
    #    Covers DLC data tables (DS3: Ashes of Ariandel, Ringed City)
    #    where the boss is a text link without image in the first column.
    #    Only tables whose first header says "boss", "npc" or "name" are processed.
    #    <thead>/<tbody> and recursive=False are used to avoid mixing with
    #    nested tooltip tables inside each boss cell.
    for table in block.find_all("table", recursive=True):
        thead = table.find("thead")
        tbody = table.find("tbody")
        if not thead or not tbody:
            continue
        header_row = thead.find("tr")
        if not header_row:
            continue
        first_cell = header_row.find(["th", "td"])
        if not first_cell or first_cell.get_text(strip=True).lower() not in ("boss", "npc", "name"):
            continue
        for row in tbody.find_all("tr", recursive=False):
            tds = row.find_all("td", recursive=False)
            if not tds:
                continue
            first_link = tds[0].find("a", class_="wiki_link", href=True)
            if first_link:
                links += _collect_links([first_link], base_url, base_domain, seen)

    return links


# ──────────────────────────────────────────────────────────────
#  CRAWLING
# ──────────────────────────────────────────────────────────────

def crawl_game(game: str, config: dict) -> dict:
    base  = config["base"]
    seeds = config["seeds"]

    print(f"\n  {'─'*56}")
    print(f"  {game}  ->  {base}")
    print(f"  {'─'*56}")

    print(f"\n  [Phase 1] Extracting URLs from seed tables...")

    char_urls = {}   # url -> name
    url_types = {}   # url -> "Boss" | "NPC"
    visited   = set()

    for seed_path in seeds:
        seed_url  = base + seed_path
        node_type = "Boss" if seed_path.rstrip("/").endswith("Bosses") else "NPC"
        print(f"    down {seed_url}...", end=" ", flush=True)

        soup, final_url = get_soup(seed_url)
        time.sleep(DELAY)

        if not soup:
            print("X (error HTTP)")
            continue

        visited.add(seed_url)
        if final_url:
            visited.add(final_url)

        links = extract_links_from_tables(soup, base)

        new = 0
        for url in links:
            if url not in char_urls and url not in visited:
                path = urlparse(url).path.strip("/")
                name = unquote(path).replace("+", " ").replace("_", " ")
                char_urls[url] = name
                url_types[url] = node_type
                new += 1

        print(f"OK  ({new} new [{node_type}])")

    print(f"\n    Total character URLs: {len(char_urls)}")

    if not char_urls:
        print(f"  WARNING: No URLs found in tables for {game}.")
        return {}, {}

    print(f"\n  [Phase 2] Downloading character pages...\n")

    char_pages  = {}
    name_types  = {}
    name_locs   = {}
    total       = len(char_urls)

    for i, (url, name) in enumerate(char_urls.items(), 1):
        pct = i / total * 100
        print(f"  [{pct:5.1f}%] {game} | {name[:45]:<45}", end=" ", flush=True)

        soup, _ = get_soup(url)
        time.sleep(DELAY)

        if not soup:
            print("X (HTTP error)")
            continue

        text = extract_text(soup)
        if not text:
            print("X (no content)")
            continue

        # Canonical name: extracted from <a id="page-title"> on the page itself.
        # Truncated at the first comma to use the short name by which
        # other characters refer to it (e.g. "Manus" instead of
        # "Manus, Father of the Abyss").
        actual_name = extract_page_name(soup) or _strip_parens(name)

        char_pages[actual_name] = text
        name_types[actual_name] = url_types[url]
        name_locs[actual_name]  = extract_location(soup, text=text, game=game)
        print(f"OK ({len(text):,} chars)")

    print(f"\n  Characters with text in {game}: {len(char_pages)}")
    return char_pages, name_types, name_locs


# ──────────────────────────────────────────────────────────────
#  NETWORK CONSTRUCTION
# ──────────────────────────────────────────────────────────────

def build_name_aliases(names: list) -> dict:
    """
    For each multi-token name, generates a single-word alias
    (first or last word) when that word uniquely identifies the character.

    Safety criteria:
      - Length >= 4 chars (avoids articles, short prepositions).
      - Not a generic game word (stop words).
      - Is the first/last word of exactly ONE name (uniqueness).
      - Does not already exist as a standalone canonical name.
      - Possessives are cleaned: "Executioner's" -> alias "Executioner".

    Returns: dict  alias -> canonical_name
    """
    _STOP = {
        # Articles, prepositions, connectors
        "the", "of", "and", "a", "an", "to", "in", "for", "with", "from",
        "by", "at", "on",
        # Titles, generic colors and materials
        "old", "new", "great", "dark", "black", "white", "silver", "golden",
        "lord", "sir", "king", "knight", "iron", "stone", "crystal",
        # Cardinal directions
        "east", "west", "north", "south",
        # Dark Souls lore terms omnipresent in texts
        "soul", "souls", "cinder", "fire", "abyss", "hollow", "undead",
        "dragon", "mass", "moon", "giant",
        # Manually reviewed: too generic or generate false positives
        "archer",    "authority", "blue",      "boar",      "burnt",     "butterfly",
        "captain",   "centipede", "champion",  "chaos",     "deep",      "four",
        "head",      "herald",    "high",      "holy",      "kings",     "knights",
        "lion",      "londor",    "looking",   "lords",     "lost",      "masterless",
        "merciless", "merchant",  "mild",      "moaning",   "moonlight", "painting",
        "paladin",   "peculiar",  "phantom",   "rock",      "ruin",      "sage",
        "sanctuary", "shrine",    "slave",     "song",      "stray",     "sweet",
        "throne",    "titanite",  "vanguard",  "wanderer",  "warrior",   "witch",
        "woman",     "wood",      "fold",      "grave",     "oolacile",
        "pickle",    "shade", "emerald", "blacksmith", "guardian"
    }

    _poss_re = re.compile(r"['\u2019]s$")   # elimina 's y 's al final

    def _strip_poss(word: str) -> str:
        """'Executioner's' -> 'Executioner',  'Lord's' -> 'Lord'"""
        return _poss_re.sub("", word)

    names_lower  = {n.lower() for n in names}
    first_owners = defaultdict(list)
    last_owners  = defaultdict(list)

    for name in names:
        words = name.split()
        if len(words) < 2:
            continue
        fw = _strip_poss(words[0]).lower()
        lw = _strip_poss(words[-1]).lower()
        if len(fw) >= 4 and fw not in _STOP and "(" not in fw and ")" not in fw:
            first_owners[fw].append(name)
        # Do not generate last-word alias if preceded by a preposition:
        # "Domhnall of Zena" -> "Zena" would be a place name, not an identifier.
        _PREPS = {"of", "the", "a", "an", "in", "at", "from", "by"}
        preceded_by_prep = len(words) >= 2 and words[-2].lower() in _PREPS
        if len(lw) >= 4 and lw not in _STOP and "(" not in lw and ")" not in lw and not preceded_by_prep:
            last_owners[lw].append(name)

    aliases = {}

    for word, owners in first_owners.items():
        if len(owners) == 1 and word not in names_lower:
            aliases[_strip_poss(owners[0].split()[0])] = owners[0]

    for word, owners in last_owners.items():
        if len(owners) == 1 and word not in names_lower:
            alias = _strip_poss(owners[0].split()[-1])
            if alias.lower() not in {a.lower() for a in aliases}:
                aliases[alias] = owners[0]

    return aliases


def count_mentions(text: str, names: list, aliases: dict = None) -> dict:
    """
    Counts how many times each name is mentioned in the text.
    Uses the full name as an exact phrase; additionally, if aliases are
    provided, it adds alias mentions that do NOT already overlap with the
    full name (avoids double counting).

    Example: if the text says "Gwyn" 5 times and "Gwyn Lord of Cinder" 2 times,
    and "Gwyn" is an alias of "Gwyn Lord of Cinder", the result is 5 mentions
    (2 full name + 3 standalone alias), not 7.
    """
    text_lower = text.lower()
    counts = {}
    for name in names:
        pattern = r"(?<!\w)" + re.escape(name.lower()) + r"(?!\w)"
        n = len(re.findall(pattern, text_lower))
        if n > 0:
            counts[name] = n

    if aliases:
        for alias, canonical in aliases.items():
            alias_n = len(re.findall(
                r"(?<!\w)" + re.escape(alias.lower()) + r"(?!\w)", text_lower))
            full_n  = len(re.findall(
                r"(?<!\w)" + re.escape(canonical.lower()) + r"(?!\w)", text_lower))
            # Menciones del alias que no son parte del nombre completo
            extra = alias_n - full_n
            if extra > 0:
                counts[canonical] = counts.get(canonical, 0) + extra

    return counts


def build_network(all_pages: dict, games_map: dict, type_map: dict, loc_map: dict):
    names        = list(all_pages.keys())
    aliases      = build_name_aliases(names)
    edge_weights = defaultdict(int)

    # Invert: canonical -> list of aliases (for CSV column)
    canonical_aliases = defaultdict(list)
    for alias, canonical in aliases.items():
        canonical_aliases[canonical].append(alias)

    print(f"  Short-name aliases generated: {len(aliases)}")
    alias_lines = []
    for alias, canonical in sorted(aliases.items()):
        line = f"    '{alias}'  ->  '{canonical}'"
        print(line)
        alias_lines.append(line)

    with open("Aliases", "w", encoding="utf-8") as f:
        f.write(f"  Short-name aliases generated: {len(aliases)}\n")
        f.write("\n".join(alias_lines) + "\n")
    print(f"  Aliases saved to: Aliases")

    print("  Computing co-mentions...")
    for src_name, src_text in all_pages.items():
        if not src_text.strip():
            continue
        mentions = count_mentions(src_text, names, aliases)
        for tgt_name, count in mentions.items():
            if tgt_name == src_name:
                continue
            a, b = sorted([src_name, tgt_name])
            edge_weights[(a, b)] += count

    edges_filtered = {
        (a, b): w for (a, b), w in edge_weights.items()
        if w >= MIN_WEIGHT
    }

    connected = set()
    for a, b in edges_filtered:
        connected.add(a)
        connected.add(b)

    rows_n = []
    for i, name in enumerate(sorted(connected)):
        rows_n.append({
            "id":       i,
            "label":    name,
            "games":    games_map.get(name, "?"),
            "type":     type_map.get(name, "?"),
            "location": loc_map.get(name, "?"),
            "alias":    "|".join(canonical_aliases.get(name, [])),
        })
    nodes_df = pd.DataFrame(rows_n)

    name_to_id = {r["label"]: r["id"] for _, r in nodes_df.iterrows()}
    rows_e = []
    for (a, b), w in sorted(edges_filtered.items(), key=lambda x: -x[1]):
        if a in name_to_id and b in name_to_id:
            rows_e.append({
                "source":       name_to_id[a],
                "source_label": a,
                "target":       name_to_id[b],
                "target_label": b,
                "weight":       w,
            })
    edges_df = pd.DataFrame(rows_e)
    return nodes_df, edges_df


# ──────────────────────────────────────────────────────────────
#  STATISTICS AND EXPORT
# ──────────────────────────────────────────────────────────────

def print_stats(nodes_df, edges_df):
    sep = "=" * 62
    print(f"\n{sep}")
    print("  STATISTICS  -  Dark Souls Saga Network")
    print(sep)
    print(f"  Nodes (characters):   {len(nodes_df)}")
    print(f"  Edges (relations):    {len(edges_df)}")
    if not edges_df.empty:
        print(f"  Max weight:           {edges_df['weight'].max()}")
        print(f"  Mean weight:          {edges_df['weight'].mean():.2f}")
        print(f"  Median weight:        {int(edges_df['weight'].median())}")

    print("\n  Nodes per game:")
    for game in ["DS1", "DS2", "DS3"]:
        n = nodes_df["games"].str.contains(game).sum()
        print(f"    {game}  ->  {n} characters")

    if not edges_df.empty:
        print("\nTOP 15 STRONGEST RELATIONS:")
        print(edges_df[["source_label","target_label","weight"]].head(15).to_string(index=False))

        degree = Counter()
        for _, row in edges_df.iterrows():
            degree[row["source_label"]] += 1
            degree[row["target_label"]] += 1

        print("\nTOP 15 MOST CONNECTED CHARACTERS (degree):")
        for name, deg in degree.most_common(15):
            g = nodes_df.loc[nodes_df["label"] == name, "games"].values
            print(f"  {name:<35} {deg:>3} connections  [{g[0] if len(g) else '?'}]")

    print(f"\n{sep}")
    print("  NETWORK SUMMARY (for practice wiki):")
    print(sep)
    print(f"\n  <Dark Souls Saga Character Network>")
    print(f"  <Undirected weighted network of {len(nodes_df)} characters")
    print(f"  from the Dark Souls trilogy (DS1, DS2 and DS3 + DLCs).")
    print(f"  Nodes: NPCs, bosses and lore characters.")
    print(f"  Edges: co-mentions (weight = number of mutual mentions)")
    print(f"  extracted by crawling the Fextralife wikis.>")
    print(f"  <https://darksouls.wiki.fextralife.com/Characters>")


def export_csvs(nodes_df, edges_df):
    nodes_df.to_csv("darksouls_nodes.csv", index=False, encoding="utf-8")
    edges_df.to_csv("darksouls_edges.csv", index=False, encoding="utf-8")
    print(f"\nFiles saved:")
    print(f"   darksouls_nodes.csv  ({len(nodes_df)} nodes)")
    print(f"   darksouls_edges.csv  ({len(edges_df)} edges)")
    print(f"\n   -> Gephi: File > Import Spreadsheet")
    print(f"             (nodes first, then edges, type 'undirected')")
    print(f"\n   -> NetworkX:")
    print(f"       import pandas as pd, networkx as nx")
    print(f"       G = nx.from_pandas_edgelist(")
    print(f"               pd.read_csv('darksouls_edges.csv'),")
    print(f"               'source_label', 'target_label', 'weight')")


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 62)
    print("  DARK SOULS SAGA - CHARACTER NETWORK CRAWLER")
    print("  DS1 + DS2 + DS3  (+ todos los DLCs)")
    print("=" * 62)
    print(f"\n  Strategy:")
    print(f"    The /Characters, /NPCs and /Bosses pages contain")
    print(f"    tables with characters as well as community,")
    print(f"    guide sections, etc.")
    print(f"    Only links from <table> elements in the content")
    print(f"    block are extracted, discarding everything else.\n")

    all_pages = {}
    games_map = {}
    type_map  = {}
    loc_map   = {}

    for game, config in WIKIS.items():
        game_pages, game_types, game_locs = crawl_game(game, config)
        for name, text in game_pages.items():
            if name in all_pages:
                all_pages[name] += " " + text
                if game not in games_map[name]:
                    games_map[name] += f"|{game}"
            else:
                all_pages[name] = text
                games_map[name] = game
                type_map[name]  = game_types.get(name, "?")
                loc_map[name]   = game_locs.get(name, "?")

    total_ok = sum(1 for t in all_pages.values() if t.strip())
    print(f"\n{'='*62}")
    print(f"  CRAWLING COMPLETE")
    print(f"  Unique characters: {len(all_pages)}  |  With text: {total_ok}")
    print(f"{'='*62}\n")

    print("─" * 62)
    print("  Building co-mention network...")
    print("─" * 62 + "\n")

    nodes_df, edges_df = build_network(all_pages, games_map, type_map, loc_map)
    print_stats(nodes_df, edges_df)
    export_csvs(nodes_df, edges_df)