# Darksouls Character Network

Web crawler and network builder for the Dark Souls trilogy (DS1, DS2, DS3 + DLCs).  
Scrapes the [Fextralife wikis](https://darksouls.wiki.fextralife.com) to extract NPCs and bosses, computes co-mention weights between characters, and exports a weighted undirected graph as CSV files ready for Gephi or NetworkX.

---

## How it works

1. **Seed crawling** — Downloads the `/NPCs` and `/Bosses` hub pages for each game.
2. **Character discovery** — Extracts character links exclusively from the `<table>` elements inside the content block, discarding community/guide sections.
3. **Page download** — Visits each character page and extracts its full text.
4. **Co-mention counting** — For every pair of characters (A, B), counts how many times B is mentioned on A's page and vice versa. The total forms the edge weight.
5. **Alias resolution** — Generates short single-word aliases (e.g. `"Gwyn"` → `"Gwyn Lord of Cinder"`) to catch informal mentions and avoid double-counting.
6. **Export** — Writes `darksouls_nodes.csv` and `darksouls_edges.csv`.

### Sources

| Game | Wiki base URL |
|------|--------------|
| DS1  | `darksouls.wiki.fextralife.com` — `/NPCs`, `/Bosses`, `/Mini+Bosses`, `/Expansion+Bosses` |
| DS2  | `darksouls2.wiki.fextralife.com` — `/NPCs`, `/Bosses` |
| DS3  | `darksouls3.wiki.fextralife.com` — `/NPCs`, `/Bosses` |

---

## Network stats (last run)

| | |
|---|---|
| **Nodes** (characters) | 228 |
| **Edges** (co-mention pairs) | 589 |
| **Edge minimum weight** | 2 |
| **Node attributes** | `id`, `label`, `games`, `type` (NPC / Boss), `location`, `alias` |
| **Edge attributes** | `source`, `source_label`, `target`, `target_label`, `weight` |

---

## Output files

### `darksouls_nodes.csv`
| Column | Description |
|--------|-------------|
| `id` | Unique integer identifier |
| `label` | Character name |
| `games` | Game(s) the character appears in (`DS1`, `DS2`, `DS3`, or combinations) |
| `type` | `NPC` or `Boss` |
| `location` | In-game location extracted from the wiki infobox or page text |
| `alias` | Short alias(es) used for mention matching (pipe-separated) |

### `darksouls_edges.csv`
| Column | Description |
|--------|-------------|
| `source` | Source node id |
| `source_label` | Source character name |
| `target` | Target node id |
| `target_label` | Target character name |
| `weight` | Total number of mutual co-mentions across all pages |

### `Aliases`
Plain-text file listing every generated alias and its canonical name, updated automatically on each run.

---

## Installation

```bash
pip install requests beautifulsoup4 pandas cloudscraper
```

> `cloudscraper` is recommended to bypass Cloudflare protection on the wikis. The script falls back to `requests` if it is not installed.

---

## Usage

```bash
python darksouls_network.py
```

The script prints progress to the terminal and writes the three output files in the working directory.

---

## Visualisation

**Gephi**
1. `File > Import Spreadsheet` → import `darksouls_nodes.csv` (node table)
2. `File > Import Spreadsheet` → import `darksouls_edges.csv` (edge table, type `Undirected`)
3. Open `DarkSouls_red.gephi` for the pre-built layout.

**NetworkX**
```python
import pandas as pd
import networkx as nx

G = nx.from_pandas_edgelist(
    pd.read_csv("darksouls_edges.csv"),
    "source_label", "target_label", "weight"
)
```

---

## Configuration

Key constants at the top of the script:

| Constant | Default | Description |
|----------|---------|-------------|
| `MIN_WEIGHT` | `2` | Minimum co-mention count to include an edge |
| `DELAY` | `0.8` s | Pause between HTTP requests |
| `TIMEOUT` | `15` s | HTTP request timeout |

---

## License

[MIT](LICENSE)
