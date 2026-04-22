import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


DEFAULT_OUTPUT_DIR = Path(r"D:\LeyesHonduras")
DEFAULT_TIMEOUT_MS = 45000
DEFAULT_DELAY_SECONDS = 1.0

COLLECTIONS = {
    "leyes": {
        "cedij": "https://legislacion.poderjudicial.gob.hn/sistemalegislacion/ConsultaRapida.aspx?cod=1&msg=Leyes",
        "tsc": "https://www.tsc.gob.hn/biblioteca/index.php/leyes",
    },
    "codigos": {
        "cedij": "https://legislacion.poderjudicial.gob.hn/sistemalegislacion/ConsultaRapida.aspx?cod=1&msg=Codigos",
        "tsc": "https://www.tsc.gob.hn/biblioteca/index.php/codigos",
    },
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(text):
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    text = re.sub(r"[\\/:*?\"<>|]+", "", text)
    text = re.sub(r"[.,;]+", "", text)
    text = re.sub(r"\s+", "_", text)
    return text[:220] or "documento"


def ensure_output_dir(path):
    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception:
        fallback = Path.cwd() / "Output" / "LeyesHonduras"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def build_opener():
    opener = urllib.request.build_opener()
    opener.addheaders = [
        (
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
    ]
    return opener


def read_json(path, default):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)


def append_jsonl(path, row):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def normalize_text(value):
    value = re.sub(r"\s+", " ", str(value or "")).strip().lower()
    value = (
        value.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    return value


class ManifestStore:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.manifest_path = output_dir / "manifest_normativa.jsonl"
        self.state_path = output_dir / "estado_scraper_normativa.json"
        self.by_url = set()
        self.by_path = set()
        self.state = read_json(self.state_path, {"sources": {}})
        self._load_existing_manifest()

    def _load_existing_manifest(self):
        if not self.manifest_path.exists():
            return
        with open(self.manifest_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("pdf_url"):
                    self.by_url.add(row["pdf_url"])
                if row.get("local_path"):
                    self.by_path.add(row["local_path"])

    def has(self, pdf_url, local_path):
        return pdf_url in self.by_url or local_path in self.by_path

    def add(self, row):
        append_jsonl(self.manifest_path, row)
        if row.get("pdf_url"):
            self.by_url.add(row["pdf_url"])
        if row.get("local_path"):
            self.by_path.add(row["local_path"])

    def update_source_state(self, source_key, page_number, page_url):
        self.state.setdefault("sources", {})
        self.state["sources"][source_key] = {
            "last_page_number": page_number,
            "last_page_url": page_url,
            "updated_at": now_iso(),
        }
        write_json(self.state_path, self.state)


def load_catalog(path):
    if not path:
        return []
    payload = read_json(Path(path), [])
    if isinstance(payload, dict):
        return payload.get("codigos", [])
    return payload


def catalog_match(title, catalog_entries):
    if not catalog_entries:
        return None

    normalized_title = normalize_text(title)
    for entry in catalog_entries:
        candidates = [entry.get("nombre_oficial"), entry.get("nombre_corto")]
        candidates.extend(entry.get("aliases", []))
        for candidate in candidates:
            candidate_norm = normalize_text(candidate)
            if not candidate_norm:
                continue
            if candidate_norm in normalized_title or normalized_title in candidate_norm:
                return entry
    return None


def make_filename(collection, source, title, decree, page_hint, pdf_url):
    parsed = urllib.parse.urlparse(pdf_url)
    ext = Path(parsed.path).suffix or ".pdf"
    parts = [collection.upper(), source.upper(), sanitize_filename(title)]
    if decree:
        parts.append(sanitize_filename(decree))
    if page_hint:
        parts.append(sanitize_filename(page_hint))
    filename = "__".join(part for part in parts if part)
    if not filename.endswith(ext):
        filename += ext
    return filename[:230]


def download_pdf(opener, pdf_url, destination, referer=None, retries=3, timeout=120):
    headers = {}
    if referer:
        headers["Referer"] = referer
    request = urllib.request.Request(pdf_url, headers=headers)

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            with opener.open(request, timeout=timeout) as response:
                content_type = response.headers.get("Content-Type", "")
                payload = response.read()
            if not payload.startswith(b"%PDF") and "pdf" not in content_type.lower():
                raise ValueError(f"Respuesta no PDF para {pdf_url} ({content_type})")
            with open(destination, "wb") as handle:
                handle.write(payload)
            return len(payload)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 * attempt)
    raise last_error


def extract_tsc_entries(page):
    return page.evaluate(
        """
        () => {
          const clean = (value) => (value || "").replace(/\\s+/g, " ").trim();
          const anchors = Array.from(document.querySelectorAll("a"))
            .filter((a) => /^Descargar/i.test(clean(a.textContent)));

          return anchors.map((anchor) => {
            let container = anchor.parentElement;
            let title = "";
            let decree = "";
            let category = "";

            for (let i = 0; i < 7 && container; i += 1) {
              const text = clean(container.innerText);
              if (!title) {
                const heading = container.querySelector("h1, h2, h3, h4, h5, h6");
                if (heading) {
                  title = clean(heading.textContent);
                }
              }
              if (!decree) {
                const decreeMatch = text.match(/(Decreto\\s+No\\.?\\s*[^\\n]+|DECRETO\\s+[^\\n]+|Acuerdo\\s+[^\\n]+)/i);
                if (decreeMatch) decree = clean(decreeMatch[1]);
              }
              if (!category) {
                const categoryMatch = text.match(/Categoría:\\s*([^\\n]+)/i);
                if (categoryMatch) category = clean(categoryMatch[1]);
              }
              if (title && decree) break;
              container = container.parentElement;
            }

            if (!title) {
              title = clean(anchor.textContent);
            }

            return {
              title,
              decree,
              category,
              pdf_url: anchor.href
            };
          });
        }
        """
    )


def extract_cedij_entries(page):
    return page.evaluate(
        """
        () => {
          const clean = (value) => (value || "").replace(/\\s+/g, " ").trim();
          return Array.from(document.querySelectorAll("a"))
            .filter((a) => /Ver Documento/i.test(clean(a.textContent)))
            .map((anchor, index) => {
              const row = anchor.closest("tr");
              const cells = row ? Array.from(row.querySelectorAll("td")) : [];
              let title = "";
              if (cells.length > 0) {
                title = clean(cells.map((cell) => clean(cell.textContent)).join(" "));
                title = title.replace(/Ver Documento/gi, "").trim();
              }
              if (!title) {
                title = clean(anchor.parentElement ? anchor.parentElement.innerText : anchor.textContent);
                title = title.replace(/Ver Documento/gi, "").trim();
              }
              return {
                result_index: index,
                title,
                href: anchor.getAttribute("href") || ""
              };
            });
        }
        """
    )


def find_next_page(page, current_page_number):
    anchors = page.locator("a")
    target = str(current_page_number + 1)
    count = anchors.count()
    candidates = []

    for idx in range(count):
        anchor = anchors.nth(idx)
        try:
            text = re.sub(r"\s+", " ", anchor.inner_text()).strip()
            href = anchor.get_attribute("href") or ""
        except Exception:
            continue
        if text == target or f"Page${target}" in href:
            candidates.append({"locator": anchor, "href": href, "priority": 10})
        elif re.search(r"(Siguiente|Next|Sig\.|>)", text, re.I) and len(text) < 15:
            candidates.append({"locator": anchor, "href": href, "priority": 1})

    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item["priority"], reverse=True)[0]


def resolve_cedij_pdf_url(page, entry_index):
    links = page.locator("a").filter(has_text=re.compile(r"Ver Documento", re.IGNORECASE))
    link = links.nth(entry_index)
    href = link.get_attribute("href") or ""
    if ".pdf" in href.lower():
        return urllib.parse.urljoin(page.url, href)
    if href:
        return urllib.parse.urljoin(page.url, href)
    raise RuntimeError("No se pudo resolver la URL del PDF en CEDIJ.")


def process_entries(entries, collection, source, current_url, output_dir, store, opener, delay_seconds, catalog_entries):
    for idx, entry in enumerate(entries, start=1):
        pdf_url = entry.get("pdf_url")
        if not pdf_url:
            continue

        title = entry.get("title") or f"{collection}_{source}_{idx}"
        matched_catalog = catalog_match(title, catalog_entries) if collection == "codigos" else None
        decree = entry.get("decree")

        filename = make_filename(collection, source, title, decree, None, pdf_url)
        destination = output_dir / filename
        if store.has(pdf_url, str(destination)) and destination.exists():
            print(f"[{source.upper()}:{collection}] Ya existe: {filename}")
            continue

        print(f"[{source.upper()}:{collection}] Descargando {idx}/{len(entries)}: {title}")
        size = download_pdf(opener, pdf_url, destination, referer=current_url)
        row = {
            "source": source,
            "collection": collection,
            "title": title,
            "decree": decree,
            "category": entry.get("category"),
            "page_url": current_url,
            "pdf_url": pdf_url,
            "local_path": str(destination),
            "downloaded_at": now_iso(),
            "size_bytes": size,
            "catalog_match": matched_catalog.get("nombre_oficial") if matched_catalog else None,
        }
        store.add(row)
        time.sleep(delay_seconds)


def scrape_tsc_collection(page, collection, opener, output_dir, store, delay_seconds, max_pages, catalog_entries):
    current_url = COLLECTIONS[collection]["tsc"]
    page_number = 1
    page.goto(current_url, wait_until="load", timeout=DEFAULT_TIMEOUT_MS)

    while True:
        if max_pages and page_number > max_pages:
            break
        page.wait_for_selector("text=Descargar", timeout=DEFAULT_TIMEOUT_MS)
        entries = extract_tsc_entries(page)
        print(f"[TSC:{collection}] Página {page_number} con {len(entries)} resultados")
        process_entries(entries, collection, "tsc", page.url, output_dir, store, opener, delay_seconds, catalog_entries)
        store.update_source_state(f"tsc_{collection}", page_number, page.url)

        next_page = find_next_page(page, page_number)
        if not next_page:
            break
        if next_page["href"] and not next_page["href"].lower().startswith("javascript:"):
            page.goto(urllib.parse.urljoin(page.url, next_page["href"]), wait_until="load", timeout=DEFAULT_TIMEOUT_MS)
        else:
            next_page["locator"].click()
            page.wait_for_load_state("load", timeout=DEFAULT_TIMEOUT_MS)
        page_number += 1


def scrape_cedij_collection(page, collection, opener, output_dir, store, delay_seconds, max_pages, catalog_entries):
    current_url = COLLECTIONS[collection]["cedij"]
    page_number = 1
    page.goto(current_url, wait_until="load", timeout=DEFAULT_TIMEOUT_MS)

    while True:
        if max_pages and page_number > max_pages:
            break
        page.wait_for_selector("text=Ver Documento", timeout=DEFAULT_TIMEOUT_MS)
        raw_entries = extract_cedij_entries(page)
        entries = []
        for entry in raw_entries:
            try:
                entry["pdf_url"] = resolve_cedij_pdf_url(page, entry["result_index"])
                entries.append(entry)
            except Exception as exc:
                print(f"[CEDIJ:{collection}] Error resolviendo PDF para '{entry.get('title')}': {exc}")
        print(f"[CEDIJ:{collection}] Página {page_number} con {len(entries)} resultados válidos")
        process_entries(entries, collection, "cedij", page.url, output_dir, store, opener, delay_seconds, catalog_entries)
        store.update_source_state(f"cedij_{collection}", page_number, page.url)

        next_page = find_next_page(page, page_number)
        if not next_page:
            break
        if next_page["href"] and not next_page["href"].lower().startswith("javascript:"):
            page.goto(urllib.parse.urljoin(page.url, next_page["href"]), wait_until="load", timeout=DEFAULT_TIMEOUT_MS)
        else:
            next_page["locator"].click()
            page.wait_for_load_state("load", timeout=DEFAULT_TIMEOUT_MS)
        page_number += 1


def parse_args():
    parser = argparse.ArgumentParser(
        description="Descarga PDFs de leyes y codigos hondureños a disco local."
    )
    parser.add_argument(
        "--source",
        choices=["cedij", "tsc", "all"],
        default="all",
        help="Fuente a usar.",
    )
    parser.add_argument(
        "--collection",
        choices=["leyes", "codigos", "todo"],
        default="todo",
        help="Coleccion a descargar.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=r"Directorio destino. Por defecto: D:\LeyesHonduras",
    )
    parser.add_argument(
        "--catalog",
        default=str(Path(__file__).with_name("catalogo_codigos_honduras.json")),
        help="Catalogo objetivo de codigos hondureños.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Limite de paginas por fuente y coleccion para pruebas. 0 = sin limite.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Pausa entre descargas.",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Muestra el navegador para depuracion.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = ensure_output_dir(Path(args.output_dir))
    store = ManifestStore(output_dir)
    opener = build_opener()

    sources = ["cedij", "tsc"] if args.source == "all" else [args.source]
    collections = ["leyes", "codigos"] if args.collection == "todo" else [args.collection]
    catalog_entries = load_catalog(args.catalog)

    print(f"Salida: {output_dir}")
    print(f"Fuentes: {', '.join(sources)}")
    print(f"Colecciones: {', '.join(collections)}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not args.headful)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(DEFAULT_TIMEOUT_MS)

        try:
            for collection in collections:
                current_catalog = catalog_entries if collection == "codigos" else []
                for source in sources:
                    if source == "cedij":
                        scrape_cedij_collection(
                            page,
                            collection,
                            opener,
                            output_dir,
                            store,
                            args.delay_seconds,
                            args.max_pages or None,
                            current_catalog,
                        )
                    else:
                        scrape_tsc_collection(
                            page,
                            collection,
                            opener,
                            output_dir,
                            store,
                            args.delay_seconds,
                            args.max_pages or None,
                            current_catalog,
                        )
        finally:
            context.close()
            browser.close()

    print("Scraping finalizado.")
    print(f"Manifest: {store.manifest_path}")
    print(f"Estado: {store.state_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Interrumpido por el usuario.")
    except urllib.error.URLError as exc:
        sys.exit(f"Error de red: {exc}")
    except Exception as exc:
        sys.exit(f"Fallo del scraper: {exc}")
