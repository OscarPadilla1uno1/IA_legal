from playwright.sync_api import sync_playwright
import sys

def debug_tsc_pagination():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.tsc.gob.hn/biblioteca/index.php/leyes", wait_until="load")
        page.wait_for_selector("text=Descargar")
        
        # Extract all anchors with text like '2' or 'Siguiente'
        links = page.evaluate('''() => {
            return Array.from(document.querySelectorAll("a")).map(a => ({
                text: a.innerText.trim(),
                href: a.href,
                aria: a.getAttribute("aria-label"),
                cls: a.className,
                html: a.outerHTML
            })).filter(l => l.text === "2" || l.text.includes("Siguiente") || l.aria && l.aria.includes("2"));
        }''')
        
        import json
        print(json.dumps(links, indent=2))
        browser.close()

if __name__ == "__main__":
    debug_tsc_pagination()
