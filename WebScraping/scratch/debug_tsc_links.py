from playwright.sync_api import sync_playwright
import json
import re

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        # TSC index.php/leyes?start=90 is usually page 10 (10 items/page)
        page.goto('https://www.tsc.gob.hn/biblioteca/index.php/leyes?start=90')
        page.wait_for_selector('text=Descargar')
        
        links = page.evaluate('''() => {
            return Array.from(document.querySelectorAll("a")).map(a => ({
                text: a.innerText.trim(), 
                href: a.href,
                html: a.outerHTML
            }));
        }''')
        
        # Filter links that might be mistaken for "11"
        candidates = [l for l in links if re.search(r'\b11\b', l['text'])]
        
        print(json.dumps(candidates, indent=2))
        browser.close()

if __name__ == "__main__":
    run()
