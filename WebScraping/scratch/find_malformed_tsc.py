from playwright.sync_api import sync_playwright
import json

def find():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        # Navigate to TSC page 23 (start=264)
        url = 'https://www.tsc.gob.hn/biblioteca/index.php/leyes?start=264'
        print(f"Navigating to {url}")
        page.goto(url)
        page.wait_for_load_state('load')
        
        # Extract items-row data
        items = page.evaluate('''() => {
            return Array.from(document.querySelectorAll('div.items-row'))
                .map(row => {
                    const text = row.innerText;
                    const links = Array.from(row.querySelectorAll('a')).map(a => ({
                        text: a.innerText,
                        href: a.href,
                        html: a.outerHTML
                    }));
                    return { text, links };
                });
        }''')
        
        found = [i for i in items if 'FONDO DE CAPITALIZACIÓN' in i['text']]
        print(json.dumps(found, indent=2))
        browser.close()

if __name__ == "__main__":
    find()
