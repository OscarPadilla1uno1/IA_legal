from playwright.sync_api import sync_playwright
import json

def find_law():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        
        # Try a few ranges
        found = False
        target_law = 'FONDO DE CAPITALIZACIÓN'
        
        # Search page by page until found or limit reached
        for start in range(0, 500, 12):
            url = f'https://www.tsc.gob.hn/biblioteca/index.php/leyes?start={start}'
            print(f"Checking {url}...")
            page.goto(url)
            page.wait_for_load_state('load')
            
            items = page.evaluate('''() => {
                return Array.from(document.querySelectorAll('div.items-row, tr'))
                    .map(row => row.innerText)
                    .filter(t => t.includes('FONDO DE CAPITALIZACIÓN'));
            }''')
            
            if items:
                print(f"FOUND ON PAGE with start={start}")
                # Detail the found item
                detail = page.evaluate('''() => {
                    const rows = Array.from(document.querySelectorAll('div.items-row, tr'))
                        .filter(row => row.innerText.includes('FONDO DE CAPITALIZACIÓN'));
                    return rows.map(row => {
                        const links = Array.from(row.querySelectorAll('a')).map(a => ({
                            text: a.innerText,
                            href: a.href,
                            html: a.outerHTML
                        }));
                        return { text: row.innerText, links };
                    });
                }''')
                print(json.dumps(detail, indent=2))
                found = True
                break
        
        if not found:
            print("Law not found in checked range.")
        browser.close()

if __name__ == "__main__":
    find_law()
