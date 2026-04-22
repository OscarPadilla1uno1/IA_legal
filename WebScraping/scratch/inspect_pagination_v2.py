from playwright.sync_api import sync_playwright
import sys

def debug_site(url, name, selector):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print(f"--- Debugging {name} ---")
        try:
            page.goto(url, wait_until="load", timeout=60000)
            page.wait_for_selector(selector)
            
            # Find all anchors that look like pagination
            html_debug = page.evaluate('''() => {
                const links = Array.from(document.querySelectorAll("a"));
                return links.filter(a => {
                    const text = a.innerText.trim();
                    return /^\\d+$/.test(text) || text.includes("...") || /Siguiente|Next|Sig|>/.test(text);
                }).map(a => {
                    let path = [];
                    let curr = a;
                    for(let i=0; i<6 && curr; i++) {
                        path.push(curr.tagName + (curr.className ? "." + curr.className.split(" ").join(".") : "") + (curr.id ? "#" + curr.id : ""));
                        curr = curr.parentElement;
                    }
                    return {
                        text: a.innerText.trim(),
                        href: a.href,
                        html: a.outerHTML,
                        path: path.join(" < ")
                    };
                });
            }''')
            
            for item in html_debug:
                print(f"Text: {item['text']}")
                print(f"Path: {item['path']}")
                print(f"HTML: {item['html']}")
                print("-" * 20)
        except Exception as e:
            print(f"Error debugging {name}: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    debug_site("https://legislacion.poderjudicial.gob.hn/sistemalegislacion/ConsultaRapida.aspx?cod=1&msg=Leyes", "CEDIJ", "text=Ver Documento")
    debug_site("https://www.tsc.gob.hn/biblioteca/index.php/leyes", "TSC", "text=Descargar")
