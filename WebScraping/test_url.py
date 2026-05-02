import urllib.request
import urllib.parse

base_url = "https://www.tsc.gob.hn/biblioteca/index.php/codigos"
link = "/biblioteca/../web/leyes/Acuerdo-SAG-106-2022.pdf"
full_url = urllib.parse.urljoin(base_url, link)
print(f"Joined URL: {full_url}")

req_pdf = urllib.request.Request(
    full_url, 
    headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/pdf,*/*;q=0.8'
    }
)
try:
    with urllib.request.urlopen(req_pdf, timeout=20) as pdf_resp:
        content = pdf_resp.read()
        print(f"Exito. Tamaño: {len(content)} bytes")
except Exception as e:
    print(f"Error: {e}")
