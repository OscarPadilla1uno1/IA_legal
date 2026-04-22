import urllib.request
import os
from pathlib import Path
import time

def download_pdf_fixed(pdf_url, destination):
    # The logic I just added to the main script
    if pdf_url.startswith("htt://"):
        print(f"Fixing malformed URL: {pdf_url}")
        pdf_url = pdf_url.replace("htt://", "https://", 1)
        print(f"New URL: {pdf_url}")
        
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-Agent", "Mozilla/5.0")]
    request = urllib.request.Request(pdf_url)
    
    try:
        with opener.open(request, timeout=20) as response:
            payload = response.read()
            with open(destination, "wb") as h:
                h.write(payload)
        print(f"Success! Downloaded {len(payload)} bytes.")
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

if __name__ == "__main__":
    malformed_url = "htt://www.tsc.gob.hn/leyes/Ley%20de%20Fondo%20de%20Capitalizac%C3%ADon%20del%20Sistema%20Financiero%20(Decreto%202-2008)%200001.pdf"
    dest = Path("test_malformed.pdf")
    download_pdf_fixed(malformed_url, dest)
    if dest.exists():
        os.remove(dest)
