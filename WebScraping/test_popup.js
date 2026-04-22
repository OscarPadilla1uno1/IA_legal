const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ acceptDownloads: true });
    const page = await context.newPage();
    
    await page.addInitScript(() => {
        const originalCreate = URL.createObjectURL;
        window.__capturedBlobs = [];
        URL.createObjectURL = function(obj) {
            const url = originalCreate(obj);
            window.__capturedBlobs.push(url);
            return url;
        };
    });

    await page.goto("https://sij.poderjudicial.gob.hn/sentences/1", { waitUntil: 'networkidle' });
    
    // Click IMPRIMIR
    await page.locator("button, a").filter({ hasText: /^\s*IMPRIMIR\s*$/i }).first().click();
    await page.locator('button:has-text("Si, continuar")').waitFor();
    await page.locator('button:has-text("Si, continuar")').click();
    
    // wait for html2canvas to finish usually ~5-10 seconds
    await page.waitForTimeout(5000);
    
    const blobs = await page.evaluate(() => window.__capturedBlobs);
    console.log("Captured Blobs:", blobs);
    
    if (blobs.length > 0) {
        const finalUrl = blobs[blobs.length - 1]; // last blob is usually the generated PDF
        console.log("Downloading blob:", finalUrl);
        const base64 = await page.evaluate(async (blobUrl) => {
             const res = await fetch(blobUrl);
             const blob = await res.blob();
             return new Promise((resolve) => {
                 const reader = new FileReader();
                 reader.onloadend = () => resolve(reader.result.split(',')[1]);
                 reader.readAsDataURL(blob);
             });
        }, finalUrl);
        fs.writeFileSync("test_js_pdf.pdf", Buffer.from(base64, 'base64'));
        console.log("PDF SAVED SUCCESSFULLY!");
    } else {
        console.log("No blobs captured!");
    }
    
    await browser.close();
})();
