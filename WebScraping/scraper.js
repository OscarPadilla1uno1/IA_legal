const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

let OUTPUT_DIR = path.resolve('D:\\Sentencias');

function sanitizeFilename(text) {
    if (!text) return '';
    let clean = text.trim().replace(/[\s\-\/,.]+/g, '_');
    clean = clean.replace(/[\\:*?"<>|]/g, '');
    return clean;
}

(async () => {
    if (!fs.existsSync(OUTPUT_DIR)) {
        try {
            fs.mkdirSync(OUTPUT_DIR, { recursive: true });
        } catch (e) {
            OUTPUT_DIR = path.resolve('Output');
            if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
        }
    }

    const start_id = parseInt(process.argv[2], 10) || 1;
    const end_id = parseInt(process.argv[3], 10) || 1000000;
    
    let current_id = start_id;
    let consecutive_failures = 0;
    const max_consecutive_failures = 40; // Mayor tolerancia por si el chunk cayó en una zona vacía grande

    console.log(`[Agente ${start_id}-${end_id}] Iniciando chunk...`);

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ acceptDownloads: true });
    
    // Inject the Blob interception correctly before any navigation
    await context.addInitScript(() => {
        const originalCreate = URL.createObjectURL;
        window.__capturedBlobs = [];
        URL.createObjectURL = function(obj) {
            const url = originalCreate(obj);
            window.__capturedBlobs.push(url);
            return url;
        };
    });

    const page = await context.newPage();

    while (current_id <= end_id && consecutive_failures < max_consecutive_failures) {
        const url = `https://sij.poderjudicial.gob.hn/sentences/${current_id}`;
        
        // Verificar si ya fue procesado para retomar desde donde se quedó
        const existingFiles = fs.readdirSync(OUTPUT_DIR);
        if (existingFiles.some(f => f.includes(`_pag${current_id}.pdf`) && !f.includes(`TipoCertificadoPDF`))) {
            console.log(`[Agente ${start_id}-${end_id}] ID ${current_id} ya existe en disco. Omitiendo...`);
            current_id++;
            continue;
        }

        console.log(`==========================================`);
        console.log(`[Agente ${start_id}-${end_id}] Visitando ${url} ...`);

        try {
            await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
            await page.evaluate(() => { window.__capturedBlobs = []; });

            try {
                await page.waitForSelector("h2 strong, p:has-text('Sentencia Consultada no Existe o no es Pública')", { timeout: 15000 });
            } catch (e) {
                console.log(`[Agente ${start_id}-${end_id}] Timeout al cargar elementos en ID ${current_id}. Saltando...`);
                consecutive_failures++;
                current_id++;
                continue;
            }

            const notFoundCount = await page.locator("p:has-text('Sentencia Consultada no Existe o no es Pública')").count();
            if (notFoundCount > 0) {
                console.log(`[Agente ${start_id}-${end_id}] ID ${current_id} no existe. Saltando...`);
                consecutive_failures++;
                current_id++;
                continue;
            }

            consecutive_failures = 0;
            const sentenciaRaw = await page.locator("h2 strong").innerText();

            async function getFieldVal(labelText) {
                try {
                    const loc = page.locator(`xpath=//div[contains(@class, 'font-weight-bold') and contains(text(), '${labelText}')]/following-sibling::div/p`);
                    if (await loc.count() > 0) {
                        return (await loc.first().innerText()).trim();
                    }
                    return "";
                } catch (e) {
                    return "";
                }
            }

            let fresRaw = await getFieldVal('Fecha de resolución');
            if (!fresRaw) fresRaw = await getFieldVal('Fecha resolución');
            
            const magisRaw = await getFieldVal('Magistrado ponente');
            const mateRaw = await getFieldVal('Materia');
            const fsenteRaw = await getFieldVal('Fecha de sentencia recurrida');

            const sentenciaClean = sanitizeFilename(sentenciaRaw);
            const fresClean = sanitizeFilename(fresRaw);
            const magisClean = sanitizeFilename(magisRaw);
            const mateClean = sanitizeFilename(mateRaw);
            const fsenteClean = sanitizeFilename(fsenteRaw);

            let baseName = `Sentencia_${sentenciaClean}`;
            if (fresClean) baseName += `_FRes_${fresClean}`;
            if (magisClean) baseName += `_Magis_${magisClean}`;
            if (mateClean) baseName += `_Mate_${mateClean}`;
            if (fsenteClean) baseName += `_FSente_${fsenteClean}`;

            if (baseName.length > 200) {
                baseName = baseName.substring(0, 200);
            }

            const pdf1Name = `${baseName}_pag${current_id}.pdf`;
            const pdf2Name = `${baseName}_TipoCertificadoPDF_pag${current_id}.pdf`;
            const pdf1Path = path.join(OUTPUT_DIR, pdf1Name);
            const pdf2Path = path.join(OUTPUT_DIR, pdf2Name);

            // ====== Capturar el TipoCertificadoPDF nativo usando el boton =======
            const btnImprimir = page.locator("button, a").filter({ hasText: /^\s*IMPRIMIR\s*$/i });
            if (await btnImprimir.count() > 0) {
                await btnImprimir.first().click();
                try {
                    await page.locator("button:has-text('Si, continuar')").waitFor({ state: 'visible', timeout: 3000 });
                    await page.locator("button:has-text('Si, continuar')").click();
                } catch (e) {}

                // Waits for html2canvas buffer
                await page.waitForTimeout(6000);

                const blobs = await page.evaluate(() => window.__capturedBlobs);
                if (blobs && blobs.length > 0) {
                    const finalUrl = blobs[blobs.length - 1];
                    const base64String = await page.evaluate(async (blobUrl) => {
                        try {
                            const res = await fetch(blobUrl);
                            const blob = await res.blob();
                            return new Promise((resolve) => {
                                const reader = new FileReader();
                                reader.onloadend = () => resolve(reader.result.split(',')[1]);
                                reader.readAsDataURL(blob);
                            });
                        } catch(err) {
                            return null;
                        }
                    }, finalUrl);
                    
                    if (base64String) {
                        fs.writeFileSync(pdf2Path, Buffer.from(base64String, 'base64'));
                        console.log(`[Agente ${start_id}-${end_id}] ID ${current_id}: Certificado nativo descargado.`);
                    }
                }
            }

            // ====== Generar PDF 1 (METADATOS ONLY) ====
            await page.evaluate(() => {
                const selectors = ['app-header', '#footer', 'app-menu-side-bar', '.page-header', '#sentenceTextDiv', '.accordion', '.btn-modern'];
                selectors.forEach(selector => {
                    document.querySelectorAll(selector).forEach(el => el.remove());
                });
                document.querySelectorAll('span').forEach(span => {
                    if (span.innerText && span.innerText.includes('Nota: Si tiene problemas')) {
                        span.remove();
                    }
                });
            });

            console.log(`[Agente ${start_id}-${end_id}] ID ${current_id}: Web descargado.`);
            await page.pdf({ path: pdf1Path, format: 'A4', printBackground: true, scale: 0.8 });

        } catch (e) {
            console.log(`[Agente ${start_id}-${end_id}] Error en ID ${current_id}: ${e.message}`);
            consecutive_failures++;
        }

        current_id++;
    }

    await browser.close();
    console.log(`[Agente ${start_id}-${end_id}] Finalizado.`);
    
    // Si se salió por muchas fallos seguidos, mandar señal a maestro para dejar de spammear
    if (consecutive_failures >= max_consecutive_failures) {
        if (process.send) {
            process.send('STOP_MASTER');
        }
    }

})();
