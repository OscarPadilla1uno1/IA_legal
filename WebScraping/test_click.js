const { chromium } = require('playwright');
(async () => {
    const b = await chromium.launch();
    const c = await b.newContext({acceptDownloads: true});
    const p = await c.newPage();
    
    p.on('popup', async pop => {
        console.log('POPUP EVENT: ' + pop.url());
        try {
            await pop.waitForLoadState('load');
            await pop.waitForTimeout(2000);
            const html = await pop.content();
            console.log('POPUP HTML:', html.substring(0, 1500));
        } catch(e) {
            console.log("Error reading popup: ", e.message);
        }
    });

    await p.goto('https://sij.poderjudicial.gob.hn/sentences/1', {waitUntil:'networkidle'});

    const btn = p.locator('button, a').filter({ hasText: /^\s*IMPRIMIR\s*$/i });
    console.log("Button found:", await btn.count());
    await btn.first().click();
    console.log("Clicked IMPRIMIR");
    
    await p.locator('button:has-text("Si, continuar")').waitFor();
    await p.locator('button:has-text("Si, continuar")').click();
    console.log('Clicked confirm, waiting 20 seconds...');
    
    await p.waitForTimeout(20000);
    await b.close();
})();
