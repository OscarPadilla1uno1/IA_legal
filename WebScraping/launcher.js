const { fork } = require('child_process');

const CHUNK_SIZE = 1000;
const CONCURRENT_AGENTS = 5; 
const MAX_CONSECUTIVE_FAILURES_LIMIT = 50; // Increased to ensure big gaps don't kill chunks too early if needed

let currentChunkStart = 1;
let activeWorkers = 0;
let stopSpawning = false;

console.log(`Iniciando orquestador: ${CONCURRENT_AGENTS} agentes simultáneos, chunks de ${CHUNK_SIZE}`);

function spawnWorker() {
    if (stopSpawning) return;

    const startId = currentChunkStart;
    const endId = currentChunkStart + CHUNK_SIZE - 1;
    currentChunkStart += CHUNK_SIZE;

    console.log(`[Orquestador] Lanzando Agente para IDs ${startId} a ${endId}...`);
    
    const worker = fork('scraper.js', [startId.toString(), endId.toString()]);
    activeWorkers++;

    worker.on('message', (msg) => {
        if (msg === 'STOP_MASTER') {
            console.log(`[Orquestador] El Agente ${startId}-${endId} reportó el final de la base de datos (límite de fallos). Deteniendo orquestación.`);
            stopSpawning = true;
        }
    });

    worker.on('exit', (code) => {
        activeWorkers--;
        console.log(`[Orquestador] Agente ${startId}-${endId} finalizado. Activos: ${activeWorkers}`);
        
        if (!stopSpawning) {
            spawnWorker();
        } else if (activeWorkers === 0) {
            console.log("[Orquestador] Todos los agentes han terminado. Fin del trabajo.");
            process.exit(0);
        }
    });
}

// Start initial batch
for (let i = 0; i < CONCURRENT_AGENTS; i++) {
    spawnWorker();
}
