const express = require('express');
const { chromium } = require('playwright');
const ipaddr = require('ipaddr.js');
const dns = require('dns');
const { URL } = require('url');
const Redis = require('ioredis');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const app = express();
const port = 8888;

// --- Config ---
const MAX_CONCURRENT_PAGES = 5;
const REQUEST_TIMEOUT = 30000;
const REDIS_HOST = process.env.REDIS_HOST || 'redis:6379';
const REDIS_QUEUE_KEY = 'render_queue';
const REDIS_RESULT_CHANNEL_PREFIX = 'render_result:';
const BROWSER_RESTART_INTERVAL = 100;

// --- Redis Setup ---
const redis = new Redis(REDIS_HOST);
// We need distinct blocking clients for workers if we want them to act independently,
// but actually sharing one connection for BLPOP might be tricky if we want to interrupt.
// ioredis supports blocking commands, but using one connection for multiple BLPOPs concurrently?
// It queues them. So 5 workers calling BLPOP on the same client object works but it's one connection.
// If we want to STOP pulling, we can't easily cancel a pending BLPOP.
// However, we can use a small timeout for BLPOP to allow checking flags.
const blockingRedis = new Redis(REDIS_HOST);
const subscriberRedis = new Redis(REDIS_HOST);

redis.on('error', (err) => console.error('Redis Client Error', err));
blockingRedis.on('error', (err) => console.error('Redis Blocking Client Error', err));
subscriberRedis.on('error', (err) => console.error('Redis Subscriber Client Error', err));

// ----------------

let browser;
let activeRequests = 0;
let requestCount = 0;
let isRestarting = false;
const eventBus = new EventEmitter();

async function launchBrowser() {
    console.log('Launching new browser instance...');
    try {
        if (browser) {
            console.log('Closing old browser...');
            await browser.close().catch(e => console.error('Error closing browser:', e));
        }
        browser = await chromium.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
        console.log('Browser launched globally.');
    } catch (e) {
        console.error('Failed to launch browser:', e);
        process.exit(1);
    }
}

// Initialize browser
(async () => {
    await launchBrowser();
    processQueueLoop();
})();

// --- Result Handling ---
subscriberRedis.psubscribe(`${REDIS_RESULT_CHANNEL_PREFIX}*`, (err, count) => {
    if (err) console.error("Failed to subscribe: %s", err.message);
});

subscriberRedis.on('pmessage', (pattern, channel, message) => {
    const requestId = channel.replace(REDIS_RESULT_CHANNEL_PREFIX, '');
    eventBus.emit(requestId, JSON.parse(message));
});


// --- Worker Logic ---
async function processQueueLoop() {
    for (let i = 0; i < MAX_CONCURRENT_PAGES; i++) {
        worker(i);
    }
}

async function worker(id) {
    console.log(`Worker ${id} started`);
    while (true) {
        try {
            // Check if restarting
            if (isRestarting) {
                await new Promise(r => setTimeout(r, 500));
                continue;
            }

            // Use a timeout of 1 second for BLPOP to check flags regularly
            const result = await blockingRedis.blpop(REDIS_QUEUE_KEY, 1);
            
            if (result) {
                const jobDataStr = result[1];
                let jobData;
                try {
                    jobData = JSON.parse(jobDataStr);
                } catch (e) {
                    console.error('Failed to parse job data from Redis:', e);
                    continue;
                }

                const { url, requestId } = jobData;
                activeRequests++;
                requestCount++;
                
                try {
                    const content = await handleRenderLogic(url);
                    const responsePayload = { status: 'success', content };
                    await redis.publish(`${REDIS_RESULT_CHANNEL_PREFIX}${requestId}`, JSON.stringify(responsePayload));
                } catch (e) {
                    console.error(`Error processing ${url}:`, e);
                    const errorPayload = { status: 'error', message: e.message };
                    await redis.publish(`${REDIS_RESULT_CHANNEL_PREFIX}${requestId}`, JSON.stringify(errorPayload));
                } finally {
                    activeRequests--;
                    
                    // Trigger Restart if needed
                    if (requestCount >= BROWSER_RESTART_INTERVAL && !isRestarting) {
                        console.log(`Request limit reached (${requestCount}). Initiating browser restart...`);
                        isRestarting = true;
                        // We do NOT call launchBrowser here immediately.
                        // We must wait for other workers to finish.
                        initiateRestart();
                    }
                }
            }
        } catch (e) {
            console.error(`Worker ${id} error:`, e);
            await new Promise(r => setTimeout(r, 1000));
        }
    }
}

async function initiateRestart() {
    // Wait until all requests are done
    // Note: This function is called by the worker that triggered the limit.
    // activeRequests has been decremented by that worker already.
    // So we wait for other workers to finish.
    
    // Simple polling
    while (activeRequests > 0) {
        console.log(`Waiting for ${activeRequests} active requests to finish before restarting...`);
        await new Promise(r => setTimeout(r, 500));
    }

    console.log('All requests finished. Restarting browser now.');
    await launchBrowser();
    
    requestCount = 0;
    isRestarting = false;
    console.log('Browser restart complete. Resuming workers.');
}

// Check for private IPs (SSRF Protection)
function isPrivateIP(ip) {
    try {
        const addr = ipaddr.parse(ip);
        if (addr.range() !== 'unicast') { 
            const range = addr.range();
            return ['private', 'loopback', 'linkLocal', 'uniqueLocal', 'carrierGradeNat'].includes(range);
        }
        return false;
    } catch (e) {
        return true; 
    }
}

async function validateURL(urlString) {
    try {
        const url = new URL(urlString);
        const hostname = url.hostname;

        const lookup = await new Promise((resolve, reject) => {
            dns.lookup(hostname, { all: true }, (err, addresses) => {
                if (err) reject(err);
                else resolve(addresses);
            });
        });

        for (const addr of lookup) {
            if (isPrivateIP(addr.address)) {
                return false;
            }
        }
        return true;
    } catch (e) {
        console.error(`DNS lookup failed for ${urlString}:`, e);
        return false;
    }
}

async function handleRenderLogic(url) {
    console.log(`Processing URL: ${url}`);

    const isSafe = await validateURL(url);
    if (!isSafe) {
        throw new Error('Forbidden: Internal or Invalid URL');
    }

    let page;
    try {
        if (!browser) {
             throw new Error("Browser not available");
        }
        
        const context = await browser.newContext();
        page = await context.newPage();

        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: REQUEST_TIMEOUT });
        const content = await page.content();
        return content;

    } catch (error) {
        throw error;
    } finally {
        if (page) {
            await page.close().catch(() => {});
        }
    }
}

app.get('/render', async (req, res) => {
    const url = req.query.url;
    if (!url) {
        return res.status(400).send('URL is required');
    }

    const requestId = crypto.randomUUID();
    const jobData = { url, requestId };

    const timeout = setTimeout(() => {
        eventBus.removeAllListeners(requestId);
        res.status(504).send('Gateway Timeout');
    }, REQUEST_TIMEOUT + 5000); 

    eventBus.once(requestId, (result) => {
        clearTimeout(timeout);
        if (result.status === 'success') {
            res.send(result.content);
        } else {
            res.status(500).send(result.message || 'Error rendering page');
        }
    });

    try {
        await redis.rpush(REDIS_QUEUE_KEY, JSON.stringify(jobData));
    } catch (e) {
        clearTimeout(timeout);
        eventBus.removeAllListeners(requestId);
        console.error('Failed to enqueue job:', e);
        res.status(500).send('Internal Server Error');
    }
});

app.listen(port, () => {
    console.log(`JS Renderer listening at http://localhost:${port}`);
});
