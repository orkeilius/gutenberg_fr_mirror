const fs = require('node:fs').promises;
const path = require('node:path');
const https = require('node:https');
const http = require('node:http');
const { URL } = require('node:url');

const MIRROR_URL = 'https://aleph.pglaf.org/';
const GUTINDEX_URL = MIRROR_URL + 'GUTINDEX.ALL';
const FILES_DIR = path.join(__dirname, '..', 'files');
const METADATA_FILE = path.join(__dirname, '..', 'metadata.json');
const CONCURRENT_DOWNLOADS = 10;


function fetchUrl(urlString) {
    return new Promise((resolve, reject) => {
        try {
            urlString = urlString.trim();
            const parsedUrl = new URL(urlString);
            const client = parsedUrl.protocol === 'https:' ? https : http;

            const options = {
                hostname: parsedUrl.hostname,
                port: parsedUrl.port,
                path: parsedUrl.pathname + parsedUrl.search,
                method: 'GET',
                timeout: 30000,
            };

            const req = client.request(options, handleResponse);

            req.on('error', reject);
            req.on('timeout', () => {
                req.destroy();
                reject(new Error('Request timeout'));
            });

            req.end();

            function handleResponse(res) {
                if ([301, 302].includes(res.statusCode)) {
                    let redirectUrl = res.headers.location;
                    if (redirectUrl && !redirectUrl.startsWith('http')) {
                        redirectUrl = `${parsedUrl.protocol}//${parsedUrl.hostname}${redirectUrl}`;
                    }
                    return fetchUrl(redirectUrl).then(resolve).catch(reject);
                }

                if (res.statusCode !== 200) {
                    reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
                    return;
                }

                let data = '';
                res.setEncoding('utf8');
                res.on('data', chunk => data += chunk);
                res.on('end', () => resolve(data));
            }
        } catch (error) {
            reject(error);
        }
    });
}

async function ensureFilesDir() {
    try {
        await fs.mkdir(FILES_DIR, { recursive: true });
    } catch (error) {
        if (error.code !== 'EEXIST') throw error;
    }
}

function buildMirrorUrl(bookId) {
    const idStr = bookId.toString();
    let dirPath = '';

    if (idStr.length > 1) {
        const dirDigits = idStr.slice(0, -1).split('');
        dirPath = dirDigits.join('/') + '/';
    }

    const basePath = `${MIRROR_URL}${dirPath}${bookId}/`;

    return {
        utf8: `${basePath}${bookId}-0.txt`,
        latin1: `${basePath}${bookId}-8.txt`,
        plain: `${basePath}${bookId}.txt`
    };
}


function isValidBookLine(line) {
    return line && line.length >= 10 && !line.startsWith('~') && !line.startsWith('=');
}

function isFrenchBook(lines, lineIndex, bookRegex) {
    for (let j = 1; j <= 5 && (lineIndex + j) < lines.length; j++) {
        const nextLine = lines[lineIndex + j].trim().toLowerCase();

        if (nextLine.includes('[language: french]')) {
            return true;
        }
        if (bookRegex.test(nextLine)) {
            return false;
        }
    }

    return false;
}

function extractBookInfo(match) {
    let title = match[1].trim();
    let author = '';

    if (title.includes(', by ')) {
        [title, author] = title.split(', by ').map(s => s.trim());
    } else if (title.includes(', par ')) {
        [title, author] = title.split(', par ').map(s => s.trim());
    } else if (title.includes(' by ')) {
        const lastBy = title.lastIndexOf(' by ');
        author = title.substring(lastBy + 4).trim();
        title = title.substring(0, lastBy).trim();
    }

    const id = Number.parseInt(match[2].trim(), 10);

    return { id, title, author };
}

function parseGutindex(content) {
    const lines = content.split('\n');
    const bookMap = new Map();
    const bookRegex = /^(.+?)\s{2,}(\d+[A-Z]?)$/;

    for (let i = 0; i < lines.length; i++) {
        const lineTrimmed = lines[i].trim();

        if (!isValidBookLine(lineTrimmed)) continue;

        const match = lineTrimmed.match(bookRegex);
        if (!match) continue;

        if (!isFrenchBook(lines, i, bookRegex)) continue;

        const { id, title, author } = extractBookInfo(match);

        if (!Number.isNaN(id) && id > 0 && !bookMap.has(id)) {
            bookMap.set(id, { id, title, author, language: 'fr' });
        }
    }

    return Array.from(bookMap.values());
}

async function getAllBooks() {
    console.log('Geting gutindex...');

    try {
        const content = await fetchUrl(GUTINDEX_URL);
        console.log('Parsing...');
        const books = parseGutindex(content);
        return books;
    } catch (error) {
        console.error(`Error while geting index: ${error.message}`);
        throw error;
    }
}

async function saveMetadata(books) {
    try {
        const metadata = {
            generatedAt: new Date().toISOString(),
            source: MIRROR_URL,
            mirrorUrl: MIRROR_URL,
            totalBooks: books.length,
            books: books.map(book => ({
                id: book.id,
                title: book.title,
                author: book.author,
                language: book.language
            }))
        };

        await fs.writeFile(METADATA_FILE, JSON.stringify(metadata, null, 2), 'utf8');
        console.log(`Downloaded gutindex`);
        return metadata;
    } catch (error) {
        console.error(`error while saving metadata: ${error.message}`);
        throw error;
    }
}

async function downloadBook(book) {
    const bookId = book.id;

    const idStr = bookId.toString();
    let dirPath = '';
    if (idStr.length > 1) {
        const dirDigits = idStr.slice(0, -1).split('');
        dirPath = dirDigits.join('/');
    }

    const bookDir = path.join(FILES_DIR, dirPath, bookId.toString());
    const filepath = path.join(bookDir, `${bookId}.txt`);

    try {
        await fs.access(filepath);
        return { success: true, skipped: true };
    } catch {
        // File doesn't exist, continue
    }

    try {
        await fs.mkdir(bookDir, { recursive: true });
    } catch (error) {
        if (error.code !== 'EEXIST') {
            console.error(`[${bookId}] Error: ${error.message}`);
            return { success: false, skipped: false };
        }
    }

    const urls = buildMirrorUrl(bookId);
    const urlsToTry = [urls.utf8, urls.latin1, urls.plain];

    for (let i = 0; i < urlsToTry.length; i++) {
        try {
            const content = await fetchUrl(urlsToTry[i]);
            await fs.writeFile(filepath, content, 'utf8');
            return { success: true, skipped: false };
        } catch (error) {
            if (i === urlsToTry.length - 1) {
                console.error(`[${bookId}] error: ${error.message}`);
                return { success: false, skipped: false };
            }
        }
    }

    return { success: false, skipped: false };
}

async function downloadBooksInBatches(books, concurrency = CONCURRENT_DOWNLOADS) {
    let downloaded = 0;
    let skipped = 0;
    let failed = 0;
    let completed = 0;
    const successfulBooks = [];

    const total = books.length;
    let currentIndex = 0;
    const activeDownloads = new Set();

    const processBook = async (book) => {
        const result = await downloadBook(book);

        if (result.success) {
            result.skipped ? skipped++ : downloaded++;
            successfulBooks.push(book);
        } else {
            failed++;
        }

        completed++;

        if (completed % 100 === 0 || completed === total) {
            console.log(`[${completed}/${total}] donwload ${downloaded} | skiped ${skipped} | failed ${failed}`);
        }
    };

    const startNext = async () => {
        if (currentIndex >= books.length) return;

        const book = books[currentIndex];
        currentIndex++;

        const downloadPromise = processBook(book).finally(() => {
            activeDownloads.delete(downloadPromise);
            startNext();
        });

        activeDownloads.add(downloadPromise);
    };

    const initialPromises = [];
    for (let i = 0; i < Math.min(concurrency, books.length); i++) {
        initialPromises.push(startNext());
    }
    await Promise.all(initialPromises);

    while (activeDownloads.size > 0) {
        await Promise.race(activeDownloads);
    }

    return { downloaded, skipped, failed, successfulBooks };
}

async function syncBooks() {
    const startTime = Date.now();

    await ensureFilesDir();

    const books = await getAllBooks();
    console.log(`${books.length} file found`);

    const { downloaded, skipped, failed, successfulBooks } = await downloadBooksInBatches(books);

    console.log('Saving metadata...');
    await saveMetadata(successfulBooks);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

    console.log(f```=== Result ===
        downloaded: ${downloaded}
        skipped: ${skipped}
        failed: ${failed}
        total: ${books.length}
        duration: ${duration}s
    ```)
}

if (require.main === module) {
    syncBooks().catch(error => {
        console.error('error:', error);
        process.exit(1);
    });
}

module.exports = { syncBooks };
