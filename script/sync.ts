import fs from 'node:fs/promises';
import path from 'node:path';
import https from 'node:https';
import {URL} from 'node:url';
import {Book, Metadata, DownloadResult, BatchDownloadResult, BookUrl, encoding} from './types';

const MIRROR_URL = 'https://aleph.pglaf.org/';
const GUTINDEX_URL = MIRROR_URL + 'GUTINDEX.ALL';
const FILES_DIR = path.join(__dirname, '..', 'files');
const METADATA_FILE = path.join(__dirname, '..', 'metadata.json');
const CONCURRENT_DOWNLOADS = 10;

function fetchUrl(urlString: string): Promise<Buffer | null> {
    return new Promise((resolve, reject) => {
        try {
            urlString = urlString.trim();
            const parsedUrl = new URL(urlString);

            const options = {
                hostname: parsedUrl.hostname,
                port: parsedUrl.port,
                path: parsedUrl.pathname + parsedUrl.search,
                method: 'GET',
                timeout: 60000,
            };

            const req = https.request(options, handleResponse)
                .on('error', reject)
                .on('timeout', () => {
                    req.destroy();
                    reject(new Error('Request timeout'));
                })
                .end();

            function handleResponse(res: any) {

                if (res.statusCode == 404) {
                    resolve(null)
                }
                if (res.statusCode !== 200) {
                    reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
                    return;
                }

                let data = Buffer.alloc(0);
                res.on('data', (chunk: Buffer) => data = Buffer.concat([data, chunk]));
                res.on('end', () => resolve(data));
            }
        } catch (error) {
            reject(error);
        }
    });
}

function buildMirrorUrl(bookId: number): BookUrl[] {
    const idStr = bookId.toString();
    let dirPath = '';

    if (idStr.length > 1) {
        const dirDigits = idStr.slice(0, -1).split('');
        dirPath = dirDigits.join('/') + '/';
    }
    const basePath = `${MIRROR_URL}${dirPath}${bookId}/`;

    return [
        {url: `${basePath}${bookId}-8.txt`, encoding: encoding.UTF8},
        {url: `${basePath}${bookId}-8.txt`, encoding: encoding.LATIN1}, // some files are encoding in latin1 but with -8 suffix
        {url: `${basePath}${bookId}-0.txt`, encoding: encoding.LATIN1},
        {url: `${basePath}${bookId}.txt`, encoding: encoding.ASCII},
    ];
}

function isValidBookLine(line: string): boolean {
    return Boolean(line && line.length >= 10 && !line.startsWith('~') && !line.startsWith('='));
}

function isFrenchBook(lines: string[], lineIndex: number, bookRegex: RegExp): boolean {
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

function extractBookInfo(match: RegExpMatchArray): { id: number; title: string; author: string } {
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

    return {id, title, author};
}

function parseGutindex(content: string): Book[] {
    const lines = content.split('\n');
    const bookMap = new Map<number, Book>();
    const bookRegex = /^(.+?)\s{2,}(\d+[A-Z]?)$/;

    for (let i = 0; i < lines.length; i++) {
        const lineTrimmed = lines[i].trim();

        if (!isValidBookLine(lineTrimmed)) continue;

        const match = bookRegex.exec(lineTrimmed);
        if (!match) continue;

        if (!isFrenchBook(lines, i, bookRegex)) continue;

        const {id, title, author} = extractBookInfo(match);

        if (!Number.isNaN(id) && id > 0 && !bookMap.has(id)) {
            bookMap.set(id, {id, title, author, language: 'fr'});
        }
    }

    return Array.from(bookMap.values());
}

async function getAllBooks(): Promise<Book[]> {
    console.log('Getting gutindex...');

    try {
        const response = await fetchUrl(GUTINDEX_URL);
        if (response === null) {
            throw new Error('GUTINDEX.ALL not found (404)');
        }
        console.log('Parsing...');
        return parseGutindex(response.toString());
    } catch (error) {
        console.error(`Error while geting index: ${(error as Error).message}`);
        throw error;
    }
}

async function saveMetadata(books: Book[]): Promise<Metadata> {
    try {
        const metadata: Metadata = {
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
        console.error(`error while saving metadata: ${(error as Error).message}`);
        throw error;
    }
}

async function downloadBook(book: Book): Promise<DownloadResult> {
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
        return {success: true, skipped: true};
    } catch {
        // File doesn't exist, continue
    }

    try {
        await fs.mkdir(bookDir, {recursive: true});
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
            console.error(`[${bookId}] Error: ${(error as Error).message}`);
            return {success: false, skipped: false};
        }
    }

    const urls = buildMirrorUrl(bookId);


    for (let url of urls) {
        try {
            let contentBuffer = await fetchUrl(url.url);
            if (contentBuffer === null) {
                continue
            }
            const content = (contentBuffer.toString(url.encoding.valueOf() as BufferEncoding));

            if (content.includes("\uFFFD")) {
                console.warn(`[${bookId}] Bugged encoding in ${url.encoding.valueOf()}`)

            }

            await fs.writeFile(filepath, content, 'utf8');
            return {success: true, skipped: false};
        } catch (e) {
            console.error(`[${bookId}] Error downloading from ${url.url}: ${(e as Error).message}`);
        }
    }
    console.error(`[${bookId}] Failed to download from all URLs (${urls.map(u => u.url).join(', ')})`);
    return {success: false, skipped: false};
}

async function downloadBooksInBatches(books: Book[], concurrency: number = CONCURRENT_DOWNLOADS): Promise<BatchDownloadResult> {
    let downloaded = 0;
    let skipped = 0;
    let failed = 0;
    let completed = 0;
    const successfulBooks: Book[] = [];

    const total = books.length;
    let currentIndex = 0;
    const activeDownloads = new Set<Promise<void>>();

    const processBook = async (book: Book): Promise<void> => {
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

    const startNext = async (): Promise<void> => {
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

    return {downloaded, skipped, failed, successfulBooks};
}

async function syncBooks(): Promise<void> {
    const startTime = Date.now();

    await fs.mkdir(FILES_DIR, {recursive: true});

    const books = await getAllBooks();
    console.log(`${books.length} file found`);

    const {downloaded, skipped, failed, successfulBooks} = await downloadBooksInBatches(books);

    console.log('Saving metadata...');
    await saveMetadata(successfulBooks);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

    console.log(`=== Result ===
        downloaded: ${downloaded}
        skipped: ${skipped}
        failed: ${failed}
        total: ${books.length}
        duration: ${duration}s
    `);
}

if (require.main === module) {
    syncBooks().catch(error => {
        console.error('error:', error);
        process.exit(1);
    }).finally(() => {
            process.exit(0)
        }
    );
}

export {syncBooks};
