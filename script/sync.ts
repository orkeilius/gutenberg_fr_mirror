import fs from 'node:fs/promises';
import path from 'node:path';
import axios from "axios";
import {Book, BookResult, Metadata} from './types';
import lodash from 'lodash';

const MIRROR_URL = 'https://aleph.pglaf.org/';
const GUTINDEX_URL = MIRROR_URL + 'GUTINDEX.ALL';
const FILES_DIR = path.join(__dirname, '..', 'files');
const METADATA_FILE = path.join(__dirname, '..', 'metadata.json');
const CONCURRENT_DOWNLOADS = 10;

export default class GutenbergMirrorSync {

    public async syncBooks(): Promise<void> {
        const startTime = Date.now();

        await fs.mkdir(FILES_DIR, {recursive: true});

        const books = await this.getMetadata();
        console.log(`${books.length} file found`);

        const result: BookResult[] = await this.downloadBooksInBatches(books)

        let downloaded = result
            .filter(b => b.result != "ko")
            .sort(b => b.book.id)
            .map(b => b.book);
        await this.saveMetadata(downloaded);

        const duration = ((Date.now() - startTime) / 1000).toFixed(2);

        console.log(`=== Result ===
        downloaded: ${result.filter(value => value.result === "ok").length}
        skipped: ${result.filter(value => value.result === "skip").length}
        failed: ${result.filter(value => value.result === "ko").length}
        total: ${books.length}
        duration: ${duration}s
    `);
    }


    async fetchUrl(url: string): Promise<string | null> {
        return axios.get(url, {
            responseType: 'arraybuffer',
            responseEncoding: 'binary'
        }).catch(error => {
            if (axios.isAxiosError(error) && error.response?.status === 404) {
                console.error(`Error fetching ${url}: ${(error as Error).message}`);
            }

            return null
        })
            .then(response => {
                    if (response?.status !== 200) {
                        console.error(`Failed to fetch ${url}: HTTP ${response?.status}`);
                        return null;
                    }
                    let decoded = this.decodeString(response.data);
                    if (decoded === null) {
                        console.warn(`Failed to decode content from ${url}`);
                    }
                    return decoded
                }
            )
    }

    decodeString(data: ArrayBuffer): string | null {
        const encodingArtefacts = ["\uFFFD", "Ã©", "�", "�", "�", "Ã§", "", ""];

        return lodash.chain(["utf-8", "iso-8859-1"])
            .map(encoding => new TextDecoder(encoding).decode(data))
            .find(string => !encodingArtefacts.some(artefact => string.includes(artefact)))
            .value() || null;

    }

    buildMirrorUrl(bookId: number): string[] {
        const idStr = bookId.toString();
        let dirPath = '';

        if (idStr.length > 1) {
            const dirDigits = idStr.slice(0, -1).split('');
            dirPath = dirDigits.join('/');
        }
        const basePath = `${MIRROR_URL}${dirPath}/${bookId}/`;

        return [
            `${basePath}${bookId}-8.txt`,
            `${basePath}${bookId}-0.txt`,
            `${basePath}${bookId}.txt`,
        ];
    }

    isFrenchBook(lines: string[], lineIndex: number, bookRegex: RegExp): boolean {
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

    extractBookInfo(match: RegExpMatchArray): { id: number; title: string; author: string } {
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

    parseGutindex(content: string): Book[] {
        const lines = content.split('\n');
        const bookMap = new Map<number, Book>();
        const bookRegex = /^(.+?)\s{2,}(\d+[A-Z]?)$/;

        for (let i = 0; i < lines.length; i++) {
            const lineTrimmed = lines[i].trim();

            let isValidBookLine = Boolean(lineTrimmed && lineTrimmed.length >= 10 && !lineTrimmed.startsWith('~') && !lineTrimmed.startsWith('='));
            if (!isValidBookLine) continue;

            const match = bookRegex.exec(lineTrimmed);
            if (!match) continue;

            if (!this.isFrenchBook(lines, i, bookRegex)) continue;

            const {id, title, author} = this.extractBookInfo(match);

            if (!Number.isNaN(id) && id > 0 && !bookMap.has(id)) {
                bookMap.set(id, {id, title, author, language: 'fr'});
            }
        }

        return Array.from(bookMap.values());
    }

    async getMetadata(): Promise<Book[]> {
        console.log('Getting gutindex...');

        try {
            const response = await axios.get(GUTINDEX_URL).then(r => r.data.toString());
            if (response === null) {
                throw new Error('GUTINDEX.ALL not found (404)');
            }
            console.log('Parsing...');
            return this.parseGutindex(response);
        } catch (error) {
            console.error(`Error while geting index: ${(error as Error).message}`);
            throw error;
        }
    }

    async saveMetadata(books: Book[]): Promise<void> {
        try {
            const metadata: Metadata = {
                generatedAt: new Date().toISOString(),
                source: MIRROR_URL,
                mirrorUrl: MIRROR_URL,
                totalBooks: books.length,
                books: books
            };

            await fs.writeFile(METADATA_FILE, JSON.stringify(metadata, null, 2), 'utf8');
            console.log(`Downloaded gutindex`);
        } catch (error) {
            console.error(`error while saving metadata: ${(error as Error).message}`);
            throw error;
        }
    }

    async downloadBook(book: Book): Promise<BookResult> {
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
            return {book: book, result: "skip"};
        } catch {
            // File doesn't exist, continue
        }

        try {
            await fs.mkdir(bookDir, {recursive: true});
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
                console.error(`[${bookId}] Error: ${(error as Error).message}`);
                return {book: book, result: "ko"};
            }
        }

        const urls = this.buildMirrorUrl(bookId);


        for (let url of urls) {
            try {
                let content = await this.fetchUrl(url);

                if (content === null) {
                    continue;
                }

                await fs.writeFile(filepath, content, 'utf8');
                return {book: book, result: "ok"};
            } catch (e) {
                console.error(`[${bookId}] Error downloading from ${url}: ${(e as Error).message}`);
            }
        }
        console.error(`[${bookId}] Failed to download from all URLs`);
        return {book: book, result: "ko"};
    }

    async downloadBooksInBatches(books: Book[], concurrency: number = CONCURRENT_DOWNLOADS): Promise<BookResult[]> {
        const bookResult: BookResult[] = [];

        let currentIndex = 0;
        const activeDownloads = new Set<Promise<void>>();

        const processBook = async (book: Book): Promise<void> => {
            const result: BookResult = await this.downloadBook(book);

            bookResult.push(result)

            if (bookResult.length % 100 === 0) {
                const ok = bookResult.filter(v => v.result === "ok").length;
                const skip = bookResult.filter(v => v.result === "skip").length;
                const ko = bookResult.filter(v => v.result === "ko").length;
                console.log(`[${bookResult.length}/${books.length}] downloaded: ${ok} | skipped: ${skip} | failed: ${ko}`);
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

        return bookResult;
    }
}

if (require.main === module) {
    (async () => {
        try {
            await new GutenbergMirrorSync().syncBooks();
            process.exit(0);
        } catch (error) {
            console.error('error:', error);
            process.exit(1);
        }
    })();
}