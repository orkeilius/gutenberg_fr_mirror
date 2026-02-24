export interface Book {
    id: number;
    title: string;
    author: string;
    language: string;
}

export interface Metadata {
    generatedAt: string;
    source: string;
    mirrorUrl: string;
    totalBooks: number;
    books: Array<{
        id: number;
        title: string;
        author: string;
        language: string;
    }>;
}

export interface DownloadResult {
    success: boolean;
    skipped: boolean;
}

export interface BatchDownloadResult {
    downloaded: number;
    skipped: number;
    failed: number;
    successfulBooks: Book[];
}

export interface BookUrl{
    url: string;
    encoding: encoding;
}

export enum encoding {
    UTF8 = 'utf-8',
    LATIN1 = 'latin1',
    ASCII = 'ascii',
}

