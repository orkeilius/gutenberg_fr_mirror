export interface Book {
    id: number;
    title: string;
    author: string;
    language: string;
}

export interface BookResult {
    book: Book;
    result : "ok" | "ko" | "skip"

}

export interface Metadata {
    generatedAt: string;
    source: string;
    mirrorUrl: string;
    totalBooks: number;
    books: Book[]
}
