import GutenbergMirrorSync from '../script/sync';
import fs from 'node:fs/promises';
import path from 'node:path';
import {Book, BookResult, Metadata} from '../script/types';
import {before} from "node:test";


describe("fetchUrl()", () => {

    test("should return a string", async () => {
        var response = await new GutenbergMirrorSync().fetchUrl("https://aleph.pglaf.org/7/6/6/2/76624/76624-0.txt");

        expect(response).not.toBeNull();
        expect(response?.length).toBe(436652);
    })

    test("should return string with latin1 encoding", async () => {
        var response = await new GutenbergMirrorSync().fetchUrl("https://aleph.pglaf.org/5/8/4/7/58476/58476-8.txt");

        expect(response).not.toBeNull();
        expect(response).not.toContain("�")
        expect(response?.length).toBe(50882);


    })

    test("should return string with utf8 encoding", async () => {
        var response = await new GutenbergMirrorSync().fetchUrl("https://aleph.pglaf.org/7/7/8/7/77876/77876-0.txt",);

        expect(response).not.toBeNull();
        expect(response?.length).toBe(222151);
        expect(response).not.toContain("");
    })

    test("should return null for non-existing URL", async () => {
        var response = await new GutenbergMirrorSync().fetchUrl("https://localhost");

        expect(response).toBeNull();
    })

    test("should return null for 404 URL", async () => {
        var response = await new GutenbergMirrorSync().fetchUrl("https://aleph.pglaf.org/non-existing-file.txt");

        expect(response).toBeNull();
    })
})

describe("syncBooks()", () => {
    test("should return data", async () => {
        const sync = new GutenbergMirrorSync();
        const mockBooks: Book[] = [
            {id: 1, title: 'Test Book 1', author: 'Author 1', language: 'fr'},
            {id: 2, title: 'Test Book 2', author: 'Author 2', language: 'fr'},
            {id: 3, title: 'Test Book 3', author: 'Author 3', language: 'fr'},
            {id: 4, title: 'Test Book 4', author: 'Author 4', language: 'fr'}
        ];

        jest.spyOn(sync, 'getMetadata').mockResolvedValue(mockBooks);
        jest.spyOn(sync, 'downloadBooksInBatches').mockResolvedValue([
            {book: mockBooks[0], result: 'ok'},
            {book: mockBooks[1], result: 'ko'},
            {book: mockBooks[2], result: 'ok'},
            {book: mockBooks[3], result: 'skip'}


        ]);
        jest.spyOn(sync, 'saveMetadata').mockResolvedValue();
        jest.spyOn(fs, 'mkdir').mockResolvedValue(undefined);

        await sync.syncBooks();

        expect(sync.getMetadata).toHaveBeenCalled();
        expect(sync.downloadBooksInBatches).toHaveBeenCalledWith(mockBooks);
        expect(sync.saveMetadata).toHaveBeenCalledWith(
            [
                {id: 1, title: 'Test Book 1', author: 'Author 1', language: 'fr'},
                {id: 3, title: 'Test Book 3', author: 'Author 3', language: 'fr'},
                {id: 4, title: 'Test Book 4', author: 'Author 4', language: 'fr'}
            ]
        );
    })
})

describe("buildMirrorUrl()", () => {
    test("should return correct URL for book ID 12345", () => {
        const sync = new GutenbergMirrorSync();

        const url = sync.buildMirrorUrl(12345);

        expect(url[0]).toBe("https://aleph.pglaf.org/1/2/3/4/12345/12345-8.txt");
        expect(url[1]).toBe("https://aleph.pglaf.org/1/2/3/4/12345/12345-0.txt");
        expect(url[2]).toBe("https://aleph.pglaf.org/1/2/3/4/12345/12345.txt");
    })

})



