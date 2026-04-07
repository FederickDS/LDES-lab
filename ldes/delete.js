const express = require('express');
const path = require('path');
const fsPromises = require('fs/promises');
const fs = require('fs');
const { DataFactory, StreamParser, Store, Writer } = require('n3');
const { quad, namedNode, blankNode, literal } = DataFactory;

const app = express();
const port = 3000;

const BASE_URL = "http://localhost:3000/ldes";

const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const XSD = "http://www.w3.org/2001/XMLSchema#";
const TREE = "https://w3id.org/tree#";
const DCT = "http://purl.org/dc/terms/";

// Map to store preloaded fragments
const fragments = new Map();

// Preload fragments at startup
async function loadFragments() {
    const files = await fsPromises.readdir(path.join(__dirname, 'data'));
    for (const fileName of files) {
        const filePath = path.join(__dirname, 'data', fileName);
        const stats = await fsPromises.stat(filePath);
        const store = new Store();

        // Read and parse RDF
        const rdfStream = fs.createReadStream(filePath).pipe(new StreamParser());
        for await (const q of rdfStream) store.addQuad(q);

        fragments.set(fileName, {
            name: fileName,
            lastModified: stats.mtimeMs,
            store
        });
    }
    console.log(`Loaded ${fragments.size} fragments into memory`);
}

// Watch for updates in the data folder
fs.watch(path.join(__dirname, 'data'), async (eventType, filename) => {
    if (!filename) return;
    console.log(`Data change detected: ${filename}`);
    await loadFragments(); // reload fragments on change
});

// Redirect to first fragment
app.get('/ldes', (req, res) => {
    const sorted = Array.from(fragments.values()).sort((a, b) => a.lastModified - b.lastModified);
    if (sorted.length === 0) return res.status(404).send('No fragments found');
    const redirectPath = req.url.endsWith('/') ? req.url + sorted[0].name : req.url + '/' + sorted[0].name;
    res.redirect(redirectPath);
});

// Function to get the most recent modified date from a store
function findGreaterThanValue(store) {
    const values = store.getQuads(null, `${DCT}modified`).map(q => new Date(q.object.value).getTime());
    values.sort((a, b) => b - a);
    return new Date(values[0]).toISOString();
}

// Serve a specific fragment
app.get('/ldes/:fragment', (req, res) => {
    const fragment = fragments.get(req.params.fragment);
    if (!fragment) return res.status(404).send('Fragment not found');

    const sorted = Array.from(fragments.values()).sort((a, b) => a.lastModified - b.lastModified);
    const index = sorted.findIndex(f => f.name === fragment.name);

    // Copy store and add tree:view
    const store = new Store(fragment.store.getQuads(null, null, null, null));
    store.addQuad(quad(
        namedNode(BASE_URL),
        namedNode(`${TREE}view`),
        namedNode(BASE_URL + '/' + sorted[0].name)
    ));

    // Set cache header
    if (index === sorted.length - 1) res.set('Cache-Control', 'max-age=10');
    else res.set('Cache-Control', 'immutable');

    // Add tree:relation to next fragment if exists
    if (index < sorted.length - 1) {
        const nextFragment = sorted[index + 1].name;
        const relationNode = blankNode('relation');

        store.addQuad(quad(
            namedNode(`${BASE_URL}/${fragment.name}`),
            namedNode(`${TREE}relation`),
            relationNode
        ));
        store.addQuad(quad(
            relationNode,
            namedNode(`${RDF}type`),
            namedNode(`${TREE}GreaterThanRelation`)
        ));
        store.addQuad(quad(
            relationNode,
            namedNode(`${TREE}node`),
            namedNode(`${BASE_URL}/${nextFragment}`)
        ));
        store.addQuad(quad(
            relationNode,
            namedNode(`${TREE}path`),
            namedNode(`${DCT}modified`)
        ));
        store.addQuad(quad(
            relationNode,
            namedNode(`${TREE}value`),
            literal(findGreaterThanValue(store), namedNode(`${XSD}dateTime`))
        ));
    }

    // Serialize to Turtle with prefixes
    const writer = new Writer({ prefixes: {
        dct: DCT,
        ldes: 'https://w3id.org/ldes#',
        person: 'http://www.w3.org/ns/person#',
        tree: TREE,
        xsd: XSD,
    } });

    writer.addQuads(store.getQuads());
    writer.end((error, result) => {
        if (error) return res.status(500).send(error.message);
        res.set('Content-Type', 'text/turtle');
        res.send(result);
    });
});

// Start server after loading fragments
loadFragments().then(() => {
    app.listen(port, () => console.log(`Server running at http://localhost:${port}/ldes`));
});