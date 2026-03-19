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

// State map that contains the set of available fragments
const fragments = new Map();

// Redirect to the first fragment (a.k.a tree:view)
app.get('/ldes', (req, res) => {
    const sortedFragments = Array.from(fragments.values()).sort((a, b) => a.lastModified - b.lastModified);

    // Handle trailing slash
    const redirectPath = req.url.endsWith('/')
        ? req.url + sortedFragments[0].name
        : req.url + '/' + sortedFragments[0].name

    res.redirect(redirectPath);
});

// Route handler for a specific fragment
app.get('/ldes/:fragment', async (req, res) => {

    // Check that the requested node exist in the data folder. Respond with a 404 otherwise.
    if (fragments.has(req.params.fragment)) {

        // Read the data file and parse its content into RDF-JS quads
        const fragmentName = fragments.get(req.params.fragment).name;
        const store = new Store();
        const rdfStream = fs.createReadStream(path.join(__dirname, 'data', fragmentName)).pipe(new StreamParser());
        for await (const quad of rdfStream) {
            store.addQuad(quad);
        } 

        // Sort the fragments by last modified date
        const sortedFragments = Array.from(fragments.values()).sort((a, b) => a.lastModified - b.lastModified);
        // Get the index of the current fragment
        const index = sortedFragments.findIndex(f => f.name === fragmentName);

        // Add tree:view pointing to the first fragment
        store.addQuad(quad(
            namedNode(BASE_URL),
            namedNode(`${TREE}view`),
            // Add here a named node with the URL of the first fragment
        ));

        // Add tree:relation to the next fragment (if any) using the proper type (tree:GreaterThanRelation) 
        // and value (see findGreaterThanValue function below).


        // Set proper cache header ('Cache-Control: max-age=10' if this is the latest fragment 
        // and 'Cache-Control: immutable' otherwise)
        if (index === sortedFragments.length - 1) {
            // This is the latest fragment
        } else {
            // This is not the latest fragment
        }

    } else {
        res.status(404).send('Not found');
        return;
    }
});

// Start HTTP server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}/`);
});

// Function to monitor the data folder and refresh the state array
fs.watch(path.join(__dirname, 'data'), updateState);

// Function to update the state array
async function updateState() {
    console.log("Storage state change detected!");
    const files = await fsPromises.readdir(path.join(__dirname, 'data'));

    for (const fileName of files) {
        // Read the last modified time of this file
        const lastModified = (await fsPromises.stat(path.join(__dirname, 'data', fileName))).mtimeMs;
        // Add it to the state map
        fragments.set(fileName, { name: fileName, lastModified });
    }
}

// Function to extract the member timestamps from a RDF-JS Store and return the most recent one
function findGreaterThanValue(store) {
    const values = store.getQuads(null, `${DCT}modified`).map(q => {
        return new Date(q.object.value).getTime();
    });

    values.sort((a, b) => b - a);
    return new Date(values[0]).toISOString();
}

// Initialize the state array at start up
updateState();
