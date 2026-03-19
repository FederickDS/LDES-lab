# LDES and SHACL lab

## Task 1: Publish an LDES

Create and publish over HTTP an RDF resource (using your favorite serialization format) that defines an [LDES](https://w3id.org/ldes/specification) with the following characteristics:

- The LDES contains *versioned* members representing people modeled using the [EU Person Core vocabulary](https://semiceu.github.io/Core-Person-Vocabulary/releases/2.1.1/): `http://www.w3.org/ns/person#`.
- Use the `http://purl.org/dc/terms/modified` property as a `ldes:timestampPath` to annotate the time of change of a member.
- Use the `http://purl.org/dc/terms/isVersionOf` property as a `ldes:versionOfPath` to indicate the canonical identifier of a member.
- Publish a time-based fragmentation over your members, by having a data file per fragment.
- Dynamically add the [TREE hypermedia](https://w3id.org/tree/specification) triples (using an RDF/JS library) upon request.
- Set a caching validity of 10s (`Cache-Control: max-age=10`) for the newest fragment and mark as immutable (`Cache-Control: immutable`) all the rest.
- Test its correctness by running an LDES client over it to extract all its members:

    ```bash
    npx ldes-client http://localhost:3000/ldes
    ```

A HTTP response from an existing fragment, for example `http://localhost:3000/ldes/node_0.ttl`, should be equivalent to the following RDF data:

```turtle
@prefix dct: <http://purl.org/dc/terms/> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix person: <http://www.w3.org/ns/person#> .
@prefix tree: <https://w3id.org/tree#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://localhost:3000/ldes> a ldes:EventStream ;
    ldes:timestampPath dct:modified ;
    ldes:versionOfPath dct:isVersionOf ;
    tree:view <http://localhost:3000/ldes/node_0.ttl> ;
    tree:member <https://persons.org/john/v0>, <https://persons.org/john/v1> .

<http://localhost:3000/ldes/node_0.ttl> a tree:Node ;
    tree:relation [
        a tree:GreaterThanRelation ;
        tree:node <http://localhost:3000/ldes/node_1.ttl> ;
        tree:path dct:modified ;
        tree:value "2012-08-01T00:00:00.000Z"^^xsd:dateTime
    ] .
    
<https:/persons.org/john/v0> a person:Person;
    person:brithName "John Doe"@en;
    dct:alternativeName "Johnny Doe"@en;
    cv:domicile [
        a locn:Address;
        locn:fullAddress "123 Main St";
        locn:postName "Ghent"@en
    ];
    person:residency [
        a dct:Jurisdiction;
        dct:id op-country:BEL;
        rdfs:label "Belgium"@en
    ];
    dct:modified "1989-06-27T17:00:00.000Z"^^xsd:dateTime;
    dct:isVersionOf <http://persons.org/john>.

<https://persons.org/john/v1> a person:Person;
    person:brithName "John Doe"@en;
    dct:alternativeName "Johnny Doe"@en;
    cv:domicile [
        a locn:Address;
        locn:fullAddress "Avenida de Betanzos 79";
        locn:postName "Madrid"@es
    ];
    person:residency [
        a dct:Jurisdiction;
        dct:id op-country:ESP;
        rdfs:label "Spain"@en
    ];
    dct:modified "2012-08-01T00:00:00.000Z"^^xsd:dateTime;
    dct:isVersionOf <http://persons.org/john>.
```

### Application structure

In the [`ldes/`](./ldes) folder you will find an initial implementation that you can use as a starting point for fulfilling the above requirements.

It uses the [`ldes/data/`](./ldes/data) folder to store each individual fragment as an RDF file. The application works by monitoring this folder and maintaining an index in memory of the existing data files.

The [`ldes/index.js`](./ldes/index.js) file contains the main application logic, which consists of an Express.js server definition that must handle the following requests:

- `/ldes`: This route will redirect a client to the first and oldest data file (fragment) present in the `data` folder (already implemented).
- `/ldes/:fragment`: This route should return the contents of the corresponding data file for example `http://localhost:3000/ldes/node_0.ttl`, while adding the proper TREE hypermedia controls of a time-based fragmentation (**you need to implement this**).

### Run it

Install the dependencies with `npm install` and run it by executing the following command:

```bash
node index.js
```

## Task 2: Describe LDES members with SHACL and validate them

The next step consists on defining a [SHACL shape](https://www.w3.org/TR/shacl/) that validates the members of your LDES. Feel free to define the constraints as you wish, but the goal is to make sure all members pass the validation.

Define your shape in the [`shacl/shape.ttl`](./shacl/shape.ttl) file.

Once done, you can run a validation pipeline created with [RDF-Connect](https://github.com/RDF-Connect/RDF-Connect) to validate your LDES members with the following commands:

1. Install RDF-Connect: `npm install -g @rdfc/orchestrator-js`
2. Make sure your LDES is up and running at `http://localhost:3000/ldes`
3. Run the validation pipeline: `rdfc rdfc-pipeline.ttl`

If validation passes, you should be able to see the members logged in the console. Otherwise and error report will be shown.

> Hint: There is a subtle mistake in one of the members that you should find with the SHACL validation and fix it.

## Submit your solution

Package your solution as a zip file containing the `ldes/` and `shacl/` folders **but without the `node_modules` folders** and send it via email to <julianandres.rojasmelendez@ugent.be>.
