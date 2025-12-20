# [1.1.0](https://github.com/opencitations/sparqlite/compare/v1.0.0...v1.1.0) (2025-12-20)


### Features

* add timeout parameter to SPARQLClient [release] ([8bfd31a](https://github.com/opencitations/sparqlite/commit/8bfd31aa3ac3f01945d45fa1be4867b8782dac9c))

# 1.0.0 (2025-12-04)


* refactor!: remove rdflib dependency, return raw N-Triples from construct/describe ([4bf7773](https://github.com/opencitations/sparqlite/commit/4bf77732bc4e34bc2031835d02526383a6c720f3))


### Bug Fixes

* exclude benchmarks directory from pytest collection [release] ([d8ae0eb](https://github.com/opencitations/sparqlite/commit/d8ae0eb661ccae7f924fdf7e61fd57c75980a4e9))
* use main branch in semantic-release configuration [release] ([7c9c9aa](https://github.com/opencitations/sparqlite/commit/7c9c9aa461a248774f033c0e51a1dc806be3f311))


### Features

* initial implementation of sparqlite SPARQL 1.1 client ([ce0ed64](https://github.com/opencitations/sparqlite/commit/ce0ed64115d692139ea808341b1ae0f0f30bbf09))


### BREAKING CHANGES

* construct() and describe() now return raw bytes in
N-Triples format instead of rdflib.Graph objects. This removes the
rdflib dependency entirely, making sparqlite lighter.
