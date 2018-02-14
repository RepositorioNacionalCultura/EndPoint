# Search
APIs REST para servicio de búsqueda, SPARQL y RDF

## Quickstart

```sh
git clone https://github.com/RepositorioNacionalCultura/EndPoint.git
cd EndPoint
mvn clean && mvn package
java -jar target/dependency/webapp-runner.jar target/EndPoint.war  
```

Make search requests to http://localhost:8080/api/v1/search?q=XXX

Make SPARQL queries posting to http://localhost:8080/sparql

Make RDF queries to http://localhost:8080/rdf/