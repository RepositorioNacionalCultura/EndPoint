# Search
APIs REST para servicio de búsqueda

## Quickstart

```sh
git clone https://github.com/RepositorioNacionalCultura/SearchEndPoint.git
cd EndPoint
mvn clean && mvn package
java -jar target/dependency/webapp-runner.jar target/SearchEndPoint.war  
```

Make search requests to http://localhost:8080/api/v1/search?q=XXX