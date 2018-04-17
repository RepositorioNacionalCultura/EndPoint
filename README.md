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

## Sample requests

### Full text search of word "oleo" 
https://localhost:8080/api/v1/search?q=oleo

### Get resource with id 8rMYJ2IBHvWlXGh3KeOq
https://localhost:8080/api/v1/search?identifier=8rMYJ2IBHvWlXGh3KeOq

### Full text search of three elements containing word "oleo"
https://localhost:8080/api/v1/search?q=oleo&size=3

### Full text search of three elements containing word "oleo" sorted by descending title
https://localhost:8080/api/v1/search?q=oleo&size=3&sort=-title

### Full text search of three elements containing word "oleo", starting from element 2
https://localhost:8080/api/v1/search?q=oleo&size=3&from=2
