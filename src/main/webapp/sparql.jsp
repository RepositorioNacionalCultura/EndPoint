<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta.3/css/bootstrap.min.css" integrity="sha384-Zug+QiDoJOrZ5t4lssLdxGhVrurbmBWopoEl+M6BdEfwnCJZtKxi1KgxUyJq13dy" crossorigin="anonymous">
    <title>Repositorio Nacional de Cultura</title>      
     <!-- Respond.js soporte de media queries para Internet Explorer 8 -->
    <!-- ie8.js EventTarget para cada nodo en Internet Explorer 8 -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/ie8/0.2.2/ie8.js"></script>
    <![endif]-->
</head>
<body>
    
    <main id="mainx" class="page">
        <%--<jsp:include page="nav.jsp"></jsp:include>--%>    
        
        <div style="padding-top: 30px;">
            <div class="container landing-wrapper">
                <section class="seccion-01" id="intro">
                    <div class="row">
                        <div class="col-md-12">
                            <header>
                                <h2>SPARQL Endpoint</h2>
                                <hr class="red" style="margin: 10px 0px 30px;">
                            </header>                             
                            <p>SPARQL es un lenguaje de consulta para RDF basado en comparación de patrones de triplas. Un patrón de triplas es similar a una tripleta RDF, excepto que cada sujeto, predicado y objeto puede ser una variable.</P>
                            <p>Favor de consultar los <a href="/ejemplos.jsp">ejemplos</a> y la <a href="http://www.w3.org/TR/sparql11-query/">documentacion</a> del SPARQL 1.1 Query Language. </p>
                            
                            <header>
                                <h2>SPARQL Query</h2>
                                <hr class="red" style="margin: 10px 0px 30px;">
                            </header>                             
                            <form action="/sparql" method="post">
                                <textarea style="width: 100%;height: 200px;" name="query">
PREFIX rdf: &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: &lt;http://www.w3.org/2002/07/owl#>
PREFIX rdfs: &lt;http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: &lt;http://www.w3.org/2001/XMLSchema#>
SELECT ?subject ?class
     WHERE { ?subject rdf:type ?class }                           
                                </textarea><br>
<!--                                <input type="hidden" name="body">
                                <button type="button" onclick="doPost(this.form)">Enviar-</button>-->
                                <input type="submit">
                                &nbsp;&nbsp;&nbsp;&nbsp;Formato:    
                                <select name="format">
                                    <option value="json">JSON</option>
                                    <option value="xml">XML</option>
                                    <option value="csv">CSV</option>
                                </select>                                   
                            </form>  
                            
                            <header>
                                <h2>Acerca de SPARQL:</h2>
                                <hr class="red" style="margin: 10px 0px 30px;">
                            </header>                              
                            <p>
                                <b>Sintaxis y clausulas SPARQL:</b>
                                <ul>
                                    <li>
                                        <b>Variables:</b> El nombre de una variable es precedido por el signo de interrogación "?". Identifica a la misma variable en cualquier lugar de una consulta.<br/>
                                        Ejemplos: ?codigo, ?recurso.
                                    </li>
                                    <li>
                                        <b>Nombre con Prefijo:</b> Permiten usar URIS de forma abreviada. Se definen al inicio mediante el uso de la palabra PREFIX seguido de una etiqueta que representa al prefijo finalizada con dos puntos ":" seguido de la IRI que asocia.<br/>
                                        Ejemplo:  PREFIX rdf:  http://www.w3.org/1999/02/22-rdf-syntax-ns#
                                    </li>
                                    <li>
                                        <b>Formas de consulta:</b> SPARQL tiene cuatro formas de consulta.<br/>
                                        <ul style="padding-left:20px;">
                                            <li><b>SELECT:</b> Devuelve todo, o un subconjunto de las variables vinculadas en una concordancia con un patrón de búsqueda.</li>
                                            <li><b>CONSTRUCT:</b> Devuelve un grafo RDF construido mediante la sustitución de variables en un conjunto de plantillas de tripleta.</li>
                                            <li><b>ASK:</b> Devuelve una variable booleana indicando si la combinaciï¿½n Sujeto-Predicado-Objeto de consulta existe en la ontología RDF consultada.</li>
                                            <li><b>DESCRIBE:</b> Devuelve un grafo RDF que describe los recursos encontrados.</li>
                                        </ul>
                                    </li>
                                    <li>
                                        <b>Clausulas:</b>
                                        <ul style="padding-left:20px;">
                                            <li><b>WHERE:</b> patrón de la consulta con una o más tripletas encerradas entre {}.</li>
                                            <li><b>DISTINCT:</b> asegura la unicidad de los resultados.</li>
                                            <li><b>FILTER:</b> permite imponer constricciones adicionales al patrón de búsqueda.</li>
                                            <li><b>ORDER BY:</b> permite ordenar los resultados obtenidos de acuerdo a parámetros específicos.</li>
                                            <li><b>LIMIT n:</b> permite limitar los resultados obtenidos a n triplas.</li>
                                            <li><b>OFFSET m:</b> permite retornar resultados a partir del registro m.</li>
                                        </ul>
                                    </li>
                                </ul>
                            </p> 
                            <p>
                                <b>Namespaces útiles:</b></br>
                                </br>PREFIX cc:  &lt;http:&#47;&#47;creativecommons.org&#47;ns#&gt;
                                </br>PREFIX crm:  &lt;http:&#47;&#47;www.cidoc-crm.org&#47;cidoc-crm&#47;&gt;
                                </br>PREFIX dbo:  &lt;http:&#47;&#47;dbpedia.org&#47;ontology&#47;&gt;
                                </br>PREFIX dc:   &lt;http:&#47;&#47;purl.org&#47;dc&#47;elements&#47;1.1&#47;&gt;
                                </br>PREFIX dcterms:   &lt;http:&#47;&#47;purl.org&#47;dc&#47;terms&#47;&gt;
                                </br>PREFIX owl:  &lt;http:&#47;&#47;www.w3.org&#47;2002&#47;07&#47;owl#&gt;
                                </br>PREFIX pcm:  &lt;http:&#47;&#47;pcm&#47;ontologies&#47;2017&#47;9&#47;onto#&gt;
                                </br>PREFIX rdf:  &lt;http:&#47;&#47;www.w3.org&#47;1999&#47;02&#47;22-rdf-syntax-ns#&gt;
                                </br>PREFIX rdfs:  &lt;http:&#47;&#47;www.w3.org&#47;2000&#47;01&#47;rdf-schema#&gt;
                                </br>PREFIX schema:  &lt;http:&#47;&#47;schema.org&#47;&gt;
                                </br>PREFIX skos:  &lt;http:&#47;&#47;www.w3.org&#47;2004&#47;02&#47;skos&#47;core#&gt;
                                </br>PREFIX time:  &lt;http:&#47;&#47;www.w3.org&#47;2006&#47;time#&gt;
                                </br>PREFIX xsd:  &lt;http:&#47;&#47;www.w3.org&#47;2001&#47;XMLSchema#&gt;
                            </p>                                 
                        </div>
                    </div>
                </section>                   
            </div>
        </div>
    </main>
</body>
<script type="text/javascript">
//    function doPost(forma){
//        var jsonbody = {
//            query:forma.query.value,
//            resultType:forma.format.value
//        };
//        
//        forma.body.value =  JSON.stringify(jsonbody);;
//        alert(forma.body.value);
//        forma.submit();
//        //alert(jsonbody);
//        
//    }
    
</script>
</html>

