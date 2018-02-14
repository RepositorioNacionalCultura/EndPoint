<%--
  User: hasdai
  Date: 23/01/18
--%>
<%@ page import="com.hp.hpl.jena.ontology.OntClass" %>
<%@ page import="com.hp.hpl.jena.ontology.OntModelSpec" %>
<%@ page import="com.hp.hpl.jena.rdf.model.RDFNode" %>
<%@ page import="mx.gob.cultura.sparql.OntologyManager" %>
<%@ page import="java.net.URL" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="com.hp.hpl.jena.rdf.model.Statement" %>
<%@ page import="com.hp.hpl.jena.util.iterator.ExtendedIterator" %>
<%@ page import="com.hp.hpl.jena.rdf.model.Resource" %>
<%@ page import="com.hp.hpl.jena.rdf.model.StmtIterator" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<%
    String uri = request.getParameter("uri");
    String ontNS = "https://cultura.gob.mx/";
    URL url = this.getClass().getClassLoader().getResource("ontsc.owl");
    OntologyManager mgr = new OntologyManager(ontNS);
    if (null != url) {
        mgr.loadFromURL(url, OntModelSpec.OWL_MEM_TRANS_INF);
    }
%>
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta.3/css/bootstrap.min.css" integrity="sha384-Zug+QiDoJOrZ5t4lssLdxGhVrurbmBWopoEl+M6BdEfwnCJZtKxi1KgxUyJq13dy" crossorigin="anonymous">
    <title>Ontología</title>
</head>
<body>
<div class="container">
    <%
        if (null != uri) {
            OntClass cls = mgr.getClass(uri);
            ArrayList<Statement> annotations = mgr.filterProperties(cls, OntologyManager.PropertyFilter.ANNOTATION);
    %>
    <div class="row">
        <div class="col">
            <h2><%= cls.getLocalName() %></h2>
            <p>URI: <%= uri %></p>
        </div>
    </div>
    <div class="row my-3">
        <div class="col">
            <h4>Anotaciones</h4>
            <hr>
            <table class="table table-bordered">
                <tbody>
                <%
                    for (Statement st : annotations) {
                        RDFNode node = st.getPredicate();
                %>
                <tr>
                    <td><a href="<%= node.asResource().getURI() %>"><%= node.getModel().shortForm(node.asResource().getURI()) %></a></td>
                    <td>
                        <%= st.getObject().asLiteral().getString() %>
                    </td>
                </tr>
                <%
                    }
                %>
                </tbody>
            </table>
        </div>
    </div>
    <div class="row my-3">
        <div class="col">
            <h4>DataType Properties</h4>
            <hr>
            <table class="table table-bordered">
                <tbody>
                <%
                    StmtIterator props = cls.asResource().listProperties();
                    while (props.hasNext()) {
                        Statement st = props.nextStatement();
                        System.out.println(st.getPredicate().toString()+" : "+st.getObject().toString());
                    }
                    for (Statement st : annotations) {
                        RDFNode node = st.getPredicate();
                %>
                <tr>
                    <td><a href="<%= node.asResource().getURI() %>"><%= node.getModel().shortForm(node.asResource().getURI()) %></a></td>
                    <td>
                        <%= st.getObject().asLiteral().getString() %>
                    </td>
                </tr>
                <%
                    }
                %>
                </tbody>
            </table>
        </div>
    </div>
    <%
        }
    %>
</div>
</body>
</html>
