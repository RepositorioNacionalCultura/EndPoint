<%@page pageEncoding="utf-8" %>
<%@ page import="com.hp.hpl.jena.ontology.OntClass" %>
<%@ page import="com.hp.hpl.jena.ontology.OntModel" %>
<%@ page import="com.hp.hpl.jena.ontology.OntModelSpec" %>
<%@ page import="com.hp.hpl.jena.rdf.model.RDFNode" %>
<%@ page import="com.hp.hpl.jena.vocabulary.DCTerms" %>
<%@ page import="com.hp.hpl.jena.vocabulary.RDFS" %>
<%@ page import="mx.gob.cultura.endpoint.rdf.OntologyManager" %>
<%@ page import="java.net.URL" %>
<%@ page import="java.net.URLEncoder" %>
<%@ page import="java.nio.charset.StandardCharsets" %>
<%@ page import="java.util.List" %>
<%
    String ontNS = "https://cultura.gob.mx/";
    URL url = this.getClass().getClassLoader().getResource("onto.owl");
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
        if (null != mgr.getOntology()) {
            OntModel ontology = mgr.getOntology();
            List<OntClass> it = ontology.listClasses().toList();

            if (!it.isEmpty()) {
    %>
    <h4>Clases <span class="badge badge-secondary"><%=it.size()%></span></h4>
    <%
        for(OntClass cls : it) {
            if (null != cls.getURI()) {
                //String name = cls.getNameSpace() + cls.getLocalName();
                String name = cls.getLocalName();
                RDFNode comments = cls.getPropertyValue(RDFS.comment);

                String comm = "";
                if (null != comments) {
                    comm = comments.asLiteral().getString();
                }

                if (null == comm || comm.isEmpty()) {
                    comments = cls.getPropertyValue(DCTerms.description);

                    if (null != comments) {
                        comm = comments.asLiteral().getString();
                    }
                }
    %>
    <div class="row my-3">
        <div class="col">
            <div class="card border-dark">
                <h5 class="card-header"><%= name %></h5>
                <div class="card-body">
                    <p class="card-text"><%= comm %></p>
                    <a href="resource.jsp?uri=<%= URLEncoder.encode(cls.getNameSpace() + cls.getLocalName(), StandardCharsets.UTF_8.name()) %>" class="btn btn-primary">Ver definición</a>
                </div>
            </div>
        </div>
    </div>
    <%
                    }
                }
            }
        }
    %>
</div>
</body>
</html>
