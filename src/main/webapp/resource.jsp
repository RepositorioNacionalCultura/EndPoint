<%--
  User: hasdai
  Date: 23/01/18
--%>
<%@page import="java.net.URLEncoder"%>
<%@page import="com.hp.hpl.jena.vocabulary.RDFS"%>
<%@page import="com.hp.hpl.jena.rdf.model.NodeIterator"%>
<%@page import="com.hp.hpl.jena.ontology.OntModel"%>
<%@page import="java.util.List"%>
<%@page import="com.hp.hpl.jena.ontology.OntProperty"%>
<%@page import="com.hp.hpl.jena.rdf.model.Literal"%>
<%@ page import="com.hp.hpl.jena.ontology.OntClass" %>
<%@ page import="com.hp.hpl.jena.ontology.OntModelSpec" %>
<%@ page import="com.hp.hpl.jena.rdf.model.RDFNode" %>
<%@ page import="mx.gob.cultura.endpoint.rdf.OntologyManager"%>
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
    URL url = this.getClass().getClassLoader().getResource("onto.owl");
    OntologyManager mgr = new OntologyManager(ontNS);
    if (null != url) {
        mgr.loadFromURL(url, OntModelSpec.OWL_MEM_TRANS_INF);
    }

    OntModel ontology = mgr.getOntology();

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
            <%        if (null != uri) {
                    OntClass cls = mgr.getClass(uri);
                    ArrayList<Statement> annotations = mgr.filterProperties(cls, OntologyManager.PropertyFilter.ANNOTATION);
                    //ArrayList<Statement> literals = mgr.filterProperties(cls, OntologyManager.PropertyFilter.LITERAL);
                    List<OntProperty> literals = cls.listDeclaredProperties(false).toList(); //mgr.filterProperties(cls, OntologyManager.PropertyFilter.DATATYPE);
                    StmtIterator objects = cls.listProperties(); //Axiomas de clase
                    //ArrayList<Statement> objects = mgr.filterProperties(cls, OntologyManager.PropertyFilter.OBJECT);
%>
            <div class="row">
                <div class="col">
                    <h2><%= cls.getLocalName()%></h2>
                    <p>URI: <%= uri%></p>
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
                                <td><a href="<%= node.asResource().getURI()%>"><%= node.getModel().shortForm(node.asResource().getURI())%></a></td>
                                <td>
                                    <%= st != null && st.getObject() != null && st.getObject().asLiteral() != null ? st.getObject().asLiteral().getString() : ""%>
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
                                for (OntProperty st : literals) {
                                    //RDFNode node = st.getLocalName();
%>
                            <tr>
                                <td><a href="<%= st.getURI()%>"><%= st.getLocalName()%></a></td>
                                <td>
                                    <%= st!=null&&st.getComment(null)!=null?st.getComment(null):""%>
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
                    <h4>Axiomas de clase</h4>
                    <hr>
                    <b>rdfs:subClassOf</b>
                    <table class="table table-bordered">
                        <tbody>
                            <%
                            //Iterator<OntClass> itc = cls.listSuperClasses();
                            NodeIterator itc = cls.listPropertyValues(RDFS.subClassOf);
                            while (itc.hasNext())
                            {
                            try
                            {
                            Resource pcls = itc.next().asResource();
                            if(pcls.getURI().equals(cls.getURI()))continue;
                            OntClass ocls=ontology.getOntClass(pcls.getURI());
                            String pname = ontology.getNsURIPrefix(pcls.getNameSpace()) + pcls.getLocalName();
                            if(ocls!=null)
                            {
                            %>                                                        
                            <tr>
                                <td><a href="?uri=<%=URLEncoder.encode(pcls.getURI())%>"><%=pname%></a></td>
                            </tr>
                            <%
                            } else {
                            %>                                                        
                            <tr>
                                <td><%=pname%></td>
                            </tr>
                            <%
                                    }
                                }
                                catch(Exception noe

                                                        
                                    ){noe.printStackTrace();
                                }
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
