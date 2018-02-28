<%-- 
    Document   : tree
    Created on : 23-feb-2018, 11:45:07
    Author     : juan.fernandez
--%>

<%@page import="java.util.HashMap"%>
<%@page import="com.hp.hpl.jena.ontology.OntProperty"%>
<%@page import="com.hp.hpl.jena.rdf.model.Statement"%>
<%@page import="org.json.JSONArray"%>
<%@page import="java.io.IOException"%>
<%@page import="org.json.XML"%>
<%@page import="org.json.JSONObject"%>
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
    JSONArray jsonA = new JSONArray();
    if (null != mgr.getOntology()) {
        OntModel ontology = mgr.getOntology();
        OntClass entidad = ontology.getOntClass("http://mdm/ontologies/2017/9/onto#E1_Entidad");
        HashMap<String, OntClass> hm = new HashMap();
        subClass(entidad, jsonA, hm);
        List<OntClass> it = ontology.listClasses().toList();
        if (it.size() > 0) {
            for (OntClass cls : it) {
                subClass(cls, jsonA, hm);
            }
        }
    }
%>
<!doctype html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
        <!--<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta.3/css/bootstrap.min.css" integrity="sha384-Zug+QiDoJOrZ5t4lssLdxGhVrurbmBWopoEl+M6BdEfwnCJZtKxi1KgxUyJq13dy" crossorigin="anonymous">-->
        <link href="/css/bootstrap.css" rel="stylesheet">
        <link href="/css/qunit-1.12.0.css" rel="stylesheet">
        <title>Ontología</title>
    </head>
    <body>

        <div class="container-fluid">
            <div class="row content">

                <div class="col-sm-5 sidenav">
                    <h2>Clases</h2>
                    <div id="ontology" class=""></div>
                </div>
                <div class="col-sm-7">

                    <div id_="content-wrapper">
                        <iframe id="main_content" name="main_content" src="resource.jsp" frameborder="0" scrolling_="no" style="overflow:hidden; border:none;" width="100%" height="100%"></iframe>              
                    </div>
                </div>
            </div>
        </div>
        <script src="/js/jquery.js"></script>
        <script src="/js/bootstrap-treeview.js"></script>
        <!--        <script src="/js/qunit-1.12.0.js"></script>-->
        <script type="text/javascript">
            $(document).ready(function () {

                var json = <%=jsonA.toString()%>;

                var $tree = $('#ontology').treeview({
                    color: "#428bca",
                    enableLinks: true,
                    data: json
                });
                
                $tree.treeview('collapseAll', {silent: $('#chk-expand-silent').is(':checked')});

                function fix_height() {
                    var h = 0;
                    $("#main_content").attr("height", (($(window).height()) - h) + "px");
                }
                $(window).resize(function () {
                    fix_height();
                }).resize();

            });

        </script>
    </body>
</html>
<%!
    public void subClass(OntClass clase, JSONArray jsonA, HashMap<String, OntClass> hm) {

        if (hm.containsKey(clase.getURI())) {
            return;
        }
        hm.put(clase.getURI(), clase);
        JSONObject jsonEntidad = new JSONObject();
        jsonA.put(jsonEntidad);
        jsonEntidad.put("text", clase.getLocalName());
        RDFNode comments = clase.getPropertyValue(RDFS.comment);

        String comm = "";
        if (null != comments) {
            comm = comments.asLiteral().getString();
        }

        if (null == comm || comm.isEmpty()) {
            comments = clase.getPropertyValue(DCTerms.description);

            if (null != comments) {
                comm = comments.asLiteral().getString();
            }
        }
        jsonEntidad.put("comment", comm);

        try {
            jsonEntidad.put("href", "/resource.jsp?uri=" + URLEncoder.encode(clase.getNameSpace() + clase.getLocalName(), StandardCharsets.UTF_8.name()));
            jsonEntidad.put("target","main_content");
        } catch (Exception e) {
        }

        List<OntClass> it = clase.listSubClasses(true).toList();
        if (it.size() > 0) {
            JSONArray jsonSub = new JSONArray();
            jsonEntidad.put("nodes", jsonSub);
            if (!it.isEmpty()) {
                for (OntClass cls : it) {
                    subClass(cls, jsonSub, hm);
                }
            }
        }
    }


%>