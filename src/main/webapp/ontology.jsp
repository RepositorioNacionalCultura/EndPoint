<%-- 
    Document   : ontology
    Created on : 15-feb-2018, 12:58:32
    Author     : juan.fernandez
--%>
<%@page import="java.util.HashMap"%>
<%@page import="com.hp.hpl.jena.vocabulary.DCTerms"%>
<%@page import="com.hp.hpl.jena.rdf.model.NodeIterator"%>
<%@page import="com.hp.hpl.jena.graph.Node"%>
<%@page import="com.hp.hpl.jena.rdf.model.Resource"%>
<%@page import="java.net.URLEncoder"%>
<%@page import="java.io.File"%>
<%@page import="com.hp.hpl.jena.ontology.OntProperty"%>
<%@page import="java.util.Comparator"%>
<%@page import="java.util.TreeSet"%>
<%@page import="com.hp.hpl.jena.rdf.model.RDFNode"%>
<%@page import="com.hp.hpl.jena.vocabulary.RDFS"%>
<%@page import="com.hp.hpl.jena.ontology.OntClass"%>
<%@page import="java.util.Iterator"%>
<%@page import="com.hp.hpl.jena.ontology.OntDocumentManager"%>
<%@page import="com.hp.hpl.jena.ontology.Ontology"%>
<%@page import="com.hp.hpl.jena.rdf.model.Model"%>
<%@page import="com.hp.hpl.jena.ontology.OntModel"%>
<%@page import="com.hp.hpl.jena.util.FileManager"%>
<%@page import="com.hp.hpl.jena.rdf.model.ModelFactory"%>
<%@page import="com.hp.hpl.jena.ontology.OntModelSpec"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%!    
    OntDocumentManager mgr = null;
    OntModel ontology = null;

    File tfile=null;

    public void init()
    {
        tfile=new File(getServletContext().getRealPath("/"));
        mgr = OntDocumentManager.getInstance();
        java.io.File[] files = tfile.listFiles();
        for (java.io.File file : files)
        {
            if (file.getName().endsWith(".owl"))
            {
                String name = file.getName();
                try
                {
                    //System.out.println("s1:"+"http://datosabiertos.gob.mx/ontology/"+name+" s2:"+file.getCanonicalPath());
                    mgr.addAltEntry("https://cultura.gob.mx/" + name, file.getCanonicalPath());
                    if(ontology==null)
                    {
                        ontology = mgr.getOntology("http://sparql.repositorionacionalcti.mx/" + name, OntModelSpec.OWL_MEM_TRANS_INF);
                    }else
                    {
                        ontology.addSubModel(mgr.getOntology("http://sparql.repositorionacionalcti.mx/" + name, OntModelSpec.OWL_MEM_TRANS_INF),false);
                    }
                } catch (java.io.IOException ioe)
                {
                    ioe.printStackTrace();
                }
                ontology.rebind();
            }
        }
    }

    String encodeURI(String uri)
    {
        return URLEncoder.encode(uri);
    }

    String getNodeString(RDFNode node, String def)
    {
        if (node != null)
        {
            return node.asLiteral().getString();
        }
        return def;
    }

    String nullValidate(Object obj, String def)
    {
        if (obj == null)
        {
            return def;
        }
        return obj.toString();
    }

    Iterator sortIterator(Iterator it)
    {
        TreeSet set = new TreeSet(new Comparator()
        {

            public int compare(Object o1, Object o2)
            {
                String s1 = nullValidate(o1, "");
                String s2 = nullValidate(o2, "");
                //System.out.println(s1+" "+s2);
                return s1.compareTo(s2);
            }
        });

        while (it.hasNext())
        {
            set.add(it.next());
        }
        return set.iterator();
    }
    
    String getNsURIPrefix(String uri)
    {
        String p=ontology.getNsURIPrefix(uri);
        return p!=null?p+":":"";
    }

    boolean filter(String name)
    {
        /*
        if (name.startsWith("swb:") || name.startsWith("swbres:") || name.startsWith("swbxf:"))
        {
            return true;
        }
        if (name.endsWith("Element") || name.startsWith("map:Select") || name.startsWith("map:Noticias"))
        {
            return true;
        }

        if (name.equals("map:AcuseRecibo") || name.equals("map:Bitacora") || name.equals("map:Usuario"))
        {
            return true;
        }
        if (name.equals("map:ConsolaEventoCapa") || name.equals("map:ContactoMetadatos") || name.equals("map:Favoritos"))
        {
            return true;
        }

        if (name.startsWith("map:Admin"))
        {
            return true;
        }
        */
        return false;
    }

%>
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Repositorio Nacional</title>      
     <!-- Respond.js soporte de media queries para Internet Explorer 8 -->
    <!-- ie8.js EventTarget para cada nodo en Internet Explorer 8 -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/ie8/0.2.2/ie8.js"></script>
    <![endif]-->
    <link href="/css/reponal.css" rel="stylesheet">
    <link href="/css/lightbox.css" rel="stylesheet">
    <link href="https://framework-gb.cdn.gob.mx/favicon.ico" rel="shortcut icon">
    <link href="https://framework-gb.cdn.gob.mx/assets/styles/main.css" rel="stylesheet">
</head>
<body>
    
    <main id="mainx" class="page">
        <jsp:include page="nav.jsp"></jsp:include>    
        
        <div style="padding-top: 30px;">
            <div class="container landing-wrapper">
                <section class="seccion-01" id="intro">
                    <div class="row">
                        <div class="col-md-12">
                                <%
                                    if (mgr == null)
                                    {
                                        init();
                                    }

                                    String scls = request.getParameter("cls");

                                    if (scls == null)
                                    {
                                %>
                            <header>
                                <h2>Ontologias</h2>
                                <hr class="red" style="margin: 10px 0px 30px;">
                            </header> 

                            <p>
                                Se cuenta con las siguientes ontolog&iacute;as:
                                <ul>
                                    <%
                                        String[] arch = tfile.list();
                                        for (String file : arch)
                                        {
                                            if (file.endsWith(".owl"))
                                            {
                                                out.println("<li><a href=\"/ontology/" + file + "\">http://sparql.repositorionacionalcti.mx/" + file + "</a></li>");
                                            }

                                        }
                                    %>
                                </ul>    
                            </p>
                                    
                            <h3>Clases</h3>
                            <ul>
                                <%
                                    Iterator<OntClass> it = sortIterator(ontology.listClasses());
                                    //Iterator<OntClass> it=ontology.listClasses();
                                    while (it.hasNext())
                                    {
                                        OntClass cls = it.next();
                                        if (cls.getURI() == null)
                                        {
                                            continue;
                                        }
                                        
                                        String name =  getNsURIPrefix(cls.getNameSpace()) + cls.getLocalName();
                                        String comm = getNodeString(cls.getPropertyValue(RDFS.comment), "");
                                        if(comm.length()==0)comm=getNodeString(cls.getPropertyValue(DCTerms.description), "");

                                        if ((comm != null && comm.startsWith("#")) || filter(name))
                                        {
                                            continue;
                                        }

                                %>
                                <li type="circle">
                                    <a href="?cls=<%=encodeURI(cls.getURI())%>"><%=name%></a><br/>
                                    <%=comm%>
                                </li>    
                                <%
                                    }
                                %>
                            </ul>
                            <%
                            } else
                            {
                                OntClass cls = ontology.getOntClass(scls);
                                String name = getNsURIPrefix(cls.getNameSpace()) + cls.getLocalName();
                                
                                HashMap<String,String> annos=new HashMap();
                                annos.put("rdfs:comment", getNodeString(cls.getPropertyValue(RDFS.comment), null));
                                annos.put("rdfs:label", getNodeString(cls.getPropertyValue(RDFS.label), null));
                                annos.put("dcterms:decription", getNodeString(cls.getPropertyValue(DCTerms.description), null));
                            %>
                                
                            <header>
                                <h2><font size="-1"><%=cls.getURI()%></font><br> Clase <%=name%></h2>
                                <hr class="red" style="margin: 10px 0px 30px;">
                            </header> 
                            
                            <p>
                            <table summary="" style="width: 100%;">
                                <thead
                                    <tr>
                                        <th align="left" colspan="2"><font size="+1">&nbsp;<b>Anotaciones</b></font></th>
                                    </tr>
                                </thead>
                                <tbody>
<%
    Iterator<String> ait=annos.keySet().iterator();
    while(ait.hasNext())
    {
        String title=ait.next();
        String comm=annos.get(title);
        if(comm==null)continue;
    
%>                                    
                                    <tr>
                                        <td>
                                            <b><font size="-1"><%=title%></font></b>
                                            <table style="width: 100%;">
                                                <tbody><tr><td><font size="-1"><%=comm%></font></td></tr></tbody>
                                            </table>
                                        </td>
                                    </tr>
<%
    }
%>                                    
                                </tbody>
                            </table>
                            <br/>                                                  
                            <table style="width: 100%;" summary="">
                                <thead>
                                    <tr>
                                        <th align="left" colspan="2"><font size="+1">&nbsp;<b>Axiomas de Clase</b></font></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr bgcolor="white">
                                        <td>
                                            <b><font size="-1">rdfs:subClassOf</font></b>
                                            <table style="width: 100%;">
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
                                                                String pname = getNsURIPrefix(pcls.getNameSpace()) + pcls.getLocalName();
                                                                if(ocls!=null)
                                                                {
                                                    %>                                                        
                                                    <tr>
                                                        <td><font size="-1"><a href="?cls=<%=encodeURI(pcls.getURI())%>"><%=pname%></a></font></td>
                                                    </tr>
                                                    <%
                                                                }else
                                                                {
                                                    %>                                                        
                                                    <tr>
                                                        <td><font size="-1"><%=pname%></font></td>
                                                    </tr>
                                                    <%                                                                    
                                                                }
                                                            }catch(Exception noe){noe.printStackTrace();}
                                                        }
                                                    %>                                                        
                                                </tbody>
                                            </table>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>                                    
                            <br/>                                
                            <table style="width: 100%;" summary="">
                                <thead>
                                    <tr>
                                        <th align="left" colspan="2"><font size="+1">&nbsp;<b>Propiedades</b></font></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td>
                                            <table style="width: 100%;">
                                                <thead>
                                                    <tr>
                                                        <th><font size="-1">Propiedad</font></th>
                                                        <th><font size="-1">Rango</font></th>
                                                        <th><font size="-1">Dominio</font></th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <%
                                                        Iterator<OntProperty> it = cls.listDeclaredProperties(false);
                                                        while (it.hasNext())
                                                        {
                                                            OntProperty prop = it.next();
                                                            if (prop.getRange() == null || prop.getDomain() == null)
                                                            {
                                                                continue;
                                                            }
                                                            String pname = getNsURIPrefix(prop.getNameSpace()) + prop.getLocalName();
                                                            String range = getNsURIPrefix(prop.getRange().getNameSpace()) + prop.getRange().getLocalName();
                                                            String domain = getNsURIPrefix(prop.getDomain().getNameSpace()) + prop.getDomain().getLocalName();
                                                            //String pcomm = getNodeString(prop.getPropertyValue(RDFS.comment), "");

                                                    %>                                                        
                                                    <tr>
                                                        <td><font size="-1"><%=pname%></font></td>
                                                        <% if (prop.isDatatypeProperty())
{%>                                                            
                                                        <td><font size="-1"><%=range%></font></td>
                                                        <%} else
{%>                                                            
                                                        <td><font size="-1"><a href="?cls=<%=encodeURI(prop.getRange().getURI())%>"><%=range%></a></font></td>
                                                        <%}%>                                                            
                                                        <td><font size="-1"><a href="?cls=<%=encodeURI(prop.getDomain().getURI())%>"><%=domain%></a></font></td>
                                                    </tr>
                                                    <%
                                                        }
                                                    %>                                                        
                                                </tbody>
                                            </table>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>     
                            </p>
                            <%
                                }


                            %>
                        </div>
                    </div>
                </section>                   
            </div>
        </div>
    </main>
    <div class="space_bottom clearfix vertical-buffer"></div>
    <!-- Include all compiled plugins (below), or include individual files as needed --> 
    <script src="/js/lightbox.js"></script>
    <script src="https://framework-gb.cdn.gob.mx/gobmx.js"></script>
    <script src="/js/index.js"></script>
    <script>
      (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
      (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
      m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
      })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
      ga('create', 'UA-78552137-2', 'auto');
      ga('send', 'pageview');
    </script>      
</body>
</html>