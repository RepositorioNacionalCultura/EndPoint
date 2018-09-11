/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.gob.cultura.endpoint.oai;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import mx.gob.cultura.commons.Util;
import mx.gob.cultura.endpoint.oai.transformers.ElasticOAIRecordTrasformer;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 *
 * @author rene.jara
 */
@Path("/oai-pmh")
public class OAIEndPoint {

    private static final Logger LOGGER = Logger.getLogger(OAIEndPoint.class);
    private static final RestHighLevelClient ELASTIC = Util.ELASTICSEARCH.getElasticClient();
    private static String indexName = getIndexName();
    public static final String REPO_INDEX = "cultura";
    public static final String REPO_INDEX_TEST = "cultura_test";
    private static final int RECORD_LIMIT = 100;
    public OAIEndPoint() {
    }

    @GET
    @Produces(MediaType.APPLICATION_XML + ";charset=UTF-8")
    public Response getOAIXMLRequest(@Context UriInfo context, @Context javax.servlet.http.HttpServletRequest request ) {
        MultivaluedMap<String, String> params = context.getQueryParameters();
        String serverUrl = request.getScheme() + "://" + request.getServerName() + ((request.getServerPort() != 80) ? (":" + request.getServerPort()) : "");
        serverUrl += "/open/oai-pmh?";
        serverUrl += request.getQueryString();
//System.out.println("servletUrl:"+serverUrl);
        Document doc = createOAIPMHEnvelope(serverUrl);
        int page = 0;

        String verb = params.getFirst("verb");
        String metadataPrefix = params.getFirst("metadataPrefix");
        String setSpec = params.getFirst("set");
        String token = params.getFirst("resumptionToken");
        String oaiid = params.getFirst("identifier");

        boolean hasVerb = null != verb && !verb.isEmpty();
        boolean hasToken = null != token && !token.isEmpty();
        boolean hasMetadataPrefix = null != metadataPrefix && !metadataPrefix.isEmpty();
        boolean hasIdentifier = null != oaiid && !oaiid.isEmpty();

        if (hasToken) {
            metadataPrefix = token.substring(0, token.lastIndexOf("_"));
            page = Integer.parseInt(token.substring(token.lastIndexOf("_") + 1, token.length()));
        }

        if (!hasVerb) {
            createErrorNode(doc, "badVerb", "Illegal verb");
            return Response.ok(documentToString(doc, false)).build();
        } else if (!hasToken && !hasMetadataPrefix) {
            createErrorNode(doc, "badArgument", verb + " must receive the metadataPrefix");
            return Response.ok(documentToString(doc, false)).build();
        }

        if (hasToken && hasMetadataPrefix) {
            createErrorNode(doc, "badArgument", "ResumptionToken cannot be sent together with from, until, metadataPrefix or set parameters");
            return Response.ok(documentToString(doc, false)).build();
        }

        if ("ListIdentifiers".equals(verb)) {
            //getNodeObjects(doc, metadataPrefix, page, true);
        } else if ("ListRecords".equals(verb)) {
            getListObjects(doc, metadataPrefix, setSpec, page);
        } else if ("GetRecord".equals(verb)) {
            if (!hasIdentifier) {
                createErrorNode(doc, "badArgument", "GetRecord verb requires the use of the parameters - identifier and metadataPrefix");
                return Response.ok(documentToString(doc, false)).build();
            }
            getObject(doc, metadataPrefix, oaiid);
        }

        return Response.ok(documentToString(doc, false)).build();
    }

     private void getListObjects(Document doc, String prefix, String setSpec, int page) {
        ArrayList<Node> records = new ArrayList<>();
        String rootElementTag = "ListRecords";
        //OAITransformer transformer;
        JSONObject result;

        result = getElasticObjects(setSpec, RECORD_LIMIT, page);
        //rootElementTag = "ListRecords";
        //transformer = new ElasticOAIDCRecordTransformer();
        ElasticOAIRecordTrasformer OAIRecord= new ElasticOAIRecordTrasformer(prefix);
        JSONArray jarray = result.getJSONArray("records");

        for (Object j : jarray){
            JSONObject record=(JSONObject)j;
            Node e = doc.importNode((Node) OAIRecord.transform(record), true);
            if (null != e) {
                records.add(e);
            }            
        }

        
        if (!records.isEmpty()) {
            Element listRecords = doc.createElement(rootElementTag);
            doc.getDocumentElement().appendChild(listRecords);

            for (Node n : records) {
                listRecords.appendChild(n);
            }

            if ((page * RECORD_LIMIT + records.size()) < result.getLong("total")) {
                Element token = doc.createElement("resumptionToken");
                token.setTextContent(prefix+"_"+String.valueOf(page + 1));
                token.setAttribute("completeListSize", String.valueOf( result.getLong("total")));
                token.setAttribute("cursor", String.valueOf(page * RECORD_LIMIT + records.size()));

                listRecords.appendChild(token);
            }
        } else {
            createErrorNode(doc, "noRecordsMatch", "");
        }
    }
     
    private void getObject(Document doc, String prefix, String oaiid) {
        ArrayList<Node> records = new ArrayList<>();
        String rootElementTag = "GetRecord";
        JSONObject result;

        result = getElasticObject(oaiid);
        ElasticOAIRecordTrasformer OAIRecord= new ElasticOAIRecordTrasformer(prefix);
        JSONArray jarray = result.getJSONArray("records");

        if (jarray.length()>0){
            JSONObject record=(JSONObject)jarray.get(0);
            Node e = doc.importNode((Node) OAIRecord.transform(record), true);
            if (null != e) {
                records.add(e);
            }            
        } 
        if (!records.isEmpty()) {
            Element getRecord = doc.createElement(rootElementTag);
            doc.getDocumentElement().appendChild(getRecord);

            getRecord.appendChild(records.get(0));
        } else {
            createErrorNode(doc, "idDoesNotExist", "");
        }
    }
    
    private Document createOAIPMHEnvelope(String request) {
        Document doc = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            doc = builder.newDocument();

            Element root = doc.createElement("OAI-PMH");
            root.setAttribute("xmlns", "http://www.openarchives.org/OAI/2.0/");
            root.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            root.setAttribute("xsi:schemaLocation", "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");

            Element responseDate = doc.createElement("responseDate");
            responseDate.setTextContent(sdf.format(new Date()));

            root.appendChild(responseDate);

            Element requestEl = doc.createElement("request");
            requestEl.setTextContent(request);

            root.appendChild(requestEl);

            doc.appendChild(root);
        } catch (ParserConfigurationException ex) {
            LOGGER.error("Creating OAIPMH envelope",ex);
        }
        return doc;
    }

    private void createErrorNode(Document doc, String errCode, String errDesc) {
        Element err = doc.createElement("error");
        err.setAttribute("code", errCode);
        err.setTextContent(errDesc);
        doc.getDocumentElement().appendChild(err);
    }
    
    private String documentToString(Document doc, boolean omitXMLDeclaration) {
        String ret = null;
        if (null != doc) {
            try {
                TransformerFactory transFactory = TransformerFactory.newInstance();
                Transformer transformer = transFactory.newTransformer();
                StringWriter buffer = new StringWriter();
                if (omitXMLDeclaration) {
                    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                }
                transformer.transform(new DOMSource(doc), new StreamResult(buffer));
                ret = buffer.toString();
            } catch (TransformerException ex) {
                LOGGER.error("Converting string",ex);
            }
        }

        return ret;
    }
    private JSONObject getElasticObjects(String setSpec,int limit, int page) {
//System.out.println("---------getElasticObjects---------------------------------------------"+limit+","+page);        
        JSONObject ret = new JSONObject();
        // @todo implementar las colecciones setSpec
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(QueryBuilders.matchAllQuery());

        //Set paging parameters
        ssb.from(page*RECORD_LIMIT);
        ssb.size(RECORD_LIMIT);

        //Set sort parameters
        ssb.sort("indexcreated", SortOrder.ASC);

        //Add source builder to request
        sr.source(ssb);

//System.out.println("---------try---------------------------------------------");        
        try {
            //Perform search
            SearchResponse resp = ELASTIC.search(sr);
            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", resp.getTook().toString());
                //Get hits
                SearchHits respHits = resp.getHits();
                SearchHit [] hits = respHits.getHits();

                ret.put("total", respHits.getTotalHits());
                ret.put("hits", hits.length);
                
                if (hits.length > 0) {
                    //Get records
                    JSONArray recs = new JSONArray();
                    for (SearchHit hit : hits) {
//System.out.println("---------for---------------------------------------------");                                
//System.out.println(hit.getSourceAsString());                        
                        JSONObject o = new JSONObject(hit.getSourceAsString());
                        o.put("_id", hit.getId());
                        recs.put(o);
                    }
                    ret.put("records", recs);

                }
            }
        } catch (IOException ex) {
            LOGGER.error("Getting elastic objects",ex);
        }
//System.out.println(ret);
       return ret;
    }
    
    private JSONObject getElasticObject(String id) {
//System.out.println("---------getElasticObject---------------------------------------------"+id);        
        JSONObject ret = new JSONObject();
        // @todo implementar las colecciones setSpec
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(QueryBuilders.matchQuery("oaiid",id));

        //Add source builder to request
        sr.source(ssb);

//System.out.println("---------try---------------------------------------------");        
        try {
            //Perform search
            SearchResponse resp = ELASTIC.search(sr);
            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", resp.getTook().toString());
                //Get hits
                SearchHits respHits = resp.getHits();
                SearchHit [] hits = respHits.getHits();

                if (hits.length > 0) {
                    JSONArray recs = new JSONArray();
                    //Get record
                    JSONObject o = new JSONObject(hits[0].getSourceAsString());
                    o.put("_id", hits[0].getId());
                    recs.put(o);
                    ret.put("records", recs);
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Getting elastic object",ex);
        }
//System.out.println(ret);
       return ret;
    }
    /**
     * Gets index name to work with according to environment configuration.
     *
     * @return Name of index to use.
     */
    public static String getIndexName() {
        if (Util.ENV_DEVELOPMENT.equals(Util.getEnvironmentName())) {
            indexName = REPO_INDEX_TEST;
        } else {
            indexName = REPO_INDEX;
        }

        return indexName;
    }    
}
