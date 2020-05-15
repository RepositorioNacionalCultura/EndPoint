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
import java.util.HashMap;
import java.util.Map;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
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
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

/**
 *
 * @author rene.jara
 */
@Path("/oaipmh")
public class OAIEndPoint {

    private static final Logger LOGGER = Logger.getLogger(OAIEndPoint.class);
    private static final RestHighLevelClient ELASTIC = Util.ELASTICSEARCH.getElasticClient();
    private static String indexName = getIndexName();
    public static final String REPO_INDEX = "cultura";
    public static final String REPO_INDEX_TEST = "cultura_test";
    private static final int RECORD_LIMIT = 100;
    private static final long SCROLL_TIMEOUT =33;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    public OAIEndPoint() {
    }
    public static final Map<String, Map> METADATA_FORMAT = new HashMap<>();
    static {
        Map<String, String> oai_dc =new HashMap<>();
        oai_dc.put("schema","http://www.openarchives.org/OAI/2.0/oai_dc.xsd");
        oai_dc.put("metadataNamespace","http://www.openarchives.org/OAI/2.0/oai_dc/");
        METADATA_FORMAT.put("oai_dc",oai_dc);
        Map<String, String> vra =new HashMap<>();
        oai_dc.put("schema","http://www.loc.gov/standards/vracore/vra-strict.xsd");
        oai_dc.put("metadataNamespace","http://www.vraweb.org/vracore4.htm");
        METADATA_FORMAT.put("vra",vra);                     
        //METADATA_FORMAT.put("marc", );
    }
    @GET
    @Produces(MediaType.APPLICATION_XML + ";charset=UTF-8")
    public Response getOAIXMLRequest(@Context UriInfo context, @Context javax.servlet.http.HttpServletRequest request ) {
        MultivaluedMap<String, String> params = context.getQueryParameters();
        String serverUrl = request.getScheme() + "://" + request.getServerName() + ((request.getServerPort() != 80) ? (":" + request.getServerPort()) : "");
        serverUrl += "/open/oaipmh";
        String queryString="?"+request.getQueryString();
        Document doc = createOAIPMHEnvelope(serverUrl+queryString);
        int page = 0;

        String verb = params.getFirst("verb");
        String metadataPrefix = params.getFirst("metadataPrefix");
        String set = params.getFirst("set");
        String token = params.getFirst("resumptionToken");
        String oaiid = params.getFirst("identifier");
        String sFrom = params.getFirst("from");
        String sUntil = params.getFirst("until");       
        Date from = null;
        Date until = null; 
        String scrollId = "";
        
        boolean hasVerb = null != verb && !verb.isEmpty();
        boolean hasToken = null != token && !token.isEmpty();
        boolean hasMetadataPrefix = null != metadataPrefix && !metadataPrefix.isEmpty();
        boolean hasSet = null != set && !set.isEmpty();
        boolean hasIdentifier = null != oaiid && !oaiid.isEmpty();
        boolean hasFrom = null != sFrom; 
        boolean hasUntil = null != sUntil;
        
        if (hasToken) {
            if (hasMetadataPrefix ||hasSet ||hasIdentifier || hasFrom || hasUntil) {
                createErrorNode(doc, "badArgument", "ResumptionToken cannot be sent together with from, until, metadataPrefix or set parameters");
                return Response.ok(documentToString(doc, false)).build();
            }            
            try{
                byte[] decodedBytes = Base64.getDecoder().decode(token);
                String[] tkel= (new String(decodedBytes)).split("\\|");
                //for(String s:tkel)System.out.println(s);
                metadataPrefix=tkel[0];
                hasMetadataPrefix=true;
                page=Integer.parseInt(tkel[1]);
                if(!" ".equals(tkel[2]))
                    set=tkel[2];
                scrollId=tkel[3];
                /*
                if(!" ".equals(tkel[3]))
                    from=DATETIME_FORMAT.parse(tkel[3]);
                if(!" ".equals(tkel[4]))
                    until=DATETIME_FORMAT.parse(tkel[4]);
                */
            }catch(Exception ex){
                ex.printStackTrace();
                createErrorNode(doc, "badResumptionToken", "The value of the resumptionToken argument is invalid");
                return Response.ok(documentToString(doc, false)).build();
            }
        }
        if(hasFrom){
            try {
                from =DATE_FORMAT.parse(sFrom);
                from =DATETIME_FORMAT.parse(sFrom);                
            } catch (Exception ignore) {
                //ex.printStackTrace();
            } 
            if(from==null){
                createErrorNode(doc, "badArgument", "The value of the from argument is invalid:"+sFrom);
                return Response.ok(documentToString(doc, false)).build();                            
            }
        }
        if(hasUntil){
            try {
                until =DATE_FORMAT.parse(sUntil);
                until =DATETIME_FORMAT.parse(sUntil);                
            } catch (Exception ignore) {
                //ex.printStackTrace();
            } 
            if(until==null){
                createErrorNode(doc, "badArgument", "The value of the until argument is invalid:"+sUntil);
                return Response.ok(documentToString(doc, false)).build();                
            }       
        }

        if (!hasVerb) {
            createErrorNode(doc, "badVerb", "Illegal verb");
            return Response.ok(documentToString(doc, false)).build();
        } 
        switch (verb) {
            case "Identify":
                if(hasIdentifier||hasMetadataPrefix||hasToken){
                    createErrorNode(doc, "badArgument ", "The request includes illegal arguments");
                    return Response.ok(documentToString(doc, false)).build();   
                }
                getIdentify(doc,serverUrl);
                break;
            case "ListMetadataFormats":
                if(hasToken||hasIdentifier){ //@todo quitar si se implementa
                    createErrorNode(doc, "badArgument ", "The request includes illegal arguments");
                    return Response.ok(documentToString(doc, false)).build();   
                }                
                listMetadata(doc);
                break;
            case "ListSets":
                if(hasMetadataPrefix ||hasSet ||hasIdentifier || hasFrom || hasUntil){
                    createErrorNode(doc, "badArgument ", "The request includes illegal arguments");
                    return Response.ok(documentToString(doc, false)).build();                
                }
                listSets(doc);
                break;    
            case "ListIdentifiers":
                if(hasIdentifier){
                    createErrorNode(doc, "badArgument", " The request includes illegal arguments or is missing required arguments.");
                    return Response.ok(documentToString(doc, false)).build();
                }else{
                    if(!hasMetadataPrefix){
                        createErrorNode(doc, "badArgument ", "The request includes illegal arguments or is missing required arguments.");
                        return Response.ok(documentToString(doc, false)).build(); 
                    
                    }
                    if(!METADATA_FORMAT.containsKey(metadataPrefix)){
                        createErrorNode(doc, "cannotDisseminateFormat ", "The value of the metadataPrefix argument is not supported by the repository.");
                        return Response.ok(documentToString(doc, false)).build(); 
                    }
                } 
                getListObjects(doc, metadataPrefix,set, from, until, page, scrollId, true);
                break;
            case "ListRecords":
                if(hasIdentifier){
                    createErrorNode(doc, "badArgument", " The request includes illegal arguments or is missing required arguments.");
                    return Response.ok(documentToString(doc, false)).build();
                }else{                                       
                    if(!hasMetadataPrefix){
                        createErrorNode(doc, "badArgument ", "The request includes illegal arguments or is missing required arguments.");
                        return Response.ok(documentToString(doc, false)).build(); 
                    
                    }
                    if(!METADATA_FORMAT.containsKey(metadataPrefix)){
                        createErrorNode(doc, "cannotDisseminateFormat ", "The value of the metadataPrefix argument is not supported by the repository.");
                        return Response.ok(documentToString(doc, false)).build(); 
                    }
                }    
                getListObjects(doc, metadataPrefix, set, from, until, page, scrollId, false);
                break;
            case "GetRecord":
                if(!hasMetadataPrefix||!hasIdentifier||hasToken||hasSet|hasFrom||hasUntil){
                    createErrorNode(doc, "badArgument ", "The request includes illegal arguments or is missing required arguments.");
                    return Response.ok(documentToString(doc, false)).build(); 

                }
                if(!METADATA_FORMAT.containsKey(metadataPrefix)){
                    createErrorNode(doc, "cannotDisseminateFormat ", "The value of the metadataPrefix argument is not supported by the repository.");
                    return Response.ok(documentToString(doc, false)).build(); 
                }
                getObject(doc, metadataPrefix, oaiid);
                break;
            default:
                createErrorNode(doc, "badVerb", "Illegal verb");
                return Response.ok(documentToString(doc, false)).build();                
        }

        return Response.ok(documentToString(doc, false)).build();
    }

     private void getListObjects(Document doc, String prefix, String set,Date from, Date until, int page, String scrollId, boolean onlyIdentifier) {
        ArrayList<Node> records = new ArrayList<>();
        String rootElementTag = "ListRecords";
        JSONObject result;

        result = getElasticObjects(set, from, until, scrollId);
        ElasticOAIRecordTrasformer OAIRecord= new ElasticOAIRecordTrasformer(prefix,onlyIdentifier);
        if(result.has("records")){
            JSONArray jarray = result.getJSONArray("records");

            for (Object j : jarray){
                JSONObject record=(JSONObject)j;
                Node e = doc.importNode((Node) OAIRecord.transform(record), true);
                if (null != e) {
                    records.add(e);
                }            
            }

        }
        if (!records.isEmpty()) {
            Element listRecords = doc.createElement(rootElementTag);
            doc.getDocumentElement().appendChild(listRecords);

            for (Node n : records) {
                listRecords.appendChild(n);
            }

            //if ((page * RECORD_LIMIT + records.size()) < result.getLong("total")&&
            if (result.has("scrollId")){
                StringBuilder sb=new StringBuilder(64);
                Element token = doc.createElement("resumptionToken");
                sb.append(prefix);
                sb.append("|");
                sb.append(String.valueOf(page + 1));
                sb.append("|");
                if(set!=null){
                    sb.append(set);
                }else{
                    sb.append(" ");
                }
                sb.append("|");
                sb.append(result.getString("scrollId"));
                /*if(from!=null){
                    sb.append(DATETIME_FORMAT.format(from));
                }else{
                    sb.append(" ");
                }
                sb.append("|");
                if(until!=null){
                    sb.append(DATETIME_FORMAT.format(until));
                }else{
                    sb.append(" ");
                }
                */
                if ((page * RECORD_LIMIT + records.size()) < result.getLong("total")){
                    byte[] encodedBytes = Base64.getEncoder().encode(sb.toString().getBytes());
                    token.setTextContent(new String(encodedBytes));
                    token.setAttribute("expirationDate",DATETIME_FORMAT.format((new Date().getTime())+(SCROLL_TIMEOUT*60*1000l)));
                }
                token.setAttribute("completeListSize", String.valueOf( result.getLong("total")));
                token.setAttribute("cursor", String.valueOf(page * RECORD_LIMIT + records.size()));                          
                listRecords.appendChild(token);
            }
        } else {
            createErrorNode(doc, "noRecordsMatch", "The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.");
        }
    }
     
    private void getObject(Document doc, String prefix, String oaiid) {
        ArrayList<Node> records = new ArrayList<>();
        String rootElementTag = "GetRecord";
        JSONObject result;

        result = getElasticObject(oaiid);
        ElasticOAIRecordTrasformer OAIRecord= new ElasticOAIRecordTrasformer(prefix,false);
        if(result.has("records")){
            JSONArray jarray = result.getJSONArray("records");

            if (jarray.length()>0){
                JSONObject record=(JSONObject)jarray.get(0);
                Node e = doc.importNode((Node) OAIRecord.transform(record), true);
                if (null != e) {
                    records.add(e);
                }            
            } 
        }    
        if (!records.isEmpty()) {
            Element getRecord = doc.createElement(rootElementTag);
            doc.getDocumentElement().appendChild(getRecord);

            getRecord.appendChild(records.get(0));
        } else {
            createErrorNode(doc, "idDoesNotExist", "The value of the identifier argument is unknown or illegal in this repository.");
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
    
    private Document getIdentify(Document doc, String url) {
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        Element root = doc.createElement("Identify");

        Element repositoryName = doc.createElement("repositoryName");
        repositoryName.setTextContent("Repositorio digital del patrimonio cultural de México");
        root.appendChild(repositoryName);

        Element baseURL = doc.createElement("baseURL");
        baseURL.setTextContent(url);
        root.appendChild(baseURL); 

        Element protocolVersion = doc.createElement("protocolVersion");
        protocolVersion.setTextContent("2.0");
        root.appendChild(protocolVersion);            

        Element earliestDatestamp = doc.createElement("earliestDatestamp");
        Date earliest=getElasticEarliestDate();
        if(earliest!=null){
            earliestDatestamp.setTextContent(sdf.format(earliest));
        }
        root.appendChild(earliestDatestamp);

        Element deletedRecord = doc.createElement("deletedRecord");
        deletedRecord.setTextContent("no");
        root.appendChild(deletedRecord); 

        Element adminEmail = doc.createElement("adminEmail");
        adminEmail.setTextContent("repositorio@cultura.gob.mx");
        root.appendChild(adminEmail); 
        
        Element granularity = doc.createElement("granularity");
        granularity.setTextContent("YYYY-MM-DDThh:mm:ssZ");
        root.appendChild(granularity);

        doc.getDocumentElement().appendChild(root);

        return doc;
    }
    
    private Document listMetadata(Document doc) {
   /*
        
        <ListMetadataFormats>
            <metadataFormat>
              <metadataPrefix>oai_dc</metadataPrefix>
              <schema>http://www.openarchives.org/OAI/2.0/oai_dc.xsd</schema>
              <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/</metadataNamespace>
            </metadataFormat>
        */
            Element root = doc.createElement("ListMetadataFormats");
            
            for (Map.Entry<String, Map> format : METADATA_FORMAT.entrySet()) {
                String key = format.getKey();
                Map<String,String> values = format.getValue();
            
                Element metadataFormat = doc.createElement("metadataFormat");
                root.appendChild(metadataFormat);
            
                Element metadataPrefix = doc.createElement("metadataPrefix");
                metadataPrefix.setTextContent(key);
                metadataFormat.appendChild(metadataPrefix); 
            
                Element schema = doc.createElement("schema");
                schema.setTextContent(values.get("schema"));
                metadataFormat.appendChild(schema);            
            
                Element metadataNamespace = doc.createElement("metadataNamespace");
                metadataNamespace.setTextContent(values.get("metadataNamespace"));
                metadataFormat.appendChild(metadataNamespace);
            }
            doc.getDocumentElement().appendChild(root);

        return doc;
    }
    private Document listSets(Document doc) {
        /*    
<ListSets>
  <set>
    <setSpec>music</setSpec>
    <setName>Music collection</setName>
  </set>

 </ListSets>
        
        */
        ArrayList<Node> records = new ArrayList<>();
        
        JSONObject result;

        result = getElasticObjectsHolderId();
        
        
        Element root = doc.createElement("ListSets");
        if(result.has("records")){
            JSONArray jarray = result.getJSONArray("records");
            for (Object j : jarray){
                JSONObject record=(JSONObject)j;
                if (!"null".equals(record.getString("key"))){
                    Element set = doc.createElement("set");
                    root.appendChild(set);

                    Element setSpec = doc.createElement("setSpec");
                    setSpec.setTextContent(record.getString("key"));
                    set.appendChild(setSpec); 

                    Element setName = doc.createElement("setName");
                    setName.setTextContent(record.getString("name"));
                    set.appendChild(setName);            
                }
            }
            doc.getDocumentElement().appendChild(root);
        } else {
            createErrorNode(doc, "noSetHierarchy","The repository does not support sets.");
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
    
    private JSONObject getElasticObjects(String set,Date from, Date until, String scrollId){//int page) {

        JSONObject ret = new JSONObject();
        SearchScrollRequest slr=null;
        SearchRequest sr=null;
        
        if(null!=scrollId&&!scrollId.isEmpty()){
            //Create search scroll request
            slr = new SearchScrollRequest(scrollId); 
            slr.scroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT));         
        }else{  
            //Create search request
            sr = new SearchRequest(indexName);

            //Create queryString query
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            BoolQueryBuilder query= QueryBuilders.boolQuery();
            query.must(QueryBuilders.existsQuery("culturaoaiid"));
            if(set!=null&&!set.isEmpty()){
                query.must(QueryBuilders.matchQuery("holderid", set));
            }
            if(from!=null){
                query.must(QueryBuilders.rangeQuery("indexcreated").from(from.getTime()));
            }        
            if(until!=null){
                query.must(QueryBuilders.rangeQuery("indexcreated").to(until.getTime()));
            }      
            ssb.query(query);
            //Set paging parameters
            //ssb.from(page*RECORD_LIMIT);
            ssb.size(RECORD_LIMIT);

            //Set sort parameters
            ssb.sort("indexcreated", SortOrder.ASC);
            //Add source builder to request
            sr.scroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT));
            sr.source(ssb);
        }
        try {
            //Perform search
            SearchResponse resp;
            if (slr!=null){
                resp= ELASTIC.searchScroll(slr);
            }else{
                resp= ELASTIC.search(sr);
            }

            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {                       
                ret.put("took", resp.getTook().toString());
                ret.put("scrollId", resp.getScrollId());
                //Get hits
                SearchHits respHits = resp.getHits();
                SearchHit [] hits = respHits.getHits();

                ret.put("total", respHits.getTotalHits());
                ret.put("hits", hits.length);
                
                if (hits.length > 0) {
                    //Get records
                    JSONArray recs = new JSONArray();
                    for (SearchHit hit : hits) {
                        JSONObject o = new JSONObject(hit.getSourceAsString());
                        o.put("_id", hit.getId());
                        recs.put(o);
                    }
                    ret.put("records", recs);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Getting elastic objects",ex);
        }
       return ret;
    }
    
    private JSONObject getElasticObject(String id) {
        JSONObject ret = new JSONObject();
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(QueryBuilders.matchQuery("culturaoaiid",id));

        //Add source builder to request
        sr.source(ssb);

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
       return ret;
       
    }
    /*
    
    deprecated
    */
    private JSONObject getElasticObjectsSets() {
    /*
    @todo
    hay que corregir el indice

https://www.elastic.co/guide/en/elasticsearch/reference/current/fielddata.html

    */

        JSONObject ret = new JSONObject();
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();

        //Build aggregations for faceted search
        TermsAggregationBuilder setAgg = AggregationBuilders.terms("setspc")
                .field("collection");
        ssb.aggregation(setAgg);        
        
        //Add source builder to request
        sr.source(ssb);
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
                        JSONObject o = new JSONObject(hit.getSourceAsString());
                        recs.put(o);
                    }
                    ret.put("records", recs);

                }
            }
        } catch (IOException ex) {
            LOGGER.error("Getting elastic objects sets",ex);
        }
       return ret;
    }
    
    private JSONObject getElasticObjectsHolderId() {

        JSONObject ret = new JSONObject();
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.size(0);
        //Build aggregations for faceted search
        TermsAggregationBuilder setAgg = AggregationBuilders.terms("setspc");
                //.field("holderid");
        Script script = new Script(
                    ScriptType.INLINE,
                    "painless",
                    "doc['holderid'].value+'|'+doc['holder.raw'].value",
                    Collections.emptyMap()
            );
        setAgg.script(script);
        setAgg.size(1000);
        ssb.aggregation(setAgg);        
        //Add source builder to request
        sr.source(ssb);
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
                ParsedStringTerms pst=resp.getAggregations().get("setspc");
                List<Bucket> buckets = (List<Bucket>) pst.getBuckets();
                if (!buckets.isEmpty()) {
                    //Get records
                    JSONArray recs = new JSONArray();
                    for (Bucket b:buckets) {
                        try{
                            String spec;
                            String name;
                            String[] split = b.getKeyAsString().split("\\|");                            
                            spec=split[0];
                            name=split[1];
                            JSONObject o = new JSONObject();
                            o.put("key",spec);
                            o.put("name",name);
                            recs.put(o);
                        }catch(Exception ignored){
                        }
                    }
                    ret.put("records", recs);

                }
            }
        } catch (IOException ex) {
            LOGGER.error("Getting elastic objects sets",ex);
        }
       return ret;
    }
        
    private Date getElasticEarliestDate() {
        Date date =null;
        JSONObject ret = new JSONObject();
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(QueryBuilders.existsQuery("culturaoaiid"));

        //Set paging parameters

        ssb.size(1);

        //Set sort parameters
        ssb.sort("indexcreated", SortOrder.DESC);

        //Add source builder to request
        sr.source(ssb);

        try {
            //Perform search
            SearchResponse resp = ELASTIC.search(sr);
            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", resp.getTook().toString());
                //Get hits
                SearchHits respHits = resp.getHits();
                SearchHit [] hits = respHits.getHits();

                if (hits.length > 0) {                    
                    //Get record
                    JSONObject o = new JSONObject(hits[0].getSourceAsString());
                    if(o.has("indexcreated")){
                        date = new Date(o.getLong("indexcreated"));
                    }
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Getting earlies object",ex);
        }
       return date;
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
