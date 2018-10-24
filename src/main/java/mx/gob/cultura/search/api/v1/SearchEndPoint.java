package mx.gob.cultura.search.api.v1;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import mx.gob.cultura.commons.Util;
import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;

/**
 * REST EndPoint to manage search requests.
 *
 * @author Hasdai Pacheco
 */
@Path("/search")
public class SearchEndPoint {

    private static final Logger LOG = Logger.getLogger(SearchEndPoint.class);
    private static RestHighLevelClient elastic = Util.ELASTICSEARCH.getElasticClient();
    private static String indexName = getIndexName();
    public static final String REPO_INDEX = "cultura";
    public static final String REPO_INDEX_TEST = "cultura_test";
    private static final LoadingCache<String, JSONObject> objectCache = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(100000L)
            .build(k -> getObjectById(k));

    /**
     * Processes search request by keyword or identifier.
     *
     * @param context {@link UriInfo} object with request context information.
     * @return Response object with search results
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response search(@Context UriInfo context) {
        MultivaluedMap<String, String> params = context.getQueryParameters();
        String id = params.getFirst("identifier");
        String q = params.getFirst("q");
        String from = params.getFirst("from");
        String size = params.getFirst("size");
        String sort = params.getFirst("sort");
        String attr = params.getFirst("attr");

        /* For filtering previous query by one or more properties  */
        String filter = params.getFirst("filter");

        JSONObject ret = null;
        if (null == id || id.isEmpty()) {
            if (null == q) {
                q = "*";
            }

            int f = -1;
            int s = 100;
            if (null != from && !from.isEmpty()) {
                f = Integer.parseInt(from);
            }

            if (null != size && !size.isEmpty()) {
                s = Integer.parseInt(size);
            }

            //Get sort parameters
            String[] sp = new String[1];
            if (null != sort && !sort.isEmpty()) {
                if (sort.contains(",")) {
                    sp = sort.split(",");
                } else {
                    sp[0] = sort;
                }
            }
            ret = searchByKeyword(q, f, s, sp, attr, filter);
        } else {
            ret = searchById(id);
        }

        if (null == ret) {
            return Response.status(Response.Status.NOT_FOUND).encoding("utf8").build();
        } else {
            return Response.ok(ret.toString()).encoding("utf8").build();
        }
    }

    /**
     * Updates object view count.
     *
     * @param oId Object ID
     */
    @Path("/hits/{objectId}")
    @POST
    public void addView(@PathParam("objectId") String oId) {
        JSONObject ret = new JSONObject();
        if (null != oId && !oId.isEmpty()) {
            UpdateRequest req = new UpdateRequest(indexName, "bic", oId);
            Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.resourcestats.views += 1", new HashMap<>());

            req.script(inline);

            try {
                UpdateResponse resp = elastic.update(req);
                if (resp.getResult() == DocWriteResponse.Result.UPDATED) {
                    ret.put("_id", resp.getId());
                }
            } catch (IOException ioex) {
                LOG.error(ioex);
            }
        }
    }

    /**
     * Gets {@link Histogram} aggregation with the given @aggName as a
     * {@link JSONObject}
     *
     * @param aggregations {@link Aggregations} object from
     * {@link SearchResponse}.
     * @param aggName Name of aggregation to get.
     * @return JSONObject with an array of aggregation names and counts.
     */
    private JSONObject getDateHistogramAggregation(Aggregations aggregations, String aggName) {
        JSONObject ret = new JSONObject();
        JSONArray aggs = new JSONArray();

        Histogram histogram = aggregations.get(aggName);
        if (!histogram.getBuckets().isEmpty()) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                if (bucket.getDocCount() > 0) {
                    JSONObject o = new JSONObject();
                    o.put("name", bucket.getKeyAsString());
                    o.put("count", bucket.getDocCount());
                    aggs.put(o);
                }
            }
        }
        ret.put(aggName, aggs);
        return ret;
    }

    /**
     * Gets {@link Histogram} aggregation with the given @aggName as a
     * {@link JSONObject}
     *
     * @param aggregations {@link Aggregations} object from
     * {@link SearchResponse}.
     * @param aggName Name of aggregation to get.
     * @return JSONObject with an array of aggregation names and counts.
     */
    private JSONObject getHistogramAggregation(Aggregations aggregations, String aggName) {
        JSONObject ret = new JSONObject();
        JSONArray aggs = new JSONArray();

        Histogram histogram = aggregations.get(aggName);
        if (!histogram.getBuckets().isEmpty()) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                if (bucket.getDocCount() > 0) {
                    JSONObject o = new JSONObject();
                    o.put("name", bucket.getKeyAsString());
                    o.put("count", bucket.getDocCount());
                    aggs.put(o);
                }
            }
        }
        ret.put(aggName, aggs);
        return ret;
    }

    /**
     * Gets {@link Terms} aggregation with the given @aggName as a
     * {@link JSONObject}
     *
     * @param aggregations {@link Aggregations} object from
     * {@link SearchResponse}.
     * @param aggName Name of aggregation to get.
     * @return JSONObject with an array of aggregation names and counts.
     */
    private JSONObject getTermAggregation(Aggregations aggregations, String aggName) {
        JSONObject ret = new JSONObject();
        JSONArray aggs = new JSONArray();
        Terms terms = aggregations.get(aggName);
        //System.out.println("Terms("+aggName+")"+terms.toString());
        if (!terms.getBuckets().isEmpty()) {
            for (Terms.Bucket bucket : terms.getBuckets()) {
                JSONObject o = new JSONObject();
                o.put("name", bucket.getKeyAsString());
                o.put("count", bucket.getDocCount());
                aggs.put(o);
            }
        }
        ret.put(aggName, aggs);
        return ret;
    }

    /**
     * Gets an object from cache using document identifier. If object is nt in
     * cache it is retrieved from ElasticSearch.
     *
     * @param id Identifier of document to retrieve.
     * @return JSONObject wrapping document information.
     */
    private JSONObject searchById(String id) {
        return objectCache.get(id);
    }

    /**
     * Gets an object from ElasticSearch using document identifier.
     *
     * @param id Identifier of document to retrieve from index.
     * @return JSONObject wrapping document information.
     */
    private static JSONObject getObjectById(String id) {
        JSONObject ret = null;
        GetRequest req = new GetRequest(indexName, "bic", id);

        try {
            GetResponse response = elastic.get(req);
            if (response.isExists()) {
                ret = new JSONObject(response.getSourceAsString());
                ret.put("_id", response.getId());
            }
        } catch (IOException ioex) {
            LOG.error(ioex);
        }

        return ret;
    }

    /**
     * Gets documents from ElasticSearch matching keyword search.
     *
     * @param q Query string
     * @param from Number of record to start from
     * @param size Number of records to retrieve
     * @param sortParams Array of sort parameters, sort will be processed in
     * array order.
     * @return JSONObject wrapping search results.
     */
    private JSONObject searchByKeyword(String q, int from, int size, String[] sortParams, String attr, String filter) {

        JSONObject ret = new JSONObject();
        String qreplaced = "";
        String tmpq = "";
        boolean isReplaced = false;
        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();

        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        BoolQueryBuilder boolshouldmain = QueryBuilders.boolQuery();
        BoolQueryBuilder boolfilters = QueryBuilders.boolQuery();
//        BoolQueryBuilder boolshould = QueryBuilders.boolQuery(); // Para que funcione como agregador
        BoolQueryBuilder boolmust = QueryBuilders.boolQuery();

        //Fields weight definition table
        Map<String, Float> fieldsweight = new HashMap();
        fieldsweight.put("recordtitle.value", 12f);
        fieldsweight.put("holder.raw", 12f);
        fieldsweight.put("holdernote", 12f);
        fieldsweight.put("creator.raw", 12f);
        fieldsweight.put("creator", 12f);
        fieldsweight.put("serie", 12f);
        fieldsweight.put("chapter", 12f);
        fieldsweight.put("episode", 12f);
        fieldsweight.put("reccollection", 12f);
        fieldsweight.put("collection", 12f);
        fieldsweight.put("resourcetype", 11f);
        fieldsweight.put("keywords", 10f);
        fieldsweight.put("datecreated.note", 9f);
        fieldsweight.put("timelinedate.textvalue", 9f);
        fieldsweight.put("digitalObject.mediatype.mime.raw", 9f);
        fieldsweight.put("rights.media.mime.keyword", 9f);
        fieldsweight.put("lugar", 8f);
        fieldsweight.put("description", 7f);

//        fieldsweight.put("catalog", 6f);
        fieldsweight.put("publisher.raw", 6f);
        fieldsweight.put("state", 6f);
        fieldsweight.put("lang", 6f);
        fieldsweight.put("gprlang", 6f);
        fieldsweight.put("oaiid", 6f);
        fieldsweight.put("dimension", 6f);
        fieldsweight.put("dimensiontype", 6f);
        fieldsweight.put("unidad", 6f);
        fieldsweight.put("unidadtype", 6f);
        fieldsweight.put("rights.rightstitle", 6f);
        //fieldsweight.put("rights.description", 6f);
//        fieldsweight.put("creatornote", 6f);
//        fieldsweight.put("creatorgroup", 6f);
//        fieldsweight.put("periodcreated.name", 6f);
//        fieldsweight.put("periodcreated.datestart.value", 6f);
//        fieldsweight.put("periodcreated.dateend.value", 6f);
        fieldsweight.put("credits", 6f);
        fieldsweight.put("availableformats", 6f);
        fieldsweight.put("documentalfund", 6f);
        fieldsweight.put("direction", 6f);
        fieldsweight.put("production", 6f);
        fieldsweight.put("music", 6f);
        fieldsweight.put("libretto", 6f);
        fieldsweight.put("musicdirection", 6f);
        fieldsweight.put("biccustodyentity", 6f);
        fieldsweight.put("director", 6f);
        fieldsweight.put("producer", 6f);
        fieldsweight.put("screenplay", 6f);
        fieldsweight.put("distribution", 6f);
        fieldsweight.put("editorial", 6f);
        fieldsweight.put("subtitle", 6f);
        fieldsweight.put("technique", 6f);
        fieldsweight.put("hiperonimo", 6f);
        fieldsweight.put("material", 6f);
        fieldsweight.put("discipline", 6f);
        fieldsweight.put("invited", 6f);
        fieldsweight.put("theme", 6f);
        fieldsweight.put("synopsis", 6f);

        fieldsweight.put("conservationstate", 6f);
        fieldsweight.put("characters", 6f);
        fieldsweight.put("observations", 6f);
        fieldsweight.put("labels", 6f);
        fieldsweight.put("period", 6f);
        fieldsweight.put("curaduria", 6f);
        fieldsweight.put("inscripcion", 6f);
        fieldsweight.put("cultura", 6f);
        fieldsweight.put("origin", 6f);
        fieldsweight.put("acervo", 6f);
        fieldsweight.put("techmaterial", 6f);
        fieldsweight.put("inscripcionobra", 6f);

        if (null != attr) {
            if (attr.equalsIgnoreCase("oaiid")) {
                ssb.query(QueryBuilders.termQuery(attr, q));
            } else {
//                if (q != null) {
//                    q = replaceSpecialChars(q).toUpperCase();
//                }
                ssb.query(QueryBuilders.matchQuery(attr, q));
            }
        } else if (null != filter) {

            if (q != null) {
                qreplaced = replaceSpecialChars(q, true).toUpperCase();
                q = replaceSpecialChars(q, false).toUpperCase();
                if (!qreplaced.equals(q)) {
                    isReplaced = true;
                }
            }
            HashMap<String, List<String>> hmfilters = new HashMap();
            List<String> listVals = null;
            String[] filters = null;
            if (filter.contains(";;")) {
                filters = new String[filter.split(";;").length];
                filters = filter.split(";;");
            } else {
                filters = new String[1];
                filters[0] = filter;
            }

            //se podrían hacer un bool query con and + otro con or
            boolshouldmain.should(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.AND).fields(fieldsweight)); //must equivale AND; should equivale OR
            if (isReplaced) {
                boolshouldmain.should(QueryBuilders.queryStringQuery(qreplaced).defaultOperator(Operator.AND).fields(fieldsweight));
            } //must equivale AND; should equivale OR
            if (null != q && q.trim().indexOf(" ") > 0) {
                String[] mstr = q.trim().split(" ");
                String[] mstrrep = qreplaced.trim().split(" ");
                for (int i = 0; i < mstr.length; i++) {
                    boolshouldmain.should(QueryBuilders.queryStringQuery(mstr[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                    if (isReplaced && !mstr[i].equals(mstrrep[i])) {
                        boolshouldmain.should(QueryBuilders.queryStringQuery(mstrrep[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                    }
                }
            }
            qb.must().add(boolshouldmain);
//            qb.must(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.OR).fields(fieldsweight)); //must equivale AND; should equivale OR
            String startDate = "";
            String endDate = "";
            for (String myfilter : filters) {
                String[] propVal = myfilter.split(":");
                if (propVal.length == 2) {
                    String key = propVal[0].toLowerCase();
                    if (hmfilters.get(key) == null) {
                        listVals = new ArrayList<String>();
                        listVals.add(propVal[1]);
                        hmfilters.put(key, listVals);
                    } else {
                        listVals = hmfilters.get(key);
                        listVals.add(propVal[1]);
                    }
                } else {
                    continue;
                }
            }

            if (!hmfilters.isEmpty()) {
                Iterator<String> it = hmfilters.keySet().iterator();
                while (it.hasNext()) {
                    BoolQueryBuilder boolshouldfilter = QueryBuilders.boolQuery(); // Para que funcione como filtro específico
                    String filterKey = it.next();

                    listVals = hmfilters.get(filterKey);
//                    System.out.println("Filter key:"+filterKey+" => "+listVals.toString()); // Para que funcione como filtro específico
                    switch (filterKey) {
                        case "holder":
                            filterKey = "holder.raw";
                            break;
                        case "resourcetype":
                            filterKey = "resourcetype.raw";
                            //filterKey = "resourcetype";
                            break;
                        case "datecreated":
//                            filterKey = "datecreated.value";
                            filterKey = "timelinedate.value";
                            break;
                        case "rights":
                            filterKey = "digitalObject.rights.rightstitle";
                            break;
                        case "mediatype":
                            filterKey = "digitalObject.mediatype.mime.raw";
                            break;
                        case "rightsmedia":
                            filterKey = "rights.media.mime.keyword";
                            break;
                        case "language":
                            filterKey = "lang";
                            break;
                        case "serie":
                            filterKey = "serie";
                            break;
                        case "collection":
                            filterKey = "reccollection";
                            break;
                        case "datestart":
                            startDate = listVals.get(0);
//                            if (!startDate.startsWith("-")&&startDate.indexOf("-") > 1) {
//                                String[] arrst = startDate.split("-");
//                                if (arrst.length == 3) {
//                                    for (String df : arrst) {
//                                        if(df.length()==4){
//                                            startDate = df;
//                                        } else if(df.length()==2){
//                                            startDate += "-"+df;
//                                        } else if(df.length()==1){
//                                            startDate += "-0"+df;
//                                        }
//                                    }
//                                    startDate += "T00:00:00.000Z";
//                                } 
//                            } else {
//                                startDate += "-01-01T00:00:00.000Z";
//                            }
                            continue;
                        //break;
                        case "dateend":
                            endDate = listVals.get(0);
//                            if (!endDate.startsWith("-")&&endDate.indexOf("-") > 1) {
//                                String[] arrst = endDate.split("-");
//                                if (arrst.length == 3) {
//                                    for (String df : arrst) {
//                                        if(df.length()==4){
//                                            endDate = df;
//                                        } else if(df.length()==2){
//                                            endDate += "-"+df;
//                                        } else if(df.length()==1){
//                                            endDate += "-0"+df;
//                                        }
//                                    }
//                                    endDate += "T23:59:59.999Z";
//                                } 
//                            } else {
//                                endDate += "-01-01T23:59:59.999Z";
//                            }
                            continue;
                    }
                    HashMap<String, HashSet<String>> added = new HashMap<>();
                    HashSet<String> typesAdded = new HashSet<>();
                    if (listVals.size() > 1 && !filterKey.equals("datestart") && !filterKey.equals("dateend")) {
//                        System.out.println("Filtro => " + filterKey);

                        for (String fval : listVals) {
//                            System.out.println("Valor original: " + fval);
                            fval = fval.toLowerCase();
                            if (filterKey.equals("resourcetype.raw")) {
                                if (!added.containsKey(filterKey)) {
                                    added.put(filterKey, new HashSet<String>());
                                }
                                typesAdded = added.get(filterKey);
                                if (!typesAdded.contains(fval)) {
                                    typesAdded.add(fval);
                                }
//                                System.out.println("fval1:" + fval);
                                boolshouldfilter.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));
                                fval = fval.substring(0, 1).toUpperCase() + fval.substring(1);
//                                System.out.println("fval2:" + fval);
                                boolshouldfilter.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));

                            } else {
                                boolshouldfilter.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));
//                            boolshould.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));  // Para que funcione como agregador

                            }
                        }
                    } else if (!filterKey.equals("datestart") && !filterKey.equals("dateend")) {
                        String fval = listVals.get(0).toLowerCase();
                        if (filterKey.equals("resourcetype.raw")) {
                            if (!added.containsKey(filterKey)) {
                                added.put(filterKey, new HashSet<String>());
                            }
                            typesAdded = added.get(filterKey);
                            if (!typesAdded.contains(fval)) {
                                typesAdded.add(fval);
                            }
//                            System.out.println("fval1:" + fval);
                            boolshouldfilter.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));
                            fval = fval.substring(0, 1).toUpperCase() + fval.substring(1);
//                            System.out.println("fval2:" + fval);
                            boolshouldfilter.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));

                        } else {
                            boolshouldfilter.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));
//                            boolshould.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));  // Para que funcione como agregador

                        }
                    }
                    boolfilters.must().add(boolshouldfilter);  // Para que funcione como filtro específico
                }
            }

            if (!startDate.equals("") && !endDate.equals("")) {
                boolfilters.must().add(QueryBuilders.rangeQuery("timelinedate.value").from(startDate).to(endDate));
            }
            //boolfilters.must().add(boolshould);  // Para que funcione como agregador
            qb.filter(boolfilters);
            ssb.query(qb);
        } else {
            if (q != null) {
                qreplaced = replaceSpecialChars(q, true).toUpperCase();
                q = replaceSpecialChars(q, false).toUpperCase();
                if (!qreplaced.equals(q)) {
                    isReplaced = true;
                }
            }
            //se podrían hacer un bool query con and + otro con or
//            boolshouldmain.should(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.AND).fields(fieldsweight)); //must equivale AND; should equivale OR
//            if (q.trim().indexOf(" ") > 0) {
//                String[] mstr = q.trim().split(" ");
//                for (String word : mstr) {
//                    boolshouldmain.should(QueryBuilders.queryStringQuery(word).defaultOperator(Operator.AND).fields(fieldsweight));
//                }
//            }

//se podrían hacer un bool query con and + otro con or
            boolshouldmain.should(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.AND).fields(fieldsweight)); //must equivale AND; should equivale OR
            if (isReplaced) {
                boolshouldmain.should(QueryBuilders.queryStringQuery(qreplaced).defaultOperator(Operator.AND).fields(fieldsweight));
            } //must equivale AND; should equivale OR
            if (null != q && q.trim().indexOf(" ") > 0) {
                String[] mstr = q.trim().split(" ");
                String[] mstrrep = qreplaced.trim().split(" ");
                for (int i = 0; i < mstr.length; i++) {
                    boolshouldmain.should(QueryBuilders.queryStringQuery(mstr[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                    if (isReplaced && !mstr[i].equals(mstrrep[i])) {
                        boolshouldmain.should(QueryBuilders.queryStringQuery(mstrrep[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                    }
                }
            }

            qb.must().add(boolshouldmain);
            ssb.query(qb);

            // Original Query with out filters, only with fields weight
            // ==========================================================================================================
//            ssb.query(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.OR).fields(fieldsweight));
            // Others test
            //ssb.query(QueryBuilders.simpleQueryStringQuery(q).defaultOperator(Operator.AND).fields(fieldsweight));
            //ssb.query(QueryBuilders.multiMatchQuery(q,"recordtitle.value").type("phrase").slop(1).operator(Operator.AND));
        }
        //Set paging parameters
        if (from > -1) {
            ssb.from(from);
        }
        if (size > 0) {
            ssb.size(size);
        }

        //Set sort parameters
        if (null != sortParams) {
            for (String param : sortParams) {
                if (null != param && !param.equals("nosort")) {
                    boolean desc = param.startsWith("-");
                    ssb.sort(param.replace("-", ""), desc ? SortOrder.DESC : SortOrder.ASC);
                }
            }
        } else {
            // Default sort first shows important =
            ssb.sort("important", SortOrder.DESC);
        }

        TermsAggregationBuilder holdersAgg = AggregationBuilders.terms("holders")
                .field("holder.raw").size(10000);// holder.raw
        TermsAggregationBuilder typesAgg = AggregationBuilders.terms("resourcetypes")
                .field("resourcetype.raw").size(10000);
        DateHistogramAggregationBuilder datesAgg = AggregationBuilders.dateHistogram("dates")
                .field("timelinedate.value").dateHistogramInterval(DateHistogramInterval.YEAR);
        TermsAggregationBuilder timelinedatesAgg = AggregationBuilders.terms("timelinedates")
                .field("timelinedate.value").size(10000);
        TermsAggregationBuilder rigthsAgg = AggregationBuilders.terms("rights")
                .field("digitalObject.rights.rightstitle").size(10000);
        TermsAggregationBuilder mediaAgg = AggregationBuilders.terms("mediastype")
                .field("digitalObject.mediatype.mime.raw").size(10000); //
        TermsAggregationBuilder rmediaAgg = AggregationBuilders.terms("rightsmedia")
                .field("rights.media.mime.keyword").size(10000); //
        TermsAggregationBuilder languagesAgg = AggregationBuilders.terms("languages")
                .field("lang").size(10000);

        ssb.aggregation(holdersAgg);
        ssb.aggregation(typesAgg);
        ssb.aggregation(datesAgg);
        ssb.aggregation(timelinedatesAgg);
        ssb.aggregation(rigthsAgg);
        ssb.aggregation(mediaAgg);
        ssb.aggregation(rmediaAgg);
        ssb.aggregation(languagesAgg);
//        System.out.println("\n\n\n===============================\n\n" + ssb.toString());
        //Add source builder to request
        sr.source(ssb);

        try {
            //Perform search
            SearchResponse resp = elastic.search(sr);

            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", resp.getTook().toString());

                //Get hits
                SearchHits respHits = resp.getHits();
                SearchHit[] hits = respHits.getHits();

                ret.put("total", respHits.getTotalHits());

                if (hits.length > 0) {
                    //Get records
                    JSONArray recs = new JSONArray();
                    for (SearchHit hit : hits) {
                        JSONObject o = new JSONObject(hit.getSourceAsString());
                        o.put("_id", hit.getId());
                        recs.put(o);
                    }
                    ret.put("records", recs);

                    //Get aggregations
                    Aggregations aggs = resp.getAggregations();
                    if (null != aggs && !aggs.asList().isEmpty()) {
                        JSONArray aggsArray = new JSONArray();
                        JSONObject agg = getTermAggregation(aggs, "holders");

                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "resourcetypes");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getDateHistogramAggregation(aggs, "dates");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "timelinedates");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "rights");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "mediastype");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "rightsmedia");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "languages");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        if (aggsArray.length() > 0) {
                            ret.put("aggs", aggsArray);
                        }
                    }
                }
            }
        } catch (IOException ioex) {
            LOG.error(ioex);
        }

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

    public static String replaceSpecialChars(String q, boolean letters) {
        StringBuilder ret = new StringBuilder();
        String aux = q;

        //aux = aux.toLowerCase();
        if (letters) {
            aux = aux.replace('Á', 'A');
            aux = aux.replace('Ä', 'A');
            aux = aux.replace('Å', 'A');
            aux = aux.replace('Â', 'A');
            aux = aux.replace('À', 'A');
            aux = aux.replace('Ã', 'A');

            aux = aux.replace('É', 'E');
            aux = aux.replace('Ê', 'E');
            aux = aux.replace('È', 'E');
            aux = aux.replace('Ë', 'E');

            aux = aux.replace('Í', 'I');
            aux = aux.replace('Î', 'I');
            aux = aux.replace('Ï', 'I');
            aux = aux.replace('Ì', 'I');

            aux = aux.replace('Ó', 'O');
            aux = aux.replace('Ö', 'O');
            aux = aux.replace('Ô', 'O');
            aux = aux.replace('Ò', 'O');
            aux = aux.replace('Õ', 'O');

            aux = aux.replace('Ú', 'U');
            aux = aux.replace('Ü', 'U');
            aux = aux.replace('Û', 'U');
            aux = aux.replace('Ù', 'U');

            aux = aux.replace('Ñ', 'N');

            aux = aux.replace('Ç', 'C');
            aux = aux.replace('Ý', 'Y');

            aux = aux.replace('á', 'a');
            aux = aux.replace('à', 'a');
            aux = aux.replace('ã', 'a');
            aux = aux.replace('â', 'a');
            aux = aux.replace('ä', 'a');
            aux = aux.replace('å', 'a');

            aux = aux.replace('é', 'e');
            aux = aux.replace('è', 'e');
            aux = aux.replace('ê', 'e');
            aux = aux.replace('ë', 'e');

            aux = aux.replace('í', 'i');
            aux = aux.replace('ì', 'i');
            aux = aux.replace('î', 'i');
            aux = aux.replace('ï', 'i');

            aux = aux.replace('ó', 'o');
            aux = aux.replace('ò', 'o');
            aux = aux.replace('ô', 'o');
            aux = aux.replace('ö', 'o');

            aux = aux.replace('ú', 'u');
            aux = aux.replace('ù', 'u');
            aux = aux.replace('ü', 'u');
            aux = aux.replace('û', 'u');

            aux = aux.replace('ñ', 'n');

            aux = aux.replace('ç', 'c');
            aux = aux.replace('ÿ', 'y');
            aux = aux.replace('ý', 'y');
        }

        if (null != aux && !aux.equals("*")) {
            int l = aux.length();
            for (int x = 0; x < l; x++) {
                char ch = aux.charAt(x);
                if ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z')
                        || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '-' || ch == '.' || ch == ' ') {
                    ret.append(ch);
                }
            }
            aux = ret.toString();
        }
        return aux;
    }
}
