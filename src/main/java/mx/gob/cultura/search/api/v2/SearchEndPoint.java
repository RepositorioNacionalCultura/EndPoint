package mx.gob.cultura.search.api.v2;


import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import mx.gob.cultura.commons.Util;
import org.apache.http.Header;
import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
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

/**
 * REST EndPoint to manage search requests.
 *
 * @author Hasdai Pacheco
 */
@Path(value="/search")
public class SearchEndPoint {
    private static final Logger LOG = Logger.getLogger(SearchEndPoint.class);
    private static RestHighLevelClient elastic = Util.ELASTICSEARCH.getElasticClient();
    private static String indexName = SearchEndPoint.getIndexName();
    public static final String REPO_INDEX = "record";
    public static final String REPO_INDEX_TEST = "cultura_test";
    private static final LoadingCache<String, JSONObject> objectCache = Caffeine.newBuilder().expireAfterWrite(10L, TimeUnit.MINUTES).refreshAfterWrite(5L, TimeUnit.MINUTES).maximumSize(100000L).build(k -> SearchEndPoint.getObjectById(k));

        /**
     * Processes search request by keyword or identifier.
     *
     * @param context {@link UriInfo} object with request context information.
     * @return Response object with search results
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response search(@Context UriInfo context) {
        MultivaluedMap params = context.getQueryParameters();
        String id = (String)params.getFirst((Object)"identifier");
        String q = (String)params.getFirst((Object)"q");
        String from = (String)params.getFirst((Object)"from");
        String size = (String)params.getFirst((Object)"size");
        String sort = (String)params.getFirst((Object)"sort");
        String attr = (String)params.getFirst((Object)"attr");
        String filter = (String)params.getFirst((Object)"filter");
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
            String[] sp = new String[1];
            if (null != sort && !sort.isEmpty()) {
                if (sort.contains(",")) {
                    sp = sort.split(",");
                } else {
                    sp[0] = sort;
                }
            }
            ret = this.searchByKeyword(q, f, s, sp, attr, filter);
        } else {
            ret = this.searchById(id);
        }
        if (null == ret) {
            return Response.status((Response.Status)Response.Status.NOT_FOUND).encoding("utf8").build();
        }
        return Response.ok((Object)ret.toString()).encoding("utf8").build();
    }

       /**
     * Updates object view count.
     *
     * @param oId Object ID
     */
    @Path("/hits/{objectId}")
    @POST
    public void addView(@PathParam(value="objectId") String oId) {
        JSONObject ret = new JSONObject();
        if (null != oId && !oId.isEmpty()) {
            UpdateRequest req = new UpdateRequest(indexName, "bic", oId);
            Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.resourcestats.views += 1", new HashMap());
            req.script(inline);
            try {
                UpdateResponse resp = elastic.update(req, new Header[0]);
                if (resp.getResult() == DocWriteResponse.Result.UPDATED) {
                    ret.put("_id", (Object)resp.getId());
                }
            }
            catch (IOException ioex) {
                LOG.error((Object)ioex);
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
        Histogram histogram = (Histogram)aggregations.get(aggName);
        if (!histogram.getBuckets().isEmpty()) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                if (bucket.getDocCount() <= 0L) continue;
                JSONObject o = new JSONObject();
                o.put("name", (Object)bucket.getKeyAsString());
                o.put("count", bucket.getDocCount());
                aggs.put((Object)o);
            }
        }
        ret.put(aggName, (Object)aggs);
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
        Histogram histogram = (Histogram)aggregations.get(aggName);
        if (!histogram.getBuckets().isEmpty()) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                if (bucket.getDocCount() <= 0L) continue;
                JSONObject o = new JSONObject();
                o.put("name", (Object)bucket.getKeyAsString());
                o.put("count", bucket.getDocCount());
                aggs.put((Object)o);
            }
        }
        ret.put(aggName, (Object)aggs);
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
        Terms terms = (Terms)aggregations.get(aggName);
        if (!terms.getBuckets().isEmpty()) {
            for (Terms.Bucket bucket : terms.getBuckets()) {
                JSONObject o = new JSONObject();
                o.put("name", (Object)bucket.getKeyAsString());
                o.put("count", bucket.getDocCount());
                aggs.put((Object)o);
            }
        }
        ret.put(aggName, (Object)aggs);
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
        SearchRequest sr = new SearchRequest(new String[]{indexName});
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        BoolQueryBuilder boolshouldmain = QueryBuilders.boolQuery();
        BoolQueryBuilder boolfilters = QueryBuilders.boolQuery();
        BoolQueryBuilder boolmust = QueryBuilders.boolQuery();
        Map<String, Float> fieldsweight = new HashMap();
        fieldsweight.put("recordtitle", 12f);
        fieldsweight.put("holder.raw", 12f);
        fieldsweight.put("holdernote", 12f);
        fieldsweight.put("author.raw", 12f);
        fieldsweight.put("author", 12f);
        fieldsweight.put("serie", 12f);
        fieldsweight.put("chapter", 12f);
        fieldsweight.put("episode", 12f);
        fieldsweight.put("reccollection", 12f);
        fieldsweight.put("collection", 12f);
        fieldsweight.put("resourcetype", 11f);
        fieldsweight.put("resourcetype.raw", 11f);
        fieldsweight.put("keywords", 10f);
        fieldsweight.put("datecreatednote", 9f);
        fieldsweight.put("timelinedate.textvalue", 9f);
        fieldsweight.put("formato", 9f);
        fieldsweight.put("media", 9f);
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
        fieldsweight.put("rightstitle", 6f);
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
                ssb.query((QueryBuilder)QueryBuilders.termQuery((String)attr, (String)q));
            } else {
                ssb.query((QueryBuilder)QueryBuilders.matchQuery((String)attr, (Object)q));
            }
        } else if (null != filter) {
            if (q != null) {
                qreplaced = SearchEndPoint.replaceSpecialChars(q, true).toUpperCase();
                if (!qreplaced.equals(q = q.toUpperCase())) {
                    isReplaced = true;
                }
            }
            HashMap hmfilters = new HashMap();
            List<String> listVals = null;
            String[] filters = null;
            if (filter.contains(";;")) {
                filters = new String[filter.split(";;").length];
                filters = filter.split(";;");
            } else {
                filters = new String[]{filter};
            }
            boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)q).defaultOperator(Operator.AND).fields(fieldsweight));
            if (isReplaced) {
                boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)qreplaced).defaultOperator(Operator.AND).fields(fieldsweight));
            }
            if (null != q && q.trim().indexOf(" ") > 0) {
                String[] mstr = q.trim().split(" ");
                String[] mstrrep = qreplaced.trim().split(" ");
                for (int i = 0; i < mstr.length; ++i) {
                    boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)mstr[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                    if (!isReplaced || mstr[i].equals(mstrrep[i])) continue;
                    boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)mstrrep[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                }
            }
            qb.must().add(boolshouldmain);
            String startDate = "";
            String endDate = "";
            for (String myfilter : filters) {
                String[] propVal = myfilter.split(":");
                if (propVal.length != 2) continue;
                String key = propVal[0].toLowerCase();
                if (hmfilters.get(key) == null) {
                    listVals = new ArrayList<String>();
                    listVals.add(propVal[1]);
                    hmfilters.put(key, listVals);
                    continue;
                }
                listVals = (List)hmfilters.get(key);
                listVals.add(propVal[1]);
            }
            if (!hmfilters.isEmpty()) {
                Iterator it = hmfilters.keySet().iterator();
                block30 : while (it.hasNext()) {
                    BoolQueryBuilder boolshouldfilter = QueryBuilders.boolQuery();
                    String filterKey = (String)it.next();
                    listVals = (ArrayList<String>)hmfilters.get(filterKey);
                    switch (filterKey) {
                        case "holder": {
                            filterKey = "holder.raw";
                            break;
                        }
                        case "resourcetype": {
                            filterKey = "resourcetype.raw";
                            break;
                        }
                        case "datecreated": {
                            filterKey = "timelinedate.value";
                            break;
                        }
                        case "rights": {
                            filterKey = "rightstitle";
                            break;
                        }
                        case "mediatype": {
                            filterKey = "formato";
                            break;
                        }
                        case "rightsmedia": {
                            filterKey = "media";
                            break;
                        }
                        case "language": {
                            filterKey = "lang";
                            break;
                        }
                        case "serie": {
                            filterKey = "serie";
                            break;
                        }
                        case "collection": {
                            filterKey = "reccollection";
                            break;
                        }
                        case "datestart": {
                            startDate = (String)listVals.get(0);
                            continue block30;
                        }
                        case "dateend": {
                            endDate = (String)listVals.get(0);
                            continue block30;
                        }
                    }
                    if (listVals.size() > 1 && !filterKey.equals("datestart") && !filterKey.equals("dateend")) {
                        for (String fval : listVals) {
                            boolshouldfilter.should().add(QueryBuilders.matchQuery((String)filterKey, (Object)fval).operator(Operator.AND));
                        }
                    } else if (!filterKey.equals("datestart") && !filterKey.equals("dateend")) {
                        boolshouldfilter.should().add(QueryBuilders.matchQuery((String)filterKey, listVals.get(0)).operator(Operator.AND));
                    }
                    boolfilters.must().add(boolshouldfilter);
                }
            }
            if (!startDate.equals("") && !endDate.equals("")) {
                boolfilters.must().add(QueryBuilders.rangeQuery((String)"timelinedate.value").from((Object)startDate).to((Object)endDate));
            }
            qb.filter((QueryBuilder)boolfilters);
            ssb.query((QueryBuilder)qb);
        } else {
            if (q != null) {
                qreplaced = SearchEndPoint.replaceSpecialChars(q, true).toUpperCase();
                if (!qreplaced.equals(q = q.toUpperCase())) {
                    isReplaced = true;
                }
            }
            boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)q).defaultOperator(Operator.AND).fields(fieldsweight));
            if (isReplaced) {
                boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)qreplaced).defaultOperator(Operator.AND).fields(fieldsweight));
            }
            if (null != q && q.trim().indexOf(" ") > 0) {
                String[] mstr = q.trim().split(" ");
                String[] mstrrep = qreplaced.trim().split(" ");
                for (int i = 0; i < mstr.length; ++i) {
                    boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)mstr[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                    if (!isReplaced || mstr[i].equals(mstrrep[i])) continue;
                    boolshouldmain.should((QueryBuilder)QueryBuilders.queryStringQuery((String)mstrrep[i]).defaultOperator(Operator.AND).fields(fieldsweight));
                }
            }
            qb.must().add(boolshouldmain);
            ssb.query((QueryBuilder)qb);
        }
        if (from > -1) {
            ssb.from(from);
        }
        if (size > 0) {
            ssb.size(size);
        }
        if (null != sortParams) {
            for (String param : sortParams) {
                boolean desc;
                if (null == param || param.equals("nosort")) continue;
                ssb.sort(param.replace("-", ""), (desc = param.startsWith("-")) ? SortOrder.DESC : SortOrder.ASC);
            }
        } else {
            ssb.sort("important", SortOrder.DESC);
        }
        
        // Sacar las propiedades del datasource Record, revisar el tipo de datos que sean iscatalog == true
        // con el tipo de dato que tiene la propiedad para agregarla en la respuesta de la búsqueda
        
        HashMap<String,String> hmAggs = new HashMap();
        String strTempKey = "holder";
        String strTempValue = "holder.raw";
        
        
        
        
        
        
        TermsAggregationBuilder holdersAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"holders").field("holder.raw")).size(10000);
        TermsAggregationBuilder typesAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"resourcetypes").field("resourcetype.raw")).size(10000);
        DateHistogramAggregationBuilder datesAgg = ((DateHistogramAggregationBuilder)AggregationBuilders.dateHistogram((String)"dates").field("timelinedate.value")).dateHistogramInterval(DateHistogramInterval.YEAR);
        TermsAggregationBuilder timelinedatesAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"timelinedates").field("timelinedate.value")).size(10000);
        TermsAggregationBuilder rigthsAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"rights").field("rightstitle")).size(10000);
        TermsAggregationBuilder mediaAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"mediastype").field("formato")).size(10000);
        TermsAggregationBuilder rmediaAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"rightsmedia").field("media")).size(10000);
        TermsAggregationBuilder languagesAgg = ((TermsAggregationBuilder)AggregationBuilders.terms((String)"languages").field("lang")).size(10000);
        ssb.aggregation((AggregationBuilder)holdersAgg);
        ssb.aggregation((AggregationBuilder)typesAgg);
        ssb.aggregation((AggregationBuilder)datesAgg);
        ssb.aggregation((AggregationBuilder)timelinedatesAgg);
        ssb.aggregation((AggregationBuilder)rigthsAgg);
        ssb.aggregation((AggregationBuilder)mediaAgg);
        ssb.aggregation((AggregationBuilder)rmediaAgg);
        ssb.aggregation((AggregationBuilder)languagesAgg);
        sr.source(ssb);
        try {
            SearchResponse resp = elastic.search(sr, new Header[0]);
            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", (Object)resp.getTook().toString());
                SearchHits respHits = resp.getHits();
                SearchHit[] hits = respHits.getHits();
                ret.put("total", respHits.getTotalHits());
                if (hits.length > 0) {
                    JSONArray recs = new JSONArray();
                    for (SearchHit hit : hits) {
                        JSONObject o = new JSONObject(hit.getSourceAsString());
                        o.put("_id", (Object)hit.getId());
                        recs.put((Object)o);
                    }
                    ret.put("records", (Object)recs);
                    Aggregations aggs = resp.getAggregations();
                    if (null != aggs && !aggs.asList().isEmpty()) {
                        JSONArray aggsArray = new JSONArray();
                        JSONObject agg = this.getTermAggregation(aggs, "holders");
                        if (agg.length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getTermAggregation(aggs, "resourcetypes")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getDateHistogramAggregation(aggs, "dates")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getTermAggregation(aggs, "timelinedates")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getTermAggregation(aggs, "rights")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getTermAggregation(aggs, "mediastype")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getTermAggregation(aggs, "rightsmedia")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if ((agg = this.getTermAggregation(aggs, "languages")).length() > 0) {
                            aggsArray.put((Object)agg);
                        }
                        if (aggsArray.length() > 0) {
                            ret.put("aggs", (Object)aggsArray);
                        }
                    }
                }
            }
        }
        catch (IOException ioex) {
            LOG.error((Object)ioex);
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

    /**
     * 
     * @param q text to be replaced special chars
     * @param letters indictes if the letters have to be replaced or not
     * @return replaced text
     */
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
