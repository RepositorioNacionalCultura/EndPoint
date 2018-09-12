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

        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();

        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        BoolQueryBuilder boolfilters = QueryBuilders.boolQuery();
        BoolQueryBuilder boolshould = QueryBuilders.boolQuery();
        

        Map<String, Float> fieldsweight = new HashMap();
        fieldsweight.put("recordtitle.value", 12f);
        fieldsweight.put("creator.raw", 10f);
        fieldsweight.put("creator", 10f);
        fieldsweight.put("keywords", 8f);
        fieldsweight.put("description", 6f);
        fieldsweight.put("collection", 4f);
        fieldsweight.put("holder.raw", 4f);
        fieldsweight.put("generator.keyword", 4f);
        fieldsweight.put("lang", 4f);
        fieldsweight.put("gprlang", 4f);
        fieldsweight.put("lugar", 4f);
        fieldsweight.put("oaiid", 4f);
        fieldsweight.put("publisher", 4f);
        fieldsweight.put("serie", 4f);
        fieldsweight.put("collection", 4f);
        fieldsweight.put("reccollection", 4f);
        //fieldsweight.put("resourcetype.raw", 4f);
        fieldsweight.put("resourcetype", 4f);
        fieldsweight.put("rights.media.mime.keyword", 4f);
        fieldsweight.put("digitalObject.mediatype.mime.raw", 4f);
//        fieldsweight.put("timelinedate.value", 4f);
        fieldsweight.put("state", 4f);
        

        if (null != attr) {
            if (attr.equalsIgnoreCase("oaiid")) {
                ssb.query(QueryBuilders.termQuery(attr, q));
            } else {
                ssb.query(QueryBuilders.matchQuery(attr, q));
            }
        } else if (null != filter) {
            HashMap<String, List<String>> hmfilters = new HashMap();
            List<String> listVals = null;
            //System.out.println("Filtros....");
            String[] filters = null;
            if (null != filter && filter.contains(";;")) {
                filters = new String[filter.split(";;").length];
                filters = filter.split(";;");
//                for (String myf : filters) {
//                    System.out.println("==>" + myf);
//                }
            } else {
                filters = new String[1];
                filters[0] = filter;
            }

            qb.must(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.OR).fields(fieldsweight)); //must equivale AND; should equivale OR
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
                    String filterKey = it.next();
                    listVals = hmfilters.get(filterKey);
                    switch (filterKey) {
                        case "holder":
                            filterKey = "holder.raw";
                            break;
                        case "resourcetype":
                            //filterKey = "resourcetype.raw";
                            filterKey = "resourcetype";
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

                            startDate = listVals.get(0) + "-01-01T00:00:00.000Z";
                            //hmfilters.remove("datestart");
                            continue;
                        //break;
                        case "dateend":

                            endDate = listVals.get(0) + "-12-31T23:59:59.999Z";
                            //hmfilters.remove("dateend");
                            continue;
                        
                        //break;

                    }
                    if (listVals.size() > 1 && !filterKey.equals("datestart") && !filterKey.equals("dateend")) {
                        //BoolQueryBuilder boolshould = QueryBuilders.boolQuery();
                        for (String fval : listVals) {
                            boolshould.should().add(QueryBuilders.matchQuery(filterKey, fval).operator(Operator.AND));
                            //boolfilters.should().add(QueryBuilders.termQuery(filterKey, fval));
                        }
                        //boolfilters.must().add(boolshould);
                    } else if (!filterKey.equals("datestart") && !filterKey.equals("dateend")) {
                        //boolfilters.must().add(QueryBuilders.matchQuery(filterKey, listVals.get(0)).operator(Operator.AND));
                        //boolfilters.should().add(QueryBuilders.matchQuery(filterKey, listVals.get(0)));
                        //boolfilters.must().add(QueryBuilders.termQuery(filterKey, listVals.get(0)));
                        boolshould.should().add(QueryBuilders.matchQuery(filterKey, listVals.get(0)).operator(Operator.AND));
                    }

                }
            }
            

            if (!startDate.equals("") && !endDate.equals("")) {
                boolfilters.must().add(QueryBuilders.rangeQuery("timelinedate.value").from(startDate).to(endDate));
                //boolshould.must().add(QueryBuilders.rangeQuery("datecreated.value").from(startDate).to(endDate));
                //qb.filter(QueryBuilders.rangeQuery("datecreated.value").from(startDate).to(endDate));
            }
            boolfilters.must().add(boolshould);
//            System.out.println("\n\nQUERY:\n\n" + qb.toString() + "\n\n==============================================\n\n");
            ssb.query(qb);
        } else {
            ssb.query(QueryBuilders.queryStringQuery(q).defaultOperator(Operator.OR).fields(fieldsweight));

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
                if (null != param) {
                    boolean desc = param.startsWith("-");
                    ssb.sort(param.replace("-", ""), desc ? SortOrder.DESC : SortOrder.ASC);
                }
            }
        }
        //System.out.println("\n\n\n===============================\n\n"+ssb.toString());
        /*BoolQueryBuilder filters = QueryBuilders.boolQuery();
        filters.must().add(QueryBuilders.matchQuery("field", "name"));
        filters.must().add(QueryBuilders.matchQuery("field", "name"));*/
//        System.out.println("\n\nFILTERS:\n\n" + boolfilters.toString() + "\n\n==============================================\n\n");
        ssb.postFilter(boolfilters);
        //Build aggregations for faceted search
        TermsAggregationBuilder holdersAgg = AggregationBuilders.terms("holders")
                .field("holder.raw"); // holder.raw
        TermsAggregationBuilder typesAgg = AggregationBuilders.terms("resourcetypes")
                .field("resourcetype.raw");
        DateHistogramAggregationBuilder datesAgg = AggregationBuilders.dateHistogram("dates")
                .field("timelinedate.value").dateHistogramInterval(DateHistogramInterval.YEAR);
        TermsAggregationBuilder rigthsAgg = AggregationBuilders.terms("rights")
                .field("digitalObject.rights.rightstitle");
        TermsAggregationBuilder mediaAgg = AggregationBuilders.terms("mediastype")
                .field("digitalObject.mediatype.mime.raw");
        TermsAggregationBuilder languagesAgg = AggregationBuilders.terms("languages")
                .field("lang");

        ssb.aggregation(holdersAgg);
        ssb.aggregation(typesAgg);
        ssb.aggregation(datesAgg);
        ssb.aggregation(rigthsAgg);
        ssb.aggregation(mediaAgg);
        ssb.aggregation(languagesAgg);

//        System.out.println("\n\n\n===============================\n\n"+ssb.toString());
        
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

                        agg = getTermAggregation(aggs, "rights");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs, "mediastype");
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
}
