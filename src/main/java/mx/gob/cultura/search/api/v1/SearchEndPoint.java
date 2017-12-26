package mx.gob.cultura.search.api.v1;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import mx.gob.cultura.commons.Util;
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
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * REST EndPoint to manage search requests.
 * @author Hasdai Pacheco
 */
@Path("/search")
public class SearchEndPoint {
    private final RestHighLevelClient elastic;
    private static String indexName;
    public static final String REPO_INDEX = "cultura";
    public static final String REPO_INDEX_TEST = "cultura_test";
    private Cache<String, JSONObject> objectCache;

    /**
     * Constructor. Creates a new instance of {@link SearchEndPoint}.
     */
    public SearchEndPoint() {
        elastic = Util.ELASTICSEARCH.getElasticClient();
        indexName = getIndexName();
        objectCache = Caffeine.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .maximumSize(100000)
                .build();
    }

    /**
     * Processes search request by keyword or identifier.
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

        JSONObject ret;
        if (null == id || id.isEmpty()) {
            if (null == q) q = "*";

            int f = -1, s = 100;
            if (null != from && !from.isEmpty()) {
                f = Integer.parseInt(from);
            }

            if (null != size && !size.isEmpty()) {
                s = Integer.parseInt(size);
            }

            //Get sort parameters
            String[] sp = new String[1];
            if (null != sort && ! sort.isEmpty()) {
                if (sort.contains(",")) {
                    sp = sort.split(",");
                } else {
                    sp[0] = sort;
                }
            }

            ret = searchByKeyword(q, f, s, sp);
        } else {
            ret = getObjectById(id);
        }

        return Response.ok(ret.toString()).build();
    }

    /**
     * Updates object view count.
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
                ioex.printStackTrace();
            }
        }
    }

    /**
     * Gets {@link Histogram} aggregation with the given @aggName as a {@link JSONObject}
     * @param aggregations {@link Aggregations} object from {@link SearchResponse}.
     * @param aggName Name of aggregation to get.
     * @return JSONObject with an array of aggregation names and counts.
     */
    private JSONObject getDateHistogramAggregation(Aggregations aggregations, String aggName) {
        JSONObject ret = new JSONObject();
        JSONArray aggs = new JSONArray();

        Histogram histogram = aggregations.get(aggName);
        if (histogram.getBuckets().size() > 0) {
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
     * Gets {@link Terms} aggregation with the given @aggName as a {@link JSONObject}
     * @param aggregations {@link Aggregations} object from {@link SearchResponse}.
     * @param aggName Name of aggregation to get.
     * @return JSONObject with an array of aggregation names and counts.
     */
    private JSONObject getTermAggregation(Aggregations aggregations, String aggName) {
        JSONObject ret = new JSONObject();
        JSONArray aggs = new JSONArray();

        Terms terms = aggregations.get(aggName);
        if (terms.getBuckets().size() > 0) {
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
     * Gets an object from ElasticSearch using document identifier.
     * @param id Identifier of document to retrieve from index.
     * @return JSONObject wrapping document information.
     */
    private JSONObject getObjectById(String id) {
        JSONObject ret = objectCache.getIfPresent(id);

        if (null == ret) {
            ret = new JSONObject();
            GetRequest req = new GetRequest(indexName, "bic", id);

            try {
                GetResponse response = elastic.get(req);
                if (response.isExists()) {
                    ret = new JSONObject(response.getSourceAsString());
                    ret.put("_id", response.getId());
                    objectCache.put(response.getId(), ret);
                }
            } catch (IOException ioex) {
                ioex.printStackTrace();
            }
        }

        return ret;
    }

    /**
     * Gets documents from ElasticSearch matching keyword search.
     * @param q Query string
     * @param from Number of record to start from
     * @param size Number of records to retrieve
     * @param sortParams Array of sort parameters, sort will be processed in array order.
     * @return JSONObject wrapping search results.
     */
    private JSONObject searchByKeyword(String q, int from, int size, String[] sortParams) {
        JSONObject ret = new JSONObject();

        //Create search request
        SearchRequest sr = new SearchRequest(indexName);

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(QueryBuilders.queryStringQuery(q));

        //Set paging parameters
        if (from > -1) ssb.from(from);
        if (size > 0) ssb.size(size);

        //Set sort parameters
        if (null != sortParams) {
            for (String param : sortParams) {
                boolean desc = param.startsWith("-");
                ssb.sort(param.replace("-", ""), desc?SortOrder.DESC:SortOrder.ASC);
            }
        }

        /*BoolQueryBuilder filters = QueryBuilders.boolQuery();
        filters.must().add(QueryBuilders.matchQuery("field", "name"));
        filters.must().add(QueryBuilders.matchQuery("field", "name"));
        ssb.postFilter(filters);*/

        //Build aggregations for faceted search
        TermsAggregationBuilder holdersAgg = AggregationBuilders.terms("holders")
                .field("holder.raw");
        TermsAggregationBuilder typesAgg = AggregationBuilders.terms("resourcetypes")
                .field("resourcetype.raw");
        DateHistogramAggregationBuilder datesAgg = AggregationBuilders.dateHistogram("dates")
                .field("datecreated.value").dateHistogramInterval(DateHistogramInterval.YEAR);

        ssb.aggregation(holdersAgg);
        ssb.aggregation(typesAgg);
        ssb.aggregation(datesAgg);

        //Add source builder to request
        sr.source(ssb);

        try {
            //Perform search
            SearchResponse resp = elastic.search(sr);
            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", resp.getTook().toString());

                //Get hits
                SearchHits respHits = resp.getHits();
                SearchHit [] hits = respHits.getHits();

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
                    if (null != aggs && aggs.asList().size() > 0) {
                        JSONArray aggsArray = new JSONArray();
                        JSONObject agg = getTermAggregation(aggs,"holders");

                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getTermAggregation(aggs,"resourcetypes");
                        if (agg.length() > 0) {
                            aggsArray.put(agg);
                        }

                        agg = getDateHistogramAggregation(aggs,"dates");
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
            ioex.printStackTrace();
        }

        return ret;
    }

    /**
     * Gets index name to work with according to environment configuration.
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
