package mx.gob.cultura.search.api.v1;

import mx.gob.cultura.commons.Util;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
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
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import java.io.IOException;

/**
 * REST EndPoint to manage search requests.
 * @author Hasdai Pacheco
 */
@Path("/search")
public class SearchEndPoint {
    private RestHighLevelClient elastic;

    /**
     * Constructor. Creates a new instance of {@link SearchEndPoint}.
     */
    public SearchEndPoint() {
        elastic = Util.DB.getElasticClient();
    }

    /**
     * Processes search request by keyword or identifier.
     * @param context {@link UriInfo} object with request context information.
     * @return
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response search(@Context UriInfo context) {
        MultivaluedMap<String, String> params = context.getQueryParameters();
        String id = params.getFirst("identifier");
        String q = params.getFirst("q");

        JSONObject ret;
        if (null == id || id.isEmpty()) {
            if (null == q) {
                q = "*";
            }
            ret = searchByKeyword(q);
        } else {
            ret = getObjectById(id);
        }

        return Response.ok(ret.toString()).build();
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
        JSONObject ret = new JSONObject();
        GetRequest req = new GetRequest("cultura", "bic", id);

        try {
            GetResponse response = elastic.get(req);
            if (response.isExists()) {
                ret = new JSONObject(response.getSourceAsString());
                ret.put("_id", response.getId());
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }

        return ret;
    }

    /**
     * Gets documents from ElasticSearch matching keyword search.
     * @param q Query string
     * @return JSONObject wrapping search results.
     */
    private JSONObject searchByKeyword(String q) {
        JSONObject ret = new JSONObject();

        //Create search request
        SearchRequest sr = new SearchRequest("cultura");

        //Create queryString query
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(QueryBuilders.queryStringQuery(q));

        //Build aggregations
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
}
