package mx.gob.cultura.search.api.v1;

import mx.gob.cultura.commons.Util;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/search")
public class SearchEndPoint {
    RestHighLevelClient elastic;

    public SearchEndPoint() {
        elastic = Util.DB.getElasticClient();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response keywordSearch(@Context UriInfo context) {
        MultivaluedMap<String, String> params = context.getQueryParameters();
        String q = params.getFirst("q");
        if (null == q) {
            q = "";
        }
        String id = params.getFirst("identifier");

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
            SearchResponse resp = elastic.search(sr);
            if (resp.status().getStatus() == RestStatus.OK.getStatus()) {
                ret.put("took", resp.getTook().toString());

                //Get hits
                SearchHits respHits = resp.getHits();
                ret.put("count", respHits.getTotalHits());

                SearchHit [] hits = respHits.getHits();
                if (hits.length > 0) {
                    JSONArray recs = new JSONArray();
                    for (SearchHit hit : hits) {
                        recs.put(new JSONObject(hit.getSourceAsString()));
                    }
                    ret.put("records", recs);


                    /*Aggregations aggs = resp.getAggregations();
                    if (aggs.asList().size() > 0) {
                        Terms holders = aggs.get("holders");
                        if (holders.getBuckets().size() > 0) {
                            for (Terms.Bucket bucket : holders.getBuckets()) {
                                System.out.println(bucket.getKeyAsString() + ":" + bucket.getDocCount());
                            }
                        }
                    }*/
                }
            } else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }

        return Response.ok(ret.toString()).build();
    }

    @Path("/facet")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response facetSearch(@Context UriInfo context) {
        return Response.ok().build();
    }
}
