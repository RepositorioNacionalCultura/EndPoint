package mx.gob.cultura.search.api.v1;

import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.dizitart.no2.filters.Filters;
import org.dizitart.no2.mapper.JacksonMapper;
import org.dizitart.no2.mapper.NitriteMapper;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

@Path("/search")
public class SearchEndPoint {
    NitriteCollection coll;
    Nitrite db;

    public SearchEndPoint() {
        db = Util.getDB();
        coll = db.getCollection("objects");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response keywordSearch(@Context UriInfo context) {
        MultivaluedMap<String, String> params = context.getQueryParameters();
        String q = params.getFirst("q");
        String id = params.getFirst("identifier");

        JSONArray ret = new JSONArray();
        Cursor cur;

        if (null != id && !id.isEmpty()) {
            cur = coll.find(Filters.elemMatch("identifier", Filters.eq("value", id)));
        } else if (null != q && !q.isEmpty()) {
            cur = coll.find(Filters.or(Filters.text("title", q), Filters.text("description", q)));
        } else {
            cur = coll.find();
        }

        for (Document doc : cur) {
            NitriteMapper nitriteMapper = new JacksonMapper();
            ret.put(new JSONObject(nitriteMapper.toJson(doc)));
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
