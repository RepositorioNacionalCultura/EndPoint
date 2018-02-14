package mx.gob.cultura.endpoint.sparql;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

/**
 * SPARQL EndPoint for query execution.
 * @author Hasdai Pacheco
 */
public class SPARQLEndPoint {
    private Model model;

    /**
     * Constructor. Creates a new instance of {@link SPARQLEndPoint}.
     */
    public SPARQLEndPoint () {
        //Retrieve model from LevelDB
    }

    /**
     * Processes query requests.
     * @param request The {@link Request} object.
     * @param response The {@link Response} object.
     * @param body Request body String as follows: {resultType: "XML|RDF|CSV", query:""}
     * @return {@link Response} object with query execution result in specified format.
     */
    @Path("/")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response executeQuery(@Context Request request, @Context Response response, String body) {
        String q = ""; //Get SPARQL query

        if (null == q || q.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        //Execute query
        Object ret = execQuery(q);
        if (null == ret) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        Response resp = Response.ok().build();
        if (ret instanceof ResultSet) { //Select query
            //Build response
        } else if (ret instanceof Model) { //Describe or construct query
            //Build response
        } else if (ret instanceof Boolean) { //Ask query
            //Build response
        }

        //Change response according to content-negotiation
        return resp;
    }

    /**
     * Executes a SPARQL query and returns result object.
     * @param q SPARQL query String
     * @return Object with query result.
     */
    private Object execQuery(String q) {
        if (null != q && null != model) {
            Query query = QueryFactory.create(q, Syntax.syntaxSPARQL_11);
            QueryExecution qe = QueryExecutionFactory.create(q, model);

            if (query.isSelectType()) {
                return qe.execSelect();
            } else if (query.isAskType()) {
                return Boolean.valueOf(qe.execAsk());
            } else if (query.isDescribeType()) {
                return qe.execDescribe();
            } else if (query.isConstructType()) {
                return qe.execConstruct();
            }
        }

        return null;
    }
}
