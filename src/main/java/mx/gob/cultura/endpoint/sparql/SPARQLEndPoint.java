package mx.gob.cultura.endpoint.sparql;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.Prologue;
import com.hp.hpl.jena.sparql.resultset.SPARQLResult;
import com.hp.hpl.jena.tdb.TDBFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * SPARQL EndPoint for query execution.
 * @author Hasdai Pacheco
 * @author Juan Fernandez
 */
@Path("/")
public class SPARQLEndPoint {
    private static final Logger log = Logger.getLogger(SPARQLEndPoint.class);
    private static Model model=null;
    private static Dataset dataset=null;
    private static Prologue prologue=null;

    /**
     * Constructor. Creates a new instance of {@link SPARQLEndPoint}.
     */
    public SPARQLEndPoint () {
        //Retrieve model from LevelDB
        getModel();
        //prologue=new Prologue(model.getGraph().getPrefixMapping());
    }

    /**
     * Gets Dataset.
     * @return DataSet
     */
    public static Dataset getDataset() {
        return dataset;
    }

    /**
     * Gets {@link Model} to execute queries against.
     * @return
     */
    public static Model getModel() {
        if(model==null)
        {
            try
            {
                //            HashMap<String,String> params=new HashMap();
                //            params.put("path", "/data/leveldb");
                //            //model=new ModelCom(new SWBTSGraphCache(new SWBTSGraph(new GraphImp("bsbm",params)),1000));
                //            //model=new ModelCom(new SWBTSGraph(new GraphImp("bsbm",params)));
                //            //model=new ModelCom(new SWBTSGraphCache(new SWBTSGraph(new GraphImp("swb",params)),1000));
                //            model=new ModelCom(new SWBTSGraph(new GraphImp("swb",params)));

                String directory = "/data/tdb" ;
                dataset = TDBFactory.createDataset(directory) ;
                model = dataset.getDefaultModel();
                prologue=new Prologue(model.getGraph().getPrefixMapping());
            }catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        return model;
    }


    /**
     * Processes requests with application/json header.
     * @param body Request body;
     * @return Response object with results of query execution.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response executeQueryJSON(String body) {
        JSONObject payload = new JSONObject(body);
        String query = payload.optString("query", "");
        String format = payload.optString("format", "xml");

        if (null == query || query.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        Response resp = getQueryResponse(query, format);
        return resp;
    }

    /**
     * Processes query requests with x-www-form-urlencoded header.
     * @param query Query string to execute.
     * @param format Response format for results.
     * @return Response object with results of query execution.
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response executeQueryForm(@FormParam("query") String query,
                                 @FormParam("format") String format) throws IOException {

        if (null == query || query.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        Response resp = getQueryResponse(query, format);
        return resp;
    }

    /**
     * Executes a SPARQL query and returns the resulting {@link SPARQLResult} object.
     * @param q SPARQL query String.
     * @return {@link SPARQLResult} object
     */
    private SPARQLResult execQuery(String q) {
        if (null != q && null != model) {
            Query query = QueryFactory.create(q, Syntax.syntaxSPARQL_11);
            QueryExecution qe = QueryExecutionFactory.create(q, model);

            if (query.isSelectType()) {
                return new SPARQLResult(qe.execSelect());
            } else if (query.isAskType()) {
                return new SPARQLResult(qe.execAsk());
            } else if (query.isDescribeType()) {
                return new SPARQLResult(qe.execDescribe());
            } else if (query.isConstructType()) {
                return new SPARQLResult(qe.execConstruct());
            }
        }

        return null;
    }

    /**
     * Formats a Graph result according to requested response format.
     * @param rs {@link SPARQLResult} object.
     * @param format Response format short String.
     * @return String of formated {@link SPARQLResult} object.
     */
    private String formatGraph(SPARQLResult rs, String format) {
        String ret = "";
        Lang lang = null;

        if (null != rs) {
            if ("xml".equals(format)) {
                lang = RDFLanguages.contentTypeToLang(org.apache.jena.atlas.web.ContentType.create(WebContent.contentTypeRDFXML));
            } else if ("json".equals(format)) {
                lang = RDFLanguages.contentTypeToLang(org.apache.jena.atlas.web.ContentType.create(WebContent.contentTypeJSONLD));
            } else if ("csv".equals(format)) {
                lang = RDFLanguages.contentTypeToLang(org.apache.jena.atlas.web.ContentType.create(WebContent.contentTypeTextCSV));
            }

            try (ByteArrayOutputStream bous = new ByteArrayOutputStream()) {
                RDFDataMgr.write(bous, rs.getModel(), lang);
                ret = new String(bous.toByteArray());
            } catch (IOException ioex) {

            }
        }

        return ret;
    }

    /**
     * Formats a ResultSet result according to requested response format.
     * @param rs {@link SPARQLResult} object.
     * @param format Response format short String.
     * @return String of formatted {@link SPARQLResult} object.
     */
    private String formatResultSet(SPARQLResult rs, String format) {
        String ret = "";

        if (null != rs) {
            try (ByteArrayOutputStream bous = new ByteArrayOutputStream()) {

                if ("xml".equals(format)) {
                    if (rs.isBoolean()) {
                        ResultSetFormatter.outputAsXML(bous, rs.getBooleanResult());
                    } else {
                        ResultSetFormatter.outputAsXML(bous, rs.getResultSet());
                    }
                } else if ("json".equals(format)) {
                    if (rs.isBoolean()) {
                        ResultSetFormatter.outputAsJSON(bous, rs.getBooleanResult());
                    } else {
                        ResultSetFormatter.outputAsJSON(bous, rs.getResultSet());
                    }
                } else if ("csv".equals(format)) {
                    if (rs.isBoolean()) {
                        ResultSetFormatter.outputAsCSV(bous, rs.getBooleanResult());
                    } else {
                        ResultSetFormatter.outputAsCSV(bous, rs.getResultSet());
                    }
                }

                ret = new String(bous.toByteArray());
            } catch (IOException ioex) {

            }
        }
        return ret;
    }

    /**
     * Gets {@link Response} object with results of query execution.
     * @param query SPARQL query String.
     * @param format Response format.
     * @return Response object with results of query execution.
     */
    private Response getQueryResponse(String query, String format) {
        if (null == query || query.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        //Execute query
        SPARQLResult ret = execQuery(query);
        if (null == ret) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if (ret.isResultSet()) {
            return Response.ok().type(getContentType(format)).entity(formatResultSet(ret, format)).build();
        } else if (ret.isGraph()) {
            return Response.ok().type(getContentType(format)).entity(formatGraph(ret, format)).build();
        } else if (ret.isBoolean()) {
            return Response.ok().type(getContentType(format)).entity(formatResultSet(ret, format)).build();
        } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Gets content type String from requested respose format.
     * @param format Response format short String.
     * @return Content-type String of response headers.
     */
    private String getContentType(String format) {
        if ("xml".equals(format)) {
            return WebContent.contentTypeRDFXML;
        } else if ("json".equals(format)) {
            return WebContent.contentTypeJSONLD;
        } else if ("csv".equals(format)) {
            return WebContent.contentTypeTextCSV;
        }
        return WebContent.contentTypeRDFXML;
    }
}
