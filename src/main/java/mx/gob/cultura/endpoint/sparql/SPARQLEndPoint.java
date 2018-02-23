package mx.gob.cultura.endpoint.sparql;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.Prologue;
import com.hp.hpl.jena.tdb.TDBFactory;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ResponseModel;
import org.apache.jena.fuseki.servlets.ResponseResultSet;
import org.json.JSONObject;

/**
 * SPARQL EndPoint for query execution.
 * @author Hasdai Pacheco
 */
@Path("/")
public class SPARQLEndPoint {
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
    
    public static Dataset getDataset() {
        return dataset;
    }

    static Model getModel() {
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
     * Processes query requests.
     * @param request The {@link Request} object.
     * @param response The {@link Response} object.
     * @param body Request body String as follows: {resultType: "JSON|XML|RDF|CSV", query:""}
     */
    
    @POST
    public void executeQuery(@Context HttpServletRequest request, @Context HttpServletResponse response, String body) {
        String q = ""; //Get SPARQL query
        String type="";
        
        response.setCharacterEncoding("UTF-8");

        if(null!=body && body.trim().length()>0){
            String[] params = body.split("&");
            String[] queryparam = params[0].split("=");
            String[] formatparam = params[1].split("=");

            q = queryparam[1];
            type = formatparam[1];
            if(null==type) type="XML";
        
            if(null!=type){
                switch (type){
                    case "XML":
                        response.setContentType("application/xml");
                       break;
                    case "RDF":
                       response.setContentType("application/rdf+xml");
                       break; 
                    case "CSV":
                       response.setContentType("text/csv");
                       break; 
                    case "JSON":
                       response.setContentType("application/json");
                       break; 
                }
            }
            
            if (null == q || q.isEmpty()) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }

            //Execute query
            Object ret = execQuery(q);
            if (null == ret) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }

            //Response resp = Response.ok().build();
            if (ret instanceof ResultSet) { //Select query
                //Build response
                ResponseResultSet.doResponseResultSet(new HttpAction(0,request,response,false), (ResultSet)ret, prologue);
            } else if (ret instanceof Model) { //Describe or construct query
                //Build response
                ResponseModel.doResponseModel(new HttpAction(0,request,response,false), (Model) ret);
            } else if (ret instanceof Boolean) { //Ask query
                //Build response
                ResponseResultSet.doResponseResultSet(new HttpAction(0,request,response,false), (Boolean)ret);
            }
        }
        
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
                return qe.execAsk();
            } else if (query.isDescribeType()) {
                return qe.execDescribe();
            } else if (query.isConstructType()) {
                return qe.execConstruct();
            }
        }

        return null;
    }
}
