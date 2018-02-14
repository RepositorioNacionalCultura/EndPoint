package mx.gob.cultura.endpoint.rdf;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

/**
 * RDF EndPoint for MDM ontology and BIC resources.
 * @author Hasdai Pacheco
 */
public class RDFEndPoint {
    /**
     * Constructor. Creates a new instance of {@link RDFEndPoint}
     */
    public RDFEndPoint () { }

    /**
     * Processes request to get ontology definition.
     * @return Response with ontology definition in RDF format.
     */
    @GET
    @Path("/ontology")
    public Response getOntology() {
        return Response.ok().build();
    }

    /**
     * Processes request to get ontology class definition.
     * @return Response with ontology class definition in RDF format.
     */
    @GET
    @Path("/ontology/class/{classname}")
    public Response getOntologyClass(@PathParam("classname") String className) {
        return Response.ok().build();
    }

    /**
     * Processes request to get ontology property definition.
     * @return Response with ontology property definition in RDF format.
     */
    @GET
    @Path("/ontology/property/{propertyname}")
    public Response getOntologyProperty(@PathParam("propertyname") String propertyName) {
        return Response.ok().build();
    }

    /**
     * Processes request to get ontology resource data.
     * @return Response with ontology resource data in RDF format.
     */
    @GET
    @Path("/resource/{resourceId}")
    public Response getResource(@PathParam("resourceId") String resourceId) {
        return Response.ok().build();
    }
}
