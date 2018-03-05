package mx.gob.cultura.endpoint;

import com.hp.hpl.jena.rdf.model.Model;
import mx.gob.cultura.commons.Util;
import mx.gob.cultura.endpoint.sparql.SPARQLEndPoint;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class SPARQLServletContextListener implements ServletContextListener {
    private static final Logger log = Logger.getLogger(SPARQLServletContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        String env = Util.getEnvironmentName();

        log.debug("ENVIRONMENT: "+env);
        log.debug("Loading model data from TDB");
        Model model = SPARQLEndPoint.getModel();

        //Load sample data
        log.debug("loading sample data...");
        RDFDataMgr.read(model, SPARQLServletContextListener.class.getClassLoader().getResourceAsStream("sampledata.nt"), Lang.NTRIPLES);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.debug("Closing model");
        SPARQLEndPoint.getModel().close();
    }
}
