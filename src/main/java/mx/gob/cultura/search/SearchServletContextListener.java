package mx.gob.cultura.search;

import mx.gob.cultura.commons.Util;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

/**
 * {@link ServletContextListener} that manages ElasticSearch client creation and index initialization.
 * @author Hasdai Pacheco
 */
public class SearchServletContextListener implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("Starting ElasticSearch indices...");
        RestHighLevelClient c = Util.DB.getElasticClient();

        try {
            Response resp = c.getLowLevelClient().performRequest("HEAD", "cultura");
            if(resp.getStatusLine().getStatusCode() != RestStatus.OK.getStatus()) {
                System.out.println("Creating index cultura...");
                if (createESIndex()) {
                    System.out.println("Index cultura created...");
                }
            } else {
                System.out.println("Index cultura already exists...");
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        //Close clients
        Util.DB.closeElasticClients();
    }

    /**
     * Creates default ElasticSearch index for cultural objects.
     * @return true if creation succeeds, false otherwise
     */
    private boolean createESIndex() {
        InputStream is = getClass().getClassLoader().getResourceAsStream("indexmapping_cultura.json");
        if (null != is) {
            String mapping = Util.FILE.readFromStream(is, "UTF-8");
            HttpEntity body = new NStringEntity(mapping, ContentType.APPLICATION_JSON);

            RestHighLevelClient c = Util.DB.getElasticClient();
            HashMap<String, String> params = new HashMap<>();

            try {
                Response resp = c.getLowLevelClient().performRequest("PUT", "/cultura", params, body);
                return resp.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
            } catch (IOException ioex) {
                ioex.printStackTrace();
            }
        }

        return false;
    }
}
