package mx.gob.cultura.search;

import com.mongodb.util.JSON;
import mx.gob.cultura.commons.Util;
import mx.gob.cultura.search.api.v1.SearchEndPoint;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * {@link ServletContextListener} that manages ElasticSearch client creation and index initialization.
 * @author Hasdai Pacheco
 */
public class SearchServletContextListener implements ServletContextListener {
    private RestHighLevelClient c;
    private String indexName;
    private String envName;

    /**
     * Constructor. Creates a new instance of {@link SearchServletContextListener}.
     */
    public SearchServletContextListener () {
        c = Util.DB.getElasticClient();
        indexName = SearchEndPoint.getIndexName();
        envName = Util.getEnvironmentName();
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("Starting ElasticSearch index...");

        try {
            Response resp = c.getLowLevelClient().performRequest("HEAD", indexName);
            if(resp.getStatusLine().getStatusCode() != RestStatus.OK.getStatus()) {
                createESIndex();
            } else {
                System.out.println("Index "+ indexName +" already exists...");
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }

        try {
            //Remove test index if env is production
            if (Util.ENV_PRODUCTION.equals(envName)) {
                System.out.println("Removing test index...");
                Response resp = c.getLowLevelClient().performRequest("DELETE", SearchEndPoint.REPO_INDEX_TEST);
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
        boolean ret = false;
        System.out.println("Creating index "+ indexName +"...");
        InputStream is = getClass().getClassLoader().getResourceAsStream("indexmapping_cultura.json");
        if (null != is) {
            String mapping = Util.FILE.readFromStream(is, StandardCharsets.UTF_8.name());
            HttpEntity body = new NStringEntity(mapping, ContentType.APPLICATION_JSON);
            HashMap<String, String> params = new HashMap<>();

            try {
                Response resp = c.getLowLevelClient().performRequest("PUT", "/"+ indexName, params, body);
                System.out.println("Index " + indexName + " created...");
                ret = resp.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
            } catch (IOException ioex) {
                ioex.printStackTrace();
            }
        }

        //Load test data
        if (ret && Util.ENV_DEVELOPMENT.equals(envName)) {
            System.out.println("Loading test data");
            InputStream datas = getClass().getClassLoader().getResourceAsStream("data.json");
            if (null != datas) {
                String jsonString = Util.FILE.readFromStream(datas, StandardCharsets.UTF_8.name());
                JSONArray data = new JSONArray(jsonString);
                for (int i = 0; i < data.length(); i++) {
                    JSONObject o = data.getJSONObject(i);
                    IndexRequest request = new IndexRequest(indexName, "bic");
                    request.source(o.toString(), XContentType.JSON);

                    try {
                        IndexResponse indexResponse = c.index(request);
                    } catch (IOException ioex) {
                        ioex.printStackTrace();
                    }
                }
            }
        }

        return ret;
    }
}
