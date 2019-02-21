package mx.gob.cultura.endpoint.oai.transformers;

import java.text.SimpleDateFormat;
import java.util.Date;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class ElasticOAIRecordTrasformer implements OAITransformer<JSONObject, Element>  {

    private static final Logger LOGGER = Logger.getLogger(ElasticOAIRecordTrasformer.class);
    private DocumentBuilder builder = null;
    private String metadataPrefix;
    private boolean onlyIdentifier;

    public ElasticOAIRecordTrasformer(String metadataPrefix, boolean onlyIdentifier) {
        this.metadataPrefix = metadataPrefix;
        this.onlyIdentifier = onlyIdentifier;
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            LOGGER.error("Initializing OAI Record transformer", ex);
        }
    }

    /*
   <record> 
    <header>
      <identifier>oai:arXiv.org:cs/0112017</identifier> 
      <datestamp>2001-12-14</datestamp>
      <setSpec>cs</setSpec> 
      <setSpec>math</setSpec>
    </header>
    <metadata>
      ...
    </metadata>
  </record>    
    
     */
    @Override
    public Element transform(JSONObject source) {
        Document doc = builder.newDocument();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        

        Element root = doc.createElement("record");
        Element header = doc.createElement("header");
        Element identifier = doc.createElement("identifier");
        identifier.appendChild(doc.createTextNode(source.getString("culturaoaiid")));
        header.appendChild(identifier);
        Element datestamp = doc.createElement("datestamp");
        datestamp.appendChild(doc.createTextNode(sdf.format(new Date(source.getLong("indexcreated")))));

        header.appendChild(datestamp);        
        if(source.has("holderid")){
            String value = source.getString("holderid");
            if (value != null && !value.isEmpty()) {
                Element setSpec = doc.createElement("setSpec");
                header.appendChild(setSpec);
                setSpec.appendChild(doc.createTextNode(value.trim()));
            }
        }
        
        root.appendChild(header);

        if(!onlyIdentifier){
            OAITransformer transformer = null;

            Element metadata = doc.createElement("metadata");      
            switch (metadataPrefix) {
                case "oai_dc":
                    transformer = new ElasticOAIDCMetaTransformer();
                    break;
                case "vra":
                    transformer = new ElasticVRAMetaTransformer();         
                    break;                    
                case "mods":
                    // @todo transformer = new ElasticMODSMetaTransformer();         
                    break;
            }       
            if (transformer != null) {
                Node e = doc.importNode((Node) transformer.transform(source), true);
                metadata.appendChild((Node) e);
            } else {
                metadata.appendChild(doc.createTextNode("unsupported prefix:" + metadataPrefix));
            }        
            root.appendChild(metadata);
        }
        return root;

    }
}
