package mx.gob.cultura.endpoint.oai.transformers;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ElasticOAIDCMetaTransformer implements OAITransformer<JSONObject, Element> {

    private static final Logger LOGGER = Logger.getLogger(ElasticOAIDCMetaTransformer.class);
    private DocumentBuilder builder = null;

    public ElasticOAIDCMetaTransformer() {
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            LOGGER.error("Initializing OAIDC metadata transformer", ex);
        }
    }

    /*
      <oai_dc:dc 
         xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
         xmlns:dc="http://purl.org/dc/elements/1.1/" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ 
         http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
        <dc:title>Using Structural Metadata to Localize Experience of 
                  Digital Content</dc:title> 
        <dc:creator>Dushay, Naomi</dc:creator>
        <dc:subject>Digital Libraries</dc:subject> 
        <dc:description>With the increasing technical sophistication of 
            both information consumers and providers, there is 
            increasing demand for more meaningful experiences of digital 
            information. We present a framework that separates digital 
            object experience, or rendering, from digital object storage 
            and manipulation, so the rendering can be tailored to 
            particular communities of users.
        </dc:description> 
        <dc:description>Comment: 23 pages including 2 appendices, 
            8 figures</dc:description> 
        <dc:date>2001-12-14</dc:date>
      </oai_dc:dc> 
     */
    @Override
    public Element transform(JSONObject source) {
        //System.out.println("---------transform metadata----------------------------------------");
        //System.out.println(source);
        Document doc = builder.newDocument();

        Element oaidc = doc.createElement("oai_dc:dc");
        oaidc.setAttribute("xmlns:oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
        oaidc.setAttribute("xmlns:dc", "http://purl.org/dc/elements/1.1/");
        oaidc.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
        oaidc.setAttribute("xsi:schemaLocation", "http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd");

        if(source.has("recordtitle")){
            Element title = doc.createElement("dc:title");
            oaidc.appendChild(title);
            JSONArray recordtitle = source.getJSONArray("recordtitle");
            if (recordtitle != null && recordtitle.length() > 0) {
                String value = "";
                for (Object obj : recordtitle) {
                    JSONObject tobj = (JSONObject) obj;
                    if ("main".equals(tobj.get("type"))&&tobj.has("value")) {
                        value = tobj.getString("value");
                    }
                }
                if (value.isEmpty()) {
                    value = ((JSONObject) recordtitle.get(1)).getString("value");
                }
                title.appendChild(doc.createTextNode(value));
            }
        }
        if(source.has("creator")){
            JSONArray jcreatora = source.getJSONArray("creator");
            if (jcreatora != null && jcreatora.length() > 0) {
                for (Object obj : jcreatora) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        Element creator = doc.createElement("dc:creator");
                        oaidc.appendChild(creator);
                        creator.appendChild(doc.createTextNode(value.trim()));
                    }
                }
            }
        }
        if(source.has("keywords")){
            JSONArray jsubject = source.getJSONArray("keywords");
            if (jsubject != null && jsubject.length() > 0) {
                for (Object obj : jsubject) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        if(value.contains(";")){
                            String[] values=value.split(";");
                            for (String avalue : values) {
                                Element subject = doc.createElement("dc:subject");
                                oaidc.appendChild(subject);
                                subject.appendChild(doc.createTextNode(avalue.trim()));                                
                            }                                                    
                        }else{
                            Element subject = doc.createElement("dc:subject");
                            oaidc.appendChild(subject);
                            subject.appendChild(doc.createTextNode(value));
                        }
                    }                                           
                }                                               
            }
        }
        if(source.has("description")){
            Element description = doc.createElement("dc:description");
            JSONArray jdescriptiona = source.getJSONArray("description");
            if (jdescriptiona != null && jdescriptiona.length() > 0) {
                StringBuilder sdescription = new StringBuilder();
                for (Object obj : jdescriptiona) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        sdescription.append(value);
                    }
                }
                description.appendChild(doc.createTextNode(sdescription.toString()));
            }
            oaidc.appendChild(description);
        }
        
        if(source.has("publisher")){
            String value = source.getString("publisher");
            if (value != null && !value.isEmpty()) {
                Element publisher = doc.createElement("dc:publisher");
                publisher.appendChild(doc.createTextNode(value));
                oaidc.appendChild(publisher);
            }
        }
        if(source.has("contributor")){
            JSONArray jcontributora = source.getJSONArray("contributor");
            if (jcontributora != null && jcontributora.length() > 0) {
                for (Object obj : jcontributora) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        Element contributor = doc.createElement("dc:contributor");                        
                        contributor.appendChild(doc.createTextNode(value.trim()));
                        oaidc.appendChild(contributor);
                    }
                }
            }
        }
        if(source.has("datecreated")){
            Element date = doc.createElement("dc:date");            
            JSONObject jdate = source.getJSONObject("datecreated");
            if (jdate != null && jdate.has("value")) {
                String value = jdate.getString("value");              
                date.appendChild(doc.createTextNode(value));
                oaidc.appendChild(date);
            }
        }
        
        if(source.has("resourcetype")){
            JSONArray jresourcetypea = source.getJSONArray("resourcetype");
            if (jresourcetypea != null && jresourcetypea.length() > 0) {
                for (Object obj : jresourcetypea) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        Element type = doc.createElement("dc:type");                        
                        type.appendChild(doc.createTextNode(value.trim()));
                        oaidc.appendChild(type);
                    }
                }
            }
        }
        
        if(source.has("digitalObject")){
            JSONArray jdigitalObjecta = source.getJSONArray("digitalObject");
            if (jdigitalObjecta != null && jdigitalObjecta.length() > 0) {
                for (Object obj : jdigitalObjecta) {
                    JSONObject jdigitalObject = (JSONObject) obj;
                    if (jdigitalObject != null && 
                            jdigitalObject.has("mediatype") &&
                            jdigitalObject.getJSONObject("mediatype").has("mime")) {
                        String value= jdigitalObject.getJSONObject("mediatype").getString("mime");
                        //if(source.has("dimension")){
                            //value+= "("+source.getString("dimension")+")";
                        //}
                        Element format = doc.createElement("dc:format");                        
                        format.appendChild(doc.createTextNode(value));
                        oaidc.appendChild(format);
                    }
                }
            }
        }
        if(source.has("identifier")){
            JSONArray jidentifiera =source.getJSONArray("identifier");
            if(jidentifiera !=null && jidentifiera.length()>0){
                for(Object obj:jidentifiera){
                    JSONObject jidentifier = (JSONObject) obj;
                    if(jidentifier !=null && jidentifier.has("value")){
                        String value= jidentifier.getString("value");
                        Element identifier = doc.createElement("dc:identifier");                        
                        identifier.appendChild(doc.createTextNode(value));
                        oaidc.appendChild(identifier);
                    }                
                }
            }
        }
        // @todo dc:source, si aplica
        if(source.has("lang")){
            JSONArray jlanga =source.getJSONArray("lang");
            if(jlanga !=null && jlanga.length()>0){
                for(Object obj:jlanga){
                    String value = (String) obj;
                    if(value !=null && !value.isEmpty()){
                        Element language = doc.createElement("dc:language");                        
                        language.appendChild(doc.createTextNode(value));
                        oaidc.appendChild(language);
                    }                
                }
            }
        }    
        // @todo dc:relation, si aplica
        // @todo dc:coverage, si aplica
        if(source.has("rights")){
            JSONObject jrights =source.getJSONObject("rights");
            if(jrights!=null){
                if(jrights.has("rightstitle")){
                    String value=jrights.getString("rightstitle");
                    Element rights = doc.createElement("dc:rights");                        
                    rights.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(rights);
                }else if(jrights.has("description")){
                    String value=jrights.getString("description");
                    Element rights = doc.createElement("dc:rights");                        
                    rights.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(rights);
                }
                if(jrights.has("url")){
                    String value=jrights.getString("url");
                    Element rights = doc.createElement("dc:rights");                        
                    rights.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(rights);
                }
            }    
        }
//System.out.println(oaidc);            
        return oaidc;

    }
}
