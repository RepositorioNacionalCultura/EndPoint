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
        http://purl.org/dc/terms/
        oaidc.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
        oaidc.setAttribute("xsi:schemaLocation", "http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd");
        oaidc.setAttribute("xmlns:dcterms","http://purl.org/dc/terms/");
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
                            subject.appendChild(doc.createTextNode(value.trim()));
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
        
        /*if(source.has("publisher")){
            String value = source.getString("publisher");
            if (value != null && !value.isEmpty()) {
                Element publisher = doc.createElement("dc:publisher");
                publisher.appendChild(doc.createTextNode(value));
                oaidc.appendChild(publisher);
            }
        }*/
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
        
        /*if(source.has("digitalObject")){
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
        }*/
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
/*        if(source.has("rights")){
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
        }*/
        /* 
        DC solicitados por el cliente segun(no se confirmo) al paper
        http://dcpapers.dublincore.org/pubs/article/view/871/867.
        */
        /* Identificador del BIC */
        if(source.has("culturaoaiid")){
            String value = source.getString("culturaoaiid");
            if (value != null && !value.isEmpty()) {
                Element identifier = doc.createElement("dcterms:identifier");
                identifier.appendChild(doc.createTextNode(value));
                oaidc.appendChild(identifier);
            }
        }
        /* Título del BIC ya*/
        /* Número */
        if(source.has("number")){
            String value = source.getString("number");
            if (value != null && !value.isEmpty()) {
                Element number = doc.createElement("dcterms:identifier");
                number.appendChild(doc.createTextNode(value));
                oaidc.appendChild(number);
            }
        }
        /*Subtítulo*/
        if(source.has("subtitle")){
            String value = source.getString("subtitle");
            if (value != null && !value.isEmpty()) {
                Element subtitle = doc.createElement("dcterms:alternative");
                subtitle.appendChild(doc.createTextNode(value));
                oaidc.appendChild(subtitle);
            }
        }
        /*Creador del BIC (Nombre)
        Creador del BIC (Apellido)      ya */
        /*Nota creador del BIC*/
        if(source.has("creatornote")){
            String value = source.getString("creatornote");
            if (value != null && !value.isEmpty()) {
                Element creatornote = doc.createElement("dc:creator");
                creatornote.appendChild(doc.createTextNode(value));
                oaidc.appendChild(creatornote);
            }
        }
        /*Grupo ceador del BIC*/
        if(source.has("creatorgroup")){
            String value = source.getString("creatorgroup");
            if (value != null && !value.isEmpty()) {
                Element creatorgroup = doc.createElement("dc:creator");
                creatorgroup.appendChild(doc.createTextNode(value));
                oaidc.appendChild(creatorgroup);
            }
        }
        /*Institución creadora del BIC*/
        if(source.has("publisher")){
            String value = source.getString("publisher");
            if (value != null && !value.isEmpty()) {
                Element publisher = doc.createElement("dc:contributor");
                publisher.appendChild(doc.createTextNode(value));
                oaidc.appendChild(publisher);
            }
        }
        /*Lengua                        ya*/
        /*Dimension*/
        if(source.has("dimension")){
            String value = source.getString("dimension");
            if (value != null && !value.isEmpty()) {
                Element dimension = doc.createElement("dcterms:extent");
                dimension.appendChild(doc.createTextNode(value));
                oaidc.appendChild(dimension);
            }
        }         
        /*Unidad*/
        if(source.has("unidad")){
            String value = source.getString("unidad");
            if (value != null && !value.isEmpty()) {
                Element dimension = doc.createElement("dc:format");
                dimension.appendChild(doc.createTextNode(value));
                oaidc.appendChild(dimension);
            }
        }  
        /*Fecha ya*/
        /*Rango inicial
        Rango final*/
        if(source.has("periodcreated")){
            JSONObject jperiodcreated =source.getJSONObject("periodcreated");
            if(jperiodcreated!=null){
                if(jperiodcreated.has("datestart")){
                    String value=jperiodcreated.getString("datestart");
                    Element created = doc.createElement("dcterms:created");                        
                    created.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(created);
                }
                if(jperiodcreated.has("dateend")){
                    String value=jperiodcreated.getString("dateend");
                    Element created = doc.createElement("dcterms:created");                        
                    created.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(created);
                }
            }    
        }
        /*Tipo del BIC        ya */

        /*Institución*/
        if(source.has("holder")){  
            JSONArray jholdera = source.getJSONArray("holder");
            if (jholdera != null && jholdera.length() > 0) {
                for (Object obj : jholdera) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        Element holder = doc.createElement("dc:publisher");                        
                        holder.appendChild(doc.createTextNode(value.trim()));
                        oaidc.appendChild(holder);
                    }
                }
            }
        }  
        /*Nota Institución*/
        if(source.has("holdernote")){
            String value = source.getString("holdernote");
            if (value != null && !value.isEmpty()) {
                Element holdernote = doc.createElement("dc:publisher");
                holdernote.appendChild(doc.createTextNode(value));
                oaidc.appendChild(holdernote);
            }
        }  
        
        /*Palabras clave*/
        if(source.has("keywords")){
            JSONArray jsubject = source.getJSONArray("keywords");
            if (jsubject != null && jsubject.length() > 0) {
                for (Object obj : jsubject) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        if(value.contains(";")){
                            String[] values=value.split(";");
                            for (String avalue : values) {
                                Element subject = doc.createElement("dcterms:subject");
                                oaidc.appendChild(subject);
                                subject.appendChild(doc.createTextNode(avalue.trim()));                                
                            }                                                    
                        }else{
                            Element subject = doc.createElement("dcterms:subject");
                            oaidc.appendChild(subject);
                            subject.appendChild(doc.createTextNode(value.trim()));
                        }
                    }                                           
                }                                               
            }
        }
        /*Derechos sobre el BIC            ya
        Declaración de uso sobre el objeto digital que representa el BIC
        Declaración de uso sobre el objeto digital que representa el BIC (URL)*/
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
                    Element rights = doc.createElement("dcterms:RightsStatement");                        
                    rights.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(rights);
                }
                if(jrights.has("url")){
                    String value=jrights.getString("url");
                    Element rights = doc.createElement("dcterms:RightsStatement");                        
                    rights.appendChild(doc.createTextNode(value));
                    oaidc.appendChild(rights);
                }
            }    
        }
        /*Serie*/
        if(source.has("serie")){
            String value = source.getString("serie");
            if (value != null && !value.isEmpty()) {
                Element serie = doc.createElement("dcterms:isPartOf");
                serie.appendChild(doc.createTextNode(value));
                oaidc.appendChild(serie);
            }
        }  
        /*Media*/
        if(source.has("media")){
            String value = source.getString("media");
            if (value != null && !value.isEmpty()) {
                Element media = doc.createElement("dcterms:FileFormat");
                media.appendChild(doc.createTextNode(value));
                oaidc.appendChild(media);
            }
        } 
        /*Formato           ya
        Nombre del objeto digital*/
        if(source.has("digitalObject")){
            JSONArray jdigitalObjecta = source.getJSONArray("digitalObject");
            if (jdigitalObjecta != null && jdigitalObjecta.length() > 0) {
                for (Object obj : jdigitalObjecta) {
                    JSONObject jdigitalObject = (JSONObject) obj;
                    if (jdigitalObject != null && jdigitalObject.has("mediatype")){
                        if(jdigitalObject.getJSONObject("mediatype").has("mime")) {
                            String value= jdigitalObject.getJSONObject("mediatype").getString("mime");
                            Element format = doc.createElement("dc:format");                        
                            format.appendChild(doc.createTextNode(value));
                            oaidc.appendChild(format);
                        }
                        if(jdigitalObject.getJSONObject("mediatype").has("name")) {
                            String value= jdigitalObject.getJSONObject("mediatype").getString("name");
                            Element name = doc.createElement("dcterms:source");                        
                            name.appendChild(doc.createTextNode(value));
                            oaidc.appendChild(name);
                        }                     
                    }
                }
            }
        }        
        /*Thumbnail*/
        if(source.has("resourcethumbnail")){
            String value = source.getString("resourcethumbnail");
            if (value != null && !value.isEmpty()) {
                Element thumbnail = doc.createElement("dcterms:source");
                thumbnail.appendChild(doc.createTextNode(value));
                oaidc.appendChild(thumbnail);
            }
        }
        /*Editorial*/
        if(source.has("editorial")){
            String value = source.getString("editorial");
            if (value != null && !value.isEmpty()) {
                Element editorial = doc.createElement("dcterms:bibliographicCitation");
                editorial.appendChild(doc.createTextNode(value));
                oaidc.appendChild(editorial);
            }
        }        
        /*Imprenta*/
        if(source.has("press")){
            String value = source.getString("press");
            if (value != null && !value.isEmpty()) {
                Element press = doc.createElement("dcterms:bibliographicCitation");
                press.appendChild(doc.createTextNode(value));
                oaidc.appendChild(press);
            }
        }         
        /*Serie*/
        if(source.has("serie")){
            String value = source.getString("serie");
            if (value != null && !value.isEmpty()) {
                Element serie = doc.createElement("dcterms:bibliographicCitation");
                serie.appendChild(doc.createTextNode(value));
                oaidc.appendChild(serie);
            }
        }         
        /*Descripción*/
        if(source.has("description")){
            Element description = doc.createElement("dcterms:description");
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
        /*Colección*/
        if(source.has("reccollection")){
            String value = source.getString("reccollection");
            if (value != null && !value.isEmpty()) {
                Element collection = doc.createElement("dcterms:isPartOf");
                collection.appendChild(doc.createTextNode(value));
                oaidc.appendChild(collection);
            }
        }  
        /*Disciplina*/
        if(source.has("discipline")){
            String value = source.getString("discipline");
            if (value != null && !value.isEmpty()) {
                Element discipline = doc.createElement("dc:type");
                discipline.appendChild(doc.createTextNode(value));
                oaidc.appendChild(discipline);
            }
        }
        /*Lugar
        Nota lugar*/
        /*URL youtube*/
        if(source.has("lugar")){
            String value = source.getString("lugar");
            if (value != null && !value.isEmpty()) {
                Element lugar = doc.createElement("dcterms:Location");
                lugar.appendChild(doc.createTextNode(value));
                oaidc.appendChild(lugar);
            }
        } 
        /*Capítulo*/
        if(source.has("chapter")){
            String value = source.getString("chapter");
            if (value != null && !value.isEmpty()) {
                Element chapter = doc.createElement("dcterms:bibliographicCitation");
                chapter.appendChild(doc.createTextNode(value));
                oaidc.appendChild(chapter);
            }
        } 
        /*Área de conocimiento*/
        if(source.has("knownarea")){
            String value = source.getString("knownarea");
            if (value != null && !value.isEmpty()) {
                Element knownarea = doc.createElement("dcterms:references");
                knownarea.appendChild(doc.createTextNode(value));
                oaidc.appendChild(knownarea);
            }
        }
        /*Créditos*/
        if(source.has("credits")){
            String value = source.getString("credits");
            if (value != null && !value.isEmpty()) {
                Element credits = doc.createElement("dcterms:references");
                credits.appendChild(doc.createTextNode(value));
                oaidc.appendChild(credits);
            }
        }         
        /*Episodio*/
        if(source.has("episode")){
            String value = source.getString("episode");
            if (value != null && !value.isEmpty()) {
                Element episode = doc.createElement("dcterms:references");
                episode.appendChild(doc.createTextNode(value));
                oaidc.appendChild(episode);
            }
        }              
        /*Director*/
        if(source.has("director")){
            String value = source.getString("director");
            if (value != null && !value.isEmpty()) {
                Element director = doc.createElement("dcterms:references");
                director.appendChild(doc.createTextNode(value));
                oaidc.appendChild(director);
            }
        }          
        /*Productor*/
        if(source.has("producer")){
            String value = source.getString("producer");
            if (value != null && !value.isEmpty()) {
                Element producer = doc.createElement("dcterms:references");
                producer.appendChild(doc.createTextNode(value));
                oaidc.appendChild(producer);
            }
        }                     
        /*Guion*/
        if(source.has("screenplay")){
            String value = source.getString("screenplay");
            if (value != null && !value.isEmpty()) {
                Element screenplay = doc.createElement("dcterms:references");
                screenplay.appendChild(doc.createTextNode(value));
                oaidc.appendChild(screenplay);
            }
        }        
        /*Reparto*/
        if(source.has("distribution")){
            String value = source.getString("distribution");
            if (value != null && !value.isEmpty()) {
                Element distribution = doc.createElement("dcterms:references");
                distribution.appendChild(doc.createTextNode(value));
                oaidc.appendChild(distribution);
            }
        }          
        /*Clasificación*/
        if(source.has("clasification")){
            String value = source.getString("clasification");
            if (value != null && !value.isEmpty()) {
                Element clasification = doc.createElement("dcterms:references");
                clasification.appendChild(doc.createTextNode(value));
                oaidc.appendChild(clasification);
            }
        }         
        /*Dirección*/
        if(source.has("direction")){
            String value = source.getString("direction");
            if (value != null && !value.isEmpty()) {
                Element direction = doc.createElement("dcterms:references");
                direction.appendChild(doc.createTextNode(value));
                oaidc.appendChild(direction);
            }
        }          
        /*Producción*/
        if(source.has("production")){
            String value = source.getString("production");
            if (value != null && !value.isEmpty()) {
                Element production = doc.createElement("dcterms:references");
                production.appendChild(doc.createTextNode(value));
                oaidc.appendChild(production);
            }
        }           
        /*Música*/
        if(source.has("music")){
            String value = source.getString("music");
            if (value != null && !value.isEmpty()) {
                Element music = doc.createElement("dcterms:references");
                music.appendChild(doc.createTextNode(value));
                oaidc.appendChild(music);
            }
        }        
        /*Libreto*/
        if(source.has("libreto")){
            String value = source.getString("libreto");
            if (value != null && !value.isEmpty()) {
                Element libreto = doc.createElement("dcterms:references");
                libreto.appendChild(doc.createTextNode(value));
                oaidc.appendChild(libreto);
            }
        }         
        /*Dirección de música*/
        if(source.has("musicdirection")){
            String value = source.getString("musicdirection");
            if (value != null && !value.isEmpty()) {
                Element musicdirection = doc.createElement("dcterms:references");
                musicdirection.appendChild(doc.createTextNode(value));
                oaidc.appendChild(musicdirection);
            }
        }         
        /*Invitado*/
        if(source.has("invited")){
            String value = source.getString("invited");
            if (value != null && !value.isEmpty()) {
                Element invited = doc.createElement("dcterms:references");
                invited.appendChild(doc.createTextNode(value));
                oaidc.appendChild(invited);
            }
        }  
        /*Tema*/
        if(source.has("theme")){
            String value = source.getString("theme");
            if (value != null && !value.isEmpty()) {
                Element theme = doc.createElement("dcterms:references");
                theme.appendChild(doc.createTextNode(value));
                oaidc.appendChild(theme);
            }
        }  
        /*Sinopsis*/
        if(source.has("synopsis")){
            String value = source.getString("synopsis");
            if (value != null && !value.isEmpty()) {
                Element synopsis = doc.createElement("dcterms:description");
                synopsis.appendChild(doc.createTextNode(value));
                oaidc.appendChild(synopsis);
            }
        }                 
        /*PERSONAJES*/
        if(source.has("characters")){
            String value = source.getString("characters");
            if (value != null && !value.isEmpty()) {
                Element characters = doc.createElement("dcterms:references");
                characters.appendChild(doc.createTextNode(value));
                oaidc.appendChild(characters);
            }
        } 
        /*Categoría*/
        if(source.has("category")){
            String value = source.getString("category");
            if (value != null && !value.isEmpty()) {
                Element category = doc.createElement("dcterms:references");
                category.appendChild(doc.createTextNode(value));
                oaidc.appendChild(category);
            }
        } 
        /*Subcategoría*/
        if(source.has("subcategory")){
            String value = source.getString("subcategory");
            if (value != null && !value.isEmpty()) {
                Element subcategory = doc.createElement("dcterms:references");
                subcategory.appendChild(doc.createTextNode(value));
                oaidc.appendChild(subcategory);
            }
        }         
        /*Técnica*/
        if(source.has("techmaterial")){
            String value = source.getString("techmaterial");
            if (value != null && !value.isEmpty()) {
                Element techmaterial = doc.createElement("dc:type");
                techmaterial.appendChild(doc.createTextNode(value));
                oaidc.appendChild(techmaterial);
            }
        }
        /*Nombre del Archivo original      ya*/
        
        /*Agrupación lingüística*/
        if(source.has("grplang")){
            JSONArray jgrplanga = source.getJSONArray("grplang");
            if (jgrplanga != null && jgrplanga.length() > 0) {
                for (Object obj : jgrplanga) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        Element grplang = doc.createElement("dc:creator");                        
                        grplang.appendChild(doc.createTextNode(value.trim()));
                        oaidc.appendChild(grplang);
                    }
                }
            }
        }
        /*Archivo*/
        if(source.has("archivo_de_origen")){
            String value = source.getString("archivo_de_origen");
            if (value != null && !value.isEmpty()) {
                Element archivo_de_origen = doc.createElement("dcterms:source");
                archivo_de_origen.appendChild(doc.createTextNode(value));
                oaidc.appendChild(archivo_de_origen);
            }
        }        
        /*Referencia bibiliográfica*/
        if(source.has("reference")){
            String value = source.getString("reference");
            if (value != null && !value.isEmpty()) {
                Element reference = doc.createElement("dcterms:bibliographicCitation");
                reference.appendChild(doc.createTextNode(value));
                oaidc.appendChild(reference);
            }
        }

        /*setSpec*/
        if(source.has("setSpec")){
            JSONArray jsetSpeca = source.getJSONArray("setSpec");
            if (jsetSpeca != null && jsetSpeca.length() > 0) {
                for (Object obj : jsetSpeca) {
                    JSONObject jsetSpec = (JSONObject) obj;
                    if (jsetSpec != null && jsetSpec.has("keyword")){
                        String value= jsetSpec.getString("keyword");
                        Element format = doc.createElement("dcterms:isPartOf");                        
                        format.appendChild(doc.createTextNode(value));
                        oaidc.appendChild(format);              
                    }
                }
            }
        }         
        /*creador     ya*/
        /*dimension*/
        if(source.has("dimension")){
            String value = source.getString("dimension");
            if (value != null && !value.isEmpty()) {
                Element dimension = doc.createElement("dcterms:extent");
                dimension.appendChild(doc.createTextNode(value));
                oaidc.appendChild(dimension);
            }
        }
        /*material*/

        /*lenguaje        ya*/
        /*estado*/
        if(source.has("state")){
            String value = source.getString("state");
            if (value != null && !value.isEmpty()) {
                Element dimension = doc.createElement("dcterms:Location");
                dimension.appendChild(doc.createTextNode(value));
                oaidc.appendChild(dimension);
            }
        }
        /*pais*/
        if(source.has("pais")){
            String value = source.getString("pais");
            if (value != null && !value.isEmpty()) {
                Element dimension = doc.createElement("dcterms:Location");
                dimension.appendChild(doc.createTextNode(value));
                oaidc.appendChild(dimension);
            }
        }
        /*URI
        */           
        return oaidc;

    }
}
