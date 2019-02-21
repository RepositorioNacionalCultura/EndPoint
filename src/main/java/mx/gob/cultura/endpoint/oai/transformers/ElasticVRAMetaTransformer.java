/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.gob.cultura.endpoint.oai.transformers;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 *
 * @author rene.jara
 */
public class ElasticVRAMetaTransformer implements OAITransformer<JSONObject, Element> {
    private static final Logger LOGGER = Logger.getLogger(ElasticVRAMetaTransformer.class);
    private DocumentBuilder builder = null;

    public ElasticVRAMetaTransformer() {
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            LOGGER.error("Initializing VRA metadata transformer", ex);
        }
    }
/*
<vra xmlns="http://www.vraweb.org/vracore4.htm"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.vraweb.org/vracore4.htm http://www.loc.gov/standards/vracore/vra-strict.xsd">
    <work id="w_1" source="Core 4 Sample Database (VCat)" refid="1">
        <agentSet>
            <display>unknown (French architect)</display>
            <notes/>
            <agent>
                <name vocab="ULAN" refid="500125274" type="personal">unknown</name>
                <role>architect</role>
            </agent>
        </agentSet>
        <culturalContextSet>
            <culturalContext>French</culturalContext>
        </culturalContextSet>
        <dateSet>
            <display>begun 1194 (creation); consecrated 1260 (other)</display>
            <notes>Louis IX (Saint Louis) present for consecration in 1260.</notes>
            <date type="creation">
                <earliestDate>1194</earliestDate>
                <latestDate>1194</latestDate>
            </date>
            <date type="other">
                <earliestDate>1260</earliestDate>
                <latestDate>1260</latestDate>
            </date>
        </dateSet>
        <descriptionSet>
            <display>The present cathedral was constructed on the foundations of the earlier church; the oldest parts of the cathedral are the crypt and Royal Portal (West Portal), remnants of a Romanesque church destroyed by fire in 1194.</display>
            <description source="CCO (Cataloging Cultural Objects) Catalog Examples; http://www.vrafoundation.org/ccoweb/cco/examplesindex.html (accessed 12/22/2008)">The present cathedral was constructed on the foundations of the earlier church; the oldest parts of the cathedral are the crypt and Royal Portal (West Portal), remnants of a Romanesque church destroyed by fire in 1194.</description>
        </descriptionSet>
        <locationSet>
            <display>Chartres, Centre, France</display>
            <notes>Eure-et-Loir (department)</notes>
            <location type="site">
                <name type="geographic" vocab="TGN" refid="7008267" extent="inhabited place">Chartres</name>
                <name type="geographic" vocab="TGN" refid="7002877" extent="region">Centre</name>
                <name type="geographic" vocab="TGN" refid="1000070" extent="nation">France</name>
                <name type="geographic" vocab="TGN" refid="1000003" extent="continent">Europe</name>
            </location>
        </locationSet>
        <materialSet>
            <display>stone; limestone</display>
            <notes/>
            <material/>
        </materialSet>
        <measurementsSet>
            <display>34 m (height) x 130 m (length)</display>
            <notes/>
            <measurements type="height" unit="m">34</measurements>
            <measurements type="length" unit="m">130</measurements>
        </measurementsSet>
        <sourceSet>
            <display>Core 4 Sample Database (VCat)</display>
            <source>
                <name>Core 4 Sample Database (VCat)</name>
            </source>
        </sourceSet>
        <stylePeriodSet>
            <display>Gothic (Medieval); Romanesque</display>
            <stylePeriod vocab="AAT" refid="300020775">Gothic (Medieval)</stylePeriod>
            <stylePeriod vocab="AAT" refid="300020768">Romanesque</stylePeriod>
        </stylePeriodSet>
        <subjectSet>
            <display>architectural exteriors; architectural interiors; New Testament; Old Testament and Apocrypha; rulers and leaders; saints; Mary, Blessed Virgin, Saint; religion and mythology; worship</display>
            <notes/>
            <subject>
                <term type="iconographicTopic" vocab="LCSAF" refid="n 81018544">Mary, Blessed Virgin, Saint</term>
            </subject>
        </subjectSet>
        <techniqueSet>
            <display>construction (assembling)</display>
            <notes/>
            <technique vocab="AAT" refid="300054608">construction (assembling)</technique>
        </techniqueSet>
        <titleSet>
            <display>Chartres Cathedral</display>
            <title type="cited" pref="true" xml:lang="en">Chartres Cathedral</title>
            <title type="cited" pref="false" xml:lang="fr">Notre-Dame de Chartres </title>
            <title type="cited" pref="false" xml:lang="fr">Cathédrale Notre-Dame de Chartres</title>
        </titleSet>
        <worktypeSet>
            <display>buildings; basilicas; buildings; religious buildings; churches; cathedrals</display>
            <worktype vocab="AAT" refid="300007501">cathedral</worktype>
            <worktype vocab="AAT" refid="300170443">basilica</worktype>
        </worktypeSet>
    </work>
    <image id="i_100"
        href="http://core.vraweb.org/examples/html/example001_full.html"
        refid="100" source="VRA Core Oversight Committee, Core 4 Sample Records">
        <agentSet>
            <display>Wiedenhoeft, Ronald</display>
            <notes/>
            <agent/>
        </agentSet>
        <dateSet>
            <display>06/15/90 (creation)</display>
            <notes/>
            <date/>
        </dateSet>
        <measurementsSet>
            <display>18 MB</display>
            <notes/>
            <measurements/>
        </measurementsSet>
        <relationSet>
            <relation type="imageOf" refid="1" source="Core 4 Sample Database (VCat)"/>
        </relationSet>
        <rightsSet>
            <display>© Dr. Ronald V. Wiedenhoeft</display>
            <rights/>
        </rightsSet>
        <sourceSet>
            <display>Saskia Ltd. Cultural Documentation Kfa-0165</display>
            <source>
                <name type="vendor">Saskia Ltd. Cultural Documentation</name>
                <refid type="vendor">Kfa-0165</refid>
            </source>
        </sourceSet>
        <subjectSet>
            <display>bell towers; spires; rose window</display>
            <notes/>
            <subject>
                <term/>
            </subject>
        </subjectSet>
        <techniqueSet>
            <display>digital imaging</display>
            <notes/>
            <technique/>
        </techniqueSet>
        <titleSet>
            <display>Total view of West facade</display>
            <title type="generalView">Total view of West facade</title>
        </titleSet>
        <worktypeSet>
            <display>digital image</display>
            <notes/>
            <worktype/>
        </worktypeSet>
    </image>
</vra>    
    
    */
    
    //Referencias
    //http://core.vraweb.org/pdfs/VRACore4SEI2009deVerges.pdf
    //https://www.loc.gov/standards/vracore/VRA_Core4_Element_Description.pdf
    @Override
    public Element transform(JSONObject source) {
        Document doc = builder.newDocument();
        
        Element vra = doc.createElement("vra");
        vra.setAttribute("xmlns", "http://www.vraweb.org/vracore4.htm");
        vra.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
        vra.setAttribute("xsi:schemaLocation", "http://www.vraweb.org/vracore4.htm http://www.loc.gov/standards/vracore/vra-strict.xsd");
        
        Element  record=doc.createElement("work");
        record.setAttribute("id", "");//"i_100"
        record.setAttribute("refid", "");//refid="100"
        record.setAttribute("href", ""); //href="http://core.vraweb.org/examples/html/example001_full.html"
        record.setAttribute("source", ""); //source="History of Art Visual Resources Collection, UCB"
        vra.appendChild(record);
                  
        if(source.has("recordtitle")){
     /* <titleSet>
            <display>Total view of West facade</display>
            <title type="generalView">Total view of West facade</title>
        </titleSet>*/
            Element titleSet = doc.createElement("titleSet");
            record.appendChild(titleSet);

            JSONArray recordtitle = source.getJSONArray("recordtitle");
            if (recordtitle != null && recordtitle.length() > 0) {
                String value = "";
                for (Object obj : recordtitle) {
                    JSONObject tobj = (JSONObject) obj;
                    if (tobj.has("value")) {

                        value = tobj.getString("value");
                        Element display = doc.createElement("display");
                        display.appendChild(doc.createTextNode(value));
                        titleSet.appendChild(display);

                        Element title = doc.createElement("title");
                        title.setAttribute("type", "generalView");
                        title.appendChild(doc.createTextNode(value));
                        titleSet.appendChild(title);            

                    }
                }
            }
        }
        //<agentSet/>
        //<culturalContextSet/>
        /*
        <dateSet>
            <display>created 1520-1525</display>
            <date type="creation" source="Grove Dictionary of Art Online" 
                href="http://www.groveart.com" dataDate="2005-06-08">
                <earliestDate>1520</earliestDate>
                <latestDate>1525</latestDate>
            </date>
        </dateSet>         
        */
        if(source.has("datecreated")){
            Element dateSet = doc.createElement("dateSet");
            record.appendChild(dateSet);
            if(source.getJSONObject("datecreated").has("value")){
                String value = source.getJSONObject("datecreated").getString("value");
                Element display = doc.createElement("display");
                display.appendChild(doc.createTextNode(value));
                dateSet.appendChild(display);


                Element date = doc.createElement("date");
                record.setAttribute("type", "creation");
                Element earliestDate = doc.createElement("earliestDate");
                earliestDate.appendChild(doc.createTextNode(value));
                date.appendChild(earliestDate);
                Element latestDate = doc.createElement("latestDate");
                latestDate.appendChild(doc.createTextNode(value));
                date.appendChild(latestDate);
                dateSet.appendChild(date);   
            }
        }
        /*
        <descriptionSet>
            <display>
                This drawing was originally part of a sketchbook, now lost, documenting the artist's 2nd
                trip to Egypt in 1867. Some of the figure's costume elements appear in a painted work of a later
                date.
            </display>
            <description source=" Hardin, Jennifer, The Lure of Egypt, St. Petersburg: Museum of Fine Arts,
            1995">
                This drawing was originally part of a sketchbook, now lost, documenting the artist's 2nd trip to
                Egypt in 1867. Some of the figure's costume elements appear in a painted work of a later
                date
            </description>
        </descriptionSet>                 
        */
        if(source.has("description")){
            Element descriptionSet = doc.createElement("descriptionSet");
            record.appendChild(descriptionSet);
            
            JSONArray jdescriptiona = source.getJSONArray("description");
            if (jdescriptiona != null && jdescriptiona.length() > 0) {
                StringBuilder sdescription = new StringBuilder();
                for (Object obj : jdescriptiona) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        sdescription.append(value);
                    }
                }
                
                String value = sdescription.toString();
                Element display = doc.createElement("display");
                display.appendChild(doc.createTextNode(value));
                descriptionSet.appendChild(display);

                Element description = doc.createElement("description");
                //record.setAttribute("source", "");
                description.appendChild(doc.createTextNode(value));
                descriptionSet.appendChild(description); ;
            }
        }

        //<inscriptionSet/>
        /*
        <locationSet>
            <display> Musée du Louvre (Paris, FR) Inv. MR 299; discovered Milos (GR)</display>
            <location type="repository">
                <name type="corporate" xml:lang="fr">Musée du Louvre</name>
                <refid type="accession">Inv. MR 299</refid>
                <name type="geographic" vocab="TGN" refid="7008038" extent="inhabited place">Paris</name>
                <name type="geographic" vocab="TGN" refid="1000070" extent="nation">France</name>
            </location>
            <location type="discovery">
                <name type="geographic" vocab="TGN" refid="7010922">Milos, Nisos</name>
                <name type="geographic" vocab="TGN" refid="1000074">Greece</name>
            </location>
        </locationSet>        
        
        */
        if(source.has("lugar")){
            Element locationSet = doc.createElement("locationSet");
            record.appendChild(locationSet);
            String value = source.getString("lugar");
            if (value != null && !value.isEmpty()) {
                Element lugar = doc.createElement("display");
                lugar.appendChild(doc.createTextNode(value));
                locationSet.appendChild(lugar);
            }
        }
        
        /*
        <materialSet>
            <display>oil paint on canvas</display>
            <material type="medium" vocab="AAT" refid="300015050">oil paint</material>
            <material type="support" vocab="AAT" refid="300014078">canvas</material>
        </materialSet>        
        */
        if(source.has("techmaterial")){
            Element materialSet = doc.createElement("materialSet");
            record.appendChild(materialSet);
            String value = source.getString("techmaterial");
            if (value != null && !value.isEmpty()) {
                Element techmaterial = doc.createElement("dc:type");
                techmaterial.appendChild(doc.createTextNode(value));
                materialSet.appendChild(techmaterial);
            }
        }
        /*
        <measurementsSet>
            <display>Base 3 cm (H) x 36 cm (W) x 24 cm (D)</display>
            <measurements type="height" unit="cm" extent="base">3</measurements>
            <measurements type="width" unit="cm" extent="base">36</measurements>
            <measurements type="depth" unit="cm" extent="base">24</measurements>
        </measurementsSet>        
        */
        if(source.has("dimension")){
            Element measurementsSet = doc.createElement("measurementsSet");
            record.appendChild(measurementsSet);
            String value = source.getString("dimension");
            if (value != null && !value.isEmpty()) {
                Element dimension = doc.createElement("display");
                dimension.appendChild(doc.createTextNode(value));
                measurementsSet.appendChild(dimension);
            }
        }
        //<relationSet/> 
        /*
        <rightsSet>
            <display>© Faith Ringgold. All rights reserved.</display>
            <notes>Contact information: PO Box 429, Englewood, NJ 07631 858-576-0397</notes>
            <rights type="copyrighted">
                <rightsHolder>Faith Ringgold</rightsHolder>
                <text>© Faith Ringgold. All Rights reserved.</text>
            </rights>
        </rightsSet> 
        */
        if(source.has("rights")){
            Element rightsSet = doc.createElement("rightsSet");
            
            JSONObject jrights =source.getJSONObject("rights");
            if(jrights!=null && jrights.has("rightstitle")){
                String rightstitle="";
                String description="";
                String url="";
                if(jrights.has("rightstitle")){
                    rightstitle=jrights.getString("rightstitle");
                    Element display = doc.createElement("display");                        
                    display.appendChild(doc.createTextNode(rightstitle));
                    rightsSet.appendChild(display);
                }else if(jrights.has("description")){
                    description=jrights.getString("description");
                    Element notes = doc.createElement("notes");                        
                    notes.appendChild(doc.createTextNode(description));
                    rightsSet.appendChild(notes);
                }
                if(jrights.has("url")){
                    url=jrights.getString("url");
                }
                
                Element rights = doc.createElement("rights");
                rights.setAttribute("type", "copyrighted");
                rights.setAttribute("url", url);
                Element text = doc.createElement("text");     
                text.appendChild(doc.createTextNode(rightstitle));
                rights.appendChild(text);
                rightsSet.appendChild(rights);
            } 
            if(rightsSet.hasChildNodes()){
                record.appendChild(rightsSet);
            }
        }
        
        
        /*
        <sourceSet>
            <display>Gascoigne, Bamber, The Great Moghuls, New York: Harper & Row, 1971</display>
            <source>
                <name type="book">Gascoigne, Bamber, The Great Moghuls, New York: Harper & Row,
                1971</name>
                <refid type="ISBN">060114673</refid>
            </source>
        </sourceSet> 
        */
        //<stateEditionSet/> 
        //<stylePeriodSet/> 
        /*
        <subjectSet>
            <display>Chicago; Chaplin, Charlie,1889-1977; actors</display>
            <subject>
                <term type="geographicPlace" vocab="TGN" refid="7013596">Chicago</term>
                <term type="personalName" vocab="LCNAF" refid="n79126907">Chaplin, Charlie,1889-
                1977</term>
                <term type="descriptiveTopic" vocab="AAT" refid="300025658">actors</term>
            </subject>
        </subjectSet>         
        */
        
        if(source.has("keywords")){
            Element subjectSet = doc.createElement("subjectSet");
            JSONArray jsubject = source.getJSONArray("keywords");
            StringBuilder subjectSetSB=new StringBuilder();
            if (jsubject != null && jsubject.length() > 0) {
                for (Object obj : jsubject) {
                    String value = (String) obj;
                    if (value != null && !value.isEmpty()) {
                        if(value.contains(";")){
                            String[] values=value.split(";");
                            for (String avalue : values) {
                                Element subject = doc.createElement("subject");
                                subjectSet.appendChild(subject);
                                Element term = doc.createElement("term");
                                subject.appendChild(term);
                                term.setAttribute("type","otherTopic");
                                term.appendChild(doc.createTextNode(avalue.trim())); 
                                
                                subjectSetSB.append(avalue.trim());
                                subjectSetSB.append(',');
                            }  
                            subjectSetSB.append(value);
                        }else{
                            Element subject = doc.createElement("subject");
                            subjectSet.appendChild(subject);
                            Element term = doc.createElement("term");
                            subject.appendChild(term);
                            term.setAttribute("type","otherTopic");
                            term.appendChild(doc.createTextNode(value.trim())); 

                            subjectSetSB.append(value);
                            subjectSetSB.append(',');
                        }                        
                    }                                           
                }                                               
            }
            if(subjectSetSB.length()>0){
                Element display = doc.createElement("display");
                display.appendChild(doc.createTextNode(subjectSetSB.toString()));
                subjectSet.appendChild(display);
                
                record.appendChild(subjectSet);
            }            
        }
        
        
        //<techniqueSet/> 
        /*
        <textrefSet>
            <display>ARV2 5 (6)</display>
            <textref>
                <name type="corpus">Beazley, J.D., Attic Red-figure Vase Painters (2nd edition), New York:
               Hacker Art Books, 1984</name>
                <refid type="citation" >p. 5, no. 6</refid>
            </textref>
            <textref type="electronic">
                <name>The Beazley Archive</name>
                <refid type="other" href="http://www.beazley.ox.ac.uk" dataDate="2005-06-08">Vase number
               200020</refid>
            </textref>
        </textrefSet> 
        */
        //<titleSet/> 
        //<worktypeSet/> 
//System.out.println(oaidc);            
        return vra;        
    }
}
