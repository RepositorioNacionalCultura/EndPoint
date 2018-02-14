package mx.gob.cultura.endpoint.rdf;

import com.hp.hpl.jena.ontology.*;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

/**
 * Provides methods to manage Ontologies.
 * @author Hasdai Pacheco
 */
public class OntologyManager {
    private String namespace;
    private OntDocumentManager ontManager = null;
    private OntModel ontology = null;

    public enum PropertyFilter {
        ANNOTATION, LITERAL, OBJECT
    };

    /**
     * Constructor. Creates a new instance of {@link OntologyManager}.
     * @param namespace Ontology namespace.
     */
    public OntologyManager(String namespace) {
        this.namespace = namespace;
        ontManager = OntDocumentManager.getInstance();
    }

    /**
     * Gets the {@link OntModel}
     * @return OntModel object.
     */
    public OntModel getOntology() {
        return ontology;
    }

    /**
     * Loads an ontology from an owl file.
     * @param owlFile {@link File} object for the owl file.
     * @param spec {@link OntModelSpec} to use.
     * @throws IOException
     */
    public void loadFromFile(File owlFile, OntModelSpec spec) throws IOException {
        String ontName = namespace + owlFile.getName();

        ontManager.addAltEntry(ontName, owlFile.getCanonicalPath());
        if (null == ontology) {
            ontology = ontManager.getOntology(ontName, spec);
        } else {
            ontology.addSubModel(ontManager.getOntology(ontName, spec), false);
        }

        ontology.rebind();
    }

    /**
     * Loads an ontology from a URL.
     * @param url {@link URL} object for the owl resource.
     * @param spec {@link OntModelSpec} to use.
     * @throws IOException
     */
    public void loadFromURL(URL url, OntModelSpec spec) throws IOException, URISyntaxException {
        File res = new File(url.toURI());
        loadFromFile(res, spec);
    }

    public OntClass getClass(String uri) {
        return ontology.getOntClass(uri);
    }

    public ArrayList<Statement> filterProperties(OntClass cls, PropertyFilter filter) {
        ArrayList<Statement> ret = new ArrayList<>();

        StmtIterator it = cls.listProperties();
        while (it.hasNext()) {
            Statement st = it.next();
            RDFNode pred = st.getPredicate();
            if (pred.isResource()) {
                OntResource rpred = cls.getOntModel().getOntResource(pred.asResource());
                switch (filter) {
                    case ANNOTATION: {
                        if (rpred.isAnnotationProperty()) ret.add(st);
                        break;
                    }
                    case OBJECT: {
                        if (rpred.isResource()) ret.add(st);
                        break;
                    }
                    case LITERAL: {
                        if (rpred.isLiteral()) ret.add(st);
                        break;
                    }
                }
            }
        }

        return ret;
    }

    private ArrayList<String> RDFNodesToStringLiteralArray(ExtendedIterator<RDFNode> nodes) {
        ArrayList<String> ret = new ArrayList<>();
        if (null != nodes && nodes.hasNext()) {
            while (nodes.hasNext()) {
                RDFNode node = nodes.next();
                if (null != node) {
                    ret.add(node.asLiteral().getString());
                }
            }
        }
        return ret;
    }
}
