package shaper.mapping.rml;

import org.apache.jena.rdf.model.*;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class RMLParser {

    public enum Lang {
//        RDF_XML("RDF/XML"), N_TRIPLE("N-TRIPLE"), N3("N3"),
        TURTLE("TURTLE"), TTL("TTL");

        private String lang;

        Lang(String lang) { this.lang = lang; }

        @Override
        public String toString() { return lang; }
    }

    private Optional<URI> base;
    private Model model;

    public RMLParser(String pathname, Lang lang) {
        File rmlFile = new File(pathname);
        base = getBase(rmlFile);
        model = getModel(rmlFile, base, lang.toString());
    }

    public String getSQLQuery(String logicalTable) {
        Resource s = createResource(logicalTable);
        Property p = createRRProperty("sqlQuery");

        Set<String> set = getLiteralObjectsOf(s, p);

        return set.size() > 0 ? set.toArray(new String[0])[0].trim() : null;
    }

    public String getTableName(String logicalTable) {
        Resource s = createResource(logicalTable);
        Property p = createRRProperty("tableName");

        Set<String> set = getLiteralObjectsOf(s, p);

        return set.size() > 0 ? set.toArray(new String[0])[0] : null;
    }

    public Set<URI> getClasses(String subjectMap) {
        Resource s = createResource(subjectMap);
        Property p = createRRProperty("class");

        return getIRIObjectsOf(s, p);
    }

    public Set<URI> getSQLVersions(String logicalTable) {
        Resource s = createResource(logicalTable);
        Property p = createRRProperty("sqlVersion");

        return getIRIObjectsOf(s, p);
    }

    private Set<String> getLiteralObjectsOf(Resource s, Property p) {
        Set<String> set = new TreeSet<>();

        NodeIterator iterator = model.listObjectsOfProperty(s, p);
        while (iterator.hasNext()) {
            RDFNode o = iterator.next();
            if (o.isLiteral()) set.add(o.asLiteral().toString().replace("\\\"", "\""));
        }

        return set;
    }

    private Set<URI> getIRIObjectsOf(Resource s, Property p) {
        Set<URI> set = new TreeSet<>();

        NodeIterator iterator = model.listObjectsOfProperty(s, p);
        while (iterator.hasNext()) {
            RDFNode o = iterator.next();
            if (o.isURIResource()) set.add(URI.create(o.asResource().getURI()));
        }

        return set;
    }

    public Set<String> getTriplesMaps() {
        // a rr:TriplesMap
        Property p = model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        Resource o = model.createResource("http://www.w3.org/ns/r2rml#TriplesMap");

        ResIterator subjects = model.listSubjectsWithProperty(p, o);
        return subjects.mapWith(resource -> resource.toString()).toSet();
    }

    public static boolean isURI(String str) {
        try { return new org.apache.jena.ext.xerces.util.URI(str) != null; }
        catch (org.apache.jena.ext.xerces.util.URI.MalformedURIException e) { return false; }
    }

    private Resource createResource(String str) {
        return isURI(str) ? model.createResource(str) : model.createResource(AnonId.create(str));
    }

    private Property createRRProperty(String localName) {
        return model.createProperty(model.getNsPrefixURI("rr"), localName);
    }

    private Property createRMLProperty(String localName) {
        return model.createProperty(model.getNsPrefixURI("rml"), localName);
    }

    public String getInverseExpression(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("inverseExpression");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public URI getDatatype(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("datatype");

        Set<URI> objects = getIRIObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new URI[0])[0] : null;
    }

    public String getLanguage(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("language");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public Set<String> getLiteralObjects(String predicateObjectMap) {
        Resource s = createResource(predicateObjectMap);
        Property p = createRRProperty("object");

        return getLiteralObjectsOf(s, p);
    }

    public Set<URI> getIRIObjects(String predicateObjectMap) {
        Resource s = createResource(predicateObjectMap);
        Property p = createRRProperty("object");

        return getIRIObjectsOf(s, p);
    }

    public Set<URI> getGraphs(String subjectMapOrPredicateObjectMap) {
        Resource s = createResource(subjectMapOrPredicateObjectMap);
        Property p = createRRProperty("graph");

        return getIRIObjectsOf(s, p);
    }

    public Set<URI> getPredicates(String predicateObjectMap) {
        Resource s = createResource(predicateObjectMap);
        Property p = createRRProperty("predicate");

        return getIRIObjectsOf(s, p);
    }

    public URI getTermType(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("termType");

        Set<URI> objects = getIRIObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new URI[0])[0] : null;
    }

    public String getTemplate(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("template");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public String getColumn(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("column");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    // when the termMap is a subject map, object map, predicate map, or graph map
    public URI getIRIConstant(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("constant");

        Set<URI> objects = getIRIObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new URI[0])[0] : null;
    }

    // when the termMap is an object map
    public String getLiteralConstant(String termMap) {
        Resource s = createResource(termMap);
        Property p = createRRProperty("constant");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public String getParentTriplesMap(String refObjectMap) {
        Resource s = createResource(refObjectMap);
        Property p = createRRProperty("parentTriplesMap");

        Set<String> objects = getResourceObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public Set<String> getObjectMaps(String predicateObjectMap) {
        Resource s = createResource(predicateObjectMap);
        Property p = createRRProperty("objectMap");

        return getResourceObjectsOf(s, p);
    }

    public Set<String> getGraphMaps(String subjectMapOrPredicateObjectMap) {
        Resource s = createResource(subjectMapOrPredicateObjectMap);
        Property p = createRRProperty("graphMap");

        return getResourceObjectsOf(s, p);
    }

    public Set<String> getPredicateMaps(String predicateObjectMap) {
        Resource s = createResource(predicateObjectMap);
        Property p = createRRProperty("predicateMap");

        return getResourceObjectsOf(s, p);
    }

    public String getChild(String joinCondition) {
        Resource s = createResource(joinCondition);
        Property p = createRRProperty("child");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.toArray(new String[0])[0];
    }

    public String getParent(String joinCondition) {
        Resource s = createResource(joinCondition);
        Property p = createRRProperty("parent");

        Set<String> objects = getLiteralObjectsOf(s, p);

        return objects.toArray(new String[0])[0];
    }

    public Set<String> getJoinConditions(String refObjectMap) {
        Resource s = createResource(refObjectMap);
        Property p = createRRProperty("joinCondition");

        return getResourceObjectsOf(s, p);
    }

    public Set<String> getPredicateObjectMaps(String triplesMap) {
        Resource s = createResource(triplesMap);
        Property p = createRRProperty("predicateObjectMap");

        return getResourceObjectsOf(s, p);
    }

    public String getSubjectMap(String triplesMap) {
        Resource s = createResource(triplesMap);
        Property p = createRRProperty("subjectMap");

        Set<String> objects = getResourceObjectsOf(s, p);

        return objects.toArray(new String[0])[0];
    }

    public String getLogicalTable(String triplesMap) {
        Resource s = createResource(triplesMap);
        Property p = createRRProperty("logicalTable");

        Set<String> objects = getResourceObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public String getLogicalSource(String triplesMap) {
        Resource s = createResource(triplesMap);
        Property p = createRMLProperty("logicalSource");

        Set<String> objects = getResourceObjectsOf(s, p);

        return objects.size() > 0 ? objects.toArray(new String[0])[0] : null;
    }

    public String getSource(String logicalSource) {
        Resource s = createResource(logicalSource);
        Property p = createRMLProperty("source");

        Set<String> set = getObjectsOf(s, p);

        return set.size() > 0 ? set.toArray(new String[0])[0] : null;
    }

    public Set<URI> getLogicalTables() {
        Set<URI> set = new TreeSet<>();

        Property p; // rr:tableName, rr:sqlQuery, rr:sqlVersion

        p = createRRProperty("tableName");
        set.addAll(getIRISubjectsOf(p));

        p = createRRProperty("sqlQuery");
        set.addAll(getIRISubjectsOf(p));

        p = createRRProperty("sqlVersion");
        set.addAll(getIRISubjectsOf(p));

        return set;
    }

    private Set<String> getResourceObjectsOf(Resource s, Property p) {
        Set<String> set = new TreeSet<>();

        NodeIterator iterator = model.listObjectsOfProperty(s, p);

        while (iterator.hasNext()) {
            RDFNode node = iterator.next();

            if (node.isLiteral()) continue;

            if (node.isURIResource())
                set.add(node.asResource().getURI());
            else
                set.add(node.asResource().getId().getBlankNodeId().getLabelString());
        }

        return set;
    }

    private Set<String> getObjectsOf(Resource s, Property p) {
        Set<String> set = new TreeSet<>();

        NodeIterator iterator = model.listObjectsOfProperty(s, p);

        while (iterator.hasNext()) {
            RDFNode node = iterator.next();

            if (node.isLiteral()) set.add(node.asLiteral().toString().replace("\\\"", "\""));
            else if (node.isURIResource()) set.add(node.asResource().getURI());
            else set.add(node.asResource().getId().getBlankNodeId().getLabelString());
        }

        return set;
    }

    private Set<String> getResourceSubjectsOf(Property p) {
        Set<String> set = new TreeSet<>();

        ResIterator iterator = model.listSubjectsWithProperty(p);

        while (iterator.hasNext()) {
            Resource resource = iterator.next();
            String uri = resource.getURI();

            if (uri != null)
                set.add(uri);
            else
                set.add(resource.getId().getBlankNodeId().getLabelString());
        }

        return set;
    }

    private Set<URI> getIRISubjectsOf(Property p) {
        Set<URI> set = new TreeSet<>();

        ResIterator iterator = model.listSubjectsWithProperty(p);

        while (iterator.hasNext()) {
            Resource resource = iterator.next();
            String uri = resource.getURI();

            if (uri != null) set.add(URI.create(uri));
        }

        return set;
    }

    public Map<String, String> getPrefixes() {
        return model.getNsPrefixMap();
    }

    private Model getModel(File rmlFile, Optional<URI> base, String lang) {
        Model model = ModelFactory.createDefaultModel();

        try(InputStream inputStream = new FileInputStream(rmlFile)) {
            if (base.isPresent())
                model.read(inputStream, base.get().toString(), lang);
            else
                model.read(inputStream, null, lang);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return model;
    }

    private Optional<URI> getBase(File rmlFile) {
        Optional<URI> base = Optional.empty();
        try(LineNumberReader reader = new LineNumberReader(new FileReader(rmlFile))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                line = line.trim();
                if (line.startsWith("@base")
                        || line.regionMatches(true, 0, "BASE", 0, 4)) {
                    base = Optional.of(URI.create(line.substring(line.indexOf("<") + 1, line.lastIndexOf(">"))));
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return base;
    }
}
