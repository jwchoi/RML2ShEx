package shaper.mapping.model.shacl;

import shaper.Shaper;
import shaper.mapping.PrefixMap;
import shaper.mapping.Symbols;
import shaper.mapping.model.Utils;
import shaper.mapping.model.dm.DMModel;
import shaper.mapping.model.dm.LiteralProperty;
import shaper.mapping.model.dm.ReferenceProperty;
import shaper.mapping.model.dm.TableIRI;
import shaper.mapping.model.r2rml.*;

import java.net.URI;
import java.nio.file.Path;
import java.util.*;

public class ShaclDocModelFactory {
    private static ShaclDocModel shaclDocModel;
    // R2RML
    public static ShaclDocModel getSHACLDocModel(R2RMLModel r2rmlModel) {
        shaclDocModel = new ShaclDocModel(URI.create(Shaper.shapeBaseURI), Shaper.prefixForShapeBaseURI);

        addPrefixes(r2rmlModel);

        Set<TriplesMap> triplesMaps = r2rmlModel.getTriplesMaps();

        // add PropertyShapes to NodeShape
        // add NodeShapes to ShaclDocModel
        for (TriplesMap triplesMap : triplesMaps) {
            SubjectMap subjectMap = triplesMap.getSubjectMap();

            //create a node shape
            NodeShape nodeShape = new NodeShape(createNodeShapeID(triplesMap), triplesMap.getUri(), subjectMap, shaclDocModel);

            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                    // create property shape from a predicate-object pair
                    String predicate = predicateMap.getConstant().get();
                    int multiplicity = getMultiplicity(triplesMap, URI.create(predicate));
                    boolean hasQualifiedValueShape = multiplicity > 1 ? true : false;
                    URI propertyShapeID = createPropertyShapeID(nodeShape, predicate, hasQualifiedValueShape);

                    PropertyShape propertyShape;

                    if (predicateObjectPair.getRefObjectMap().isPresent()) {
                        // when referencing object map
                        RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();
                        propertyShape = new PropertyShape(propertyShapeID, predicateMap, refObjectMap, hasQualifiedValueShape, shaclDocModel);
                    } else {
                        // when object map
                        ObjectMap objectMap = predicateObjectPair.getObjectMap().get();
                        propertyShape = new PropertyShape(propertyShapeID, predicateMap, objectMap, hasQualifiedValueShape, shaclDocModel);
                    }
                    shaclDocModel.addShape(propertyShape);

                    // for reference from node shape to property shape
                    nodeShape.addPropertyShape(propertyShape.getID());
                }
            }

            shaclDocModel.addShape(nodeShape);
        }

        Set<NodeShape> derivedNodeShapes = new TreeSet<>();
        Set<URI> checkedTriplesMaps = new HashSet<>();
        for (TriplesMap triplesMap : triplesMaps) {
            URI uriOfTriplesMap = triplesMap.getUri();
            if (!checkedTriplesMaps.contains(uriOfTriplesMap)) {
                // find the mapped node shape
                NodeShape mappedNodeShape = shaclDocModel.getMappedNodeShape(uriOfTriplesMap);
                ////////////////////////////////////////////////////////////////////////////////////////////////////////////
                Set<NodeShape> baseNodeShapes = shaclDocModel.getNodeShapesOfSameSubject(mappedNodeShape);
                for (NodeShape baseNodeShape : baseNodeShapes)
                    checkedTriplesMaps.add(baseNodeShape.getMappedTriplesMap().get());

                Set<Set<NodeShape>> setsForDerivedNodeShapes = shaclDocModel.getSubsetOfPowerSetOf(baseNodeShapes);
                for (Set<NodeShape> set: setsForDerivedNodeShapes) {

                    Set<URI> nodeShapeIRIs = new TreeSet<>();
                    for (NodeShape nodeShape: set)
                        nodeShapeIRIs.add(nodeShape.getID());

                    NodeShape derivedShape = new NodeShape(createNodeShapeID(set), nodeShapeIRIs, shaclDocModel);

                    derivedNodeShapes.add(derivedShape);
                }
            }
        }
        for (NodeShape derivedNodeShape: derivedNodeShapes)
            shaclDocModel.addShape(derivedNodeShape);

        return shaclDocModel;
    }

    private static String obtainFragmentOrLastPathSegmentOf(URI uri) {
        String postfix = uri.getFragment();
        if (postfix == null) {
            Path path = Path.of(uri.getPath());
            postfix = path.getName(path.getNameCount()-1).toString();
        }

        return postfix;
    }

    private static URI createNodeShapeID(Set<NodeShape> nodeShapes) {
        StringBuffer postfix = new StringBuffer();
        for (NodeShape nodeShape: nodeShapes) {
            if (nodeShape.getMappedTriplesMap().isPresent()) {
                postfix.append(obtainFragmentOrLastPathSegmentOf(nodeShape.getMappedTriplesMap().get()));
                postfix.append(Symbols.UNDERSCORE);
            }
        }
        postfix.append("Shape");

        return URI.create(shaclDocModel.getBaseIRI() + Symbols.HASH + postfix);
    }

    private static URI createNodeShapeID(TriplesMap triplesMap) {
        String postfix = obtainFragmentOrLastPathSegmentOf(triplesMap.getUri());
        return URI.create(shaclDocModel.getBaseIRI() + Symbols.HASH + postfix + "Shape");
    }

    private static int getMultiplicity(TriplesMap triplesMap, URI predicate) {
        int multiplicity = 0;

        List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
        for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {

            List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
            for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {

                PredicateMap predicateMap = predicateObjectPair.getPredicateMap();
                if (predicateMap.getConstant().isPresent() && predicateMap.getConstant().get().equals(predicate.toString()))
                    multiplicity++;
            }
        }

        return multiplicity;
    }

    private static URI createPropertyShapeID(NodeShape nodeShape, String predicateURIString, boolean hasQualifiedValueShape) {
        URI predicateURI = URI.create(predicateURIString);
        String prefixOfPredicateURI = shaclDocModel.getPrefixOf(predicateURI);

        String postfix = obtainFragmentOrLastPathSegmentOf(predicateURI);

        if (prefixOfPredicateURI != null)
            postfix = prefixOfPredicateURI + Symbols.DASH + postfix;

        URI propertyShapeID = URI.create(nodeShape.getID() + Symbols.DASH + postfix);

        if (hasQualifiedValueShape) {
            URI tempPropertyShapeID;
            Set<URI> propertyShapeIDs = nodeShape.getPropertyShapeIDs();
            int index = 0;
            do {
                index++;
                tempPropertyShapeID = URI.create(propertyShapeID + Symbols.DASH + "q" + index);
            } while (propertyShapeIDs.contains(tempPropertyShapeID));
            propertyShapeID = tempPropertyShapeID;
        }

        return propertyShapeID;
    }

    private static void addPrefixes(R2RMLModel r2rmlModel) {
        // PREFIX: from RDF graph generated by the R2RML document.
        Set<Map.Entry<String, String>> entrySet = r2rmlModel.getPrefixMap().entrySet();
        for (Map.Entry<String, String> entry: entrySet)
            shaclDocModel.addPrefixDecl(entry.getKey(), entry.getValue());

        shaclDocModel.addPrefixDecl("rdf", PrefixMap.getURI("rdf").toString());
    }

    //Direct Mapping
    public static ShaclDocModel getSHACLDocModel(DMModel dmModel) {
        shaclDocModel = new ShaclDocModel(URI.create(Shaper.shapeBaseURI), Shaper.prefixForShapeBaseURI);

        addPrefixes(dmModel);

        Set<TableIRI> tableIRIs = dmModel.getTableIRIs();

        for(TableIRI tableIRI : tableIRIs) {
            //-> node shape
            URI nodeShapeID = createNodeShapeID(tableIRI);
            NodeShape nodeShape = new NodeShape(nodeShapeID, tableIRI, shaclDocModel);
            shaclDocModel.addShape(nodeShape);
            //<- node shape

            //-> property shape
            //-> BEGIN Literal Property
            List<LiteralProperty> literalProperties = Arrays.asList(dmModel.getLiteralProperties(tableIRI).toArray(new LiteralProperty[0]));
            for(int i = 0; i < literalProperties.size(); i++) {
                URI propertyShapeID = URI.create(nodeShapeID + Symbols.DASH + "col" + (i + 1));
                PropertyShape propertyShape = new PropertyShape(propertyShapeID, literalProperties.get(i), shaclDocModel);

                shaclDocModel.addShape(propertyShape);
                nodeShape.addPropertyShape(propertyShapeID);
            }
            //<- END Literal Property

            //-> BEGIN Reference Property
            List<ReferenceProperty> referenceProperties = Arrays.asList(dmModel.getReferenceProperties(tableIRI, false).toArray(new ReferenceProperty[0]));
            for(int i = 0; i < referenceProperties.size(); i++) {
                URI propertyShapeID = URI.create(nodeShapeID + Symbols.DASH + "ref" + (i + 1));
                PropertyShape propertyShape = new PropertyShape(propertyShapeID, referenceProperties.get(i), false, shaclDocModel);

                shaclDocModel.addShape(propertyShape);
                nodeShape.addPropertyShape(propertyShapeID);
            }
            //<- END Reference Property

            // Begin Inverse Referential Constraint
            List<ReferenceProperty> inverseReferenceProperties = Arrays.asList(dmModel.getReferenceProperties(tableIRI, true).toArray(new ReferenceProperty[0]));
            for(int i = 0; i < inverseReferenceProperties.size(); i++) {
                URI propertyShapeID = URI.create(nodeShapeID + Symbols.DASH + "inverse" + (i + 1));
                PropertyShape propertyShape = new PropertyShape(propertyShapeID, inverseReferenceProperties.get(i), true, shaclDocModel);

                shaclDocModel.addShape(propertyShape);
                nodeShape.addPropertyShape(propertyShapeID);
            } // END Inverse Referential Constraint
            //<- property shape
        } // END TABLE

        return shaclDocModel;
    }

    private static void addPrefixes(DMModel dmModel) {
        shaclDocModel.addPrefixDecl(dmModel.getPrefix(), dmModel.getBaseIRI().toString());
        shaclDocModel.addPrefixDecl("rdf", PrefixMap.getURI("rdf").toString());
        shaclDocModel.addPrefixDecl("xsd", PrefixMap.getURI("xsd").toString());
    }

    private static URI createNodeShapeID(TableIRI tableIRI) {
        URI namespaceIRI = shaclDocModel.getNamespaceIRI(shaclDocModel.getPrefix()).get();
        return URI.create(namespaceIRI + Utils.encode(tableIRI.getMappedTableName()) + "Shape");
    }
}
