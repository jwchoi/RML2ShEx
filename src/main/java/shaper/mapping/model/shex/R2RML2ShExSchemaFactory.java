package shaper.mapping.model.shex;

import shaper.Shaper;
import shaper.mapping.model.ID;
import shaper.mapping.model.r2rml.*;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

class R2RML2ShExSchemaFactory {
    // R2RML
    static ShExSchema getShExSchemaModel(R2RMLModel r2rmlModel) {
        ShExSchema shExSchema = new ShExSchema(URI.create(Shaper.shapeBaseURI), Shaper.prefixForShapeBaseURI);

        Set<TriplesMap> triplesMaps = r2rmlModel.getTriplesMaps();

        for (TriplesMap triplesMap : triplesMaps) {

            SubjectMap subjectMap = triplesMap.getSubjectMap();

            // create a shape constraint
            URI uriOfTriplesMap = triplesMap.getUri();
            ID shapeID = buildShapeID(shExSchema.getPrefix(), shExSchema.getBaseIRI(), uriOfTriplesMap);
            Shape shape = new R2RMLShape(shapeID, uriOfTriplesMap, subjectMap);

            // create Triple Constraint From rr:class
            TripleConstraint tcFromClasses = new R2RMLTripleConstraint(subjectMap.getClassIRIs());
            shape.addTripleConstraint(tcFromClasses);
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            int postfix = 0;

            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {

                    // when referencing object map
                    if (predicateObjectPair.getRefObjectMap().isPresent()) continue;

                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();
                    ObjectMap objectMap = predicateObjectPair.getObjectMap().get();

                    // create Node Constraint From predicate-object map
                    ID nodeConstraintID = new ID(shExSchema.getPrefix(), shExSchema.getBaseIRI(), shape.getID().getLocalPart() + "_Obj" + (++postfix));
                    NodeConstraint nodeConstraint = new R2RMLNodeConstraint(nodeConstraintID, objectMap);
                    shExSchema.addNodeConstraint(nodeConstraint);

                    // create Triple Constraint From predicate-object map
                    TripleConstraint tcFromPOMap = new R2RMLTripleConstraint(predicateMap, objectMap);
                    shape.addTripleConstraint(tcFromPOMap);
                }

            }

            shExSchema.addShape(shape);
        }

        for (TriplesMap triplesMap : triplesMaps) {
            // find the mapped shape
            Shape shape = shExSchema.getMappedShape(triplesMap.getUri());
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    // when object map
                    if (predicateObjectPair.getObjectMap().isPresent()) continue;

                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();
                    RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();

                    // create Triple Constraint From referencing-object map
                    TripleConstraint tcFromPROMap = new R2RMLTripleConstraint(predicateMap, refObjectMap);
                    shape.addTripleConstraint(tcFromPROMap);
                }
            }
        }


        Set<URI> checkedTriplesMaps = new HashSet<>();
        for (TriplesMap triplesMap : triplesMaps) {
            URI uriOfTriplesMap = triplesMap.getUri();
            if (!checkedTriplesMaps.contains(uriOfTriplesMap)) {
                // find the mapped shape
                Shape mappedShape = shExSchema.getMappedShape(uriOfTriplesMap);
                ////////////////////////////////////////////////////////////////////////////////////////////////////////////
                Set<Shape> baseShapes = shExSchema.getShapesToShareTheSameSubjects(mappedShape);
                baseShapes.stream()
                        .map(baseShape -> (R2RMLShape) baseShape)
                        .forEach(baseShape -> checkedTriplesMaps.add(baseShape.getMappedTriplesMap().get()));

                Set<Set<Shape>> setsForDerivedShapes = shExSchema.createSetsForDerivedShapes(baseShapes);
                for (Set<Shape> set: setsForDerivedShapes) {
                    ID shapeID = buildShapeID(shExSchema.getPrefix(), shExSchema.getBaseIRI(), set);
                    Shape derivedShape = new R2RMLShape(shapeID, set);

                    // tripleConstraint
                    Set<URI> classIRIs = new TreeSet<>();
                    for (Shape shape: set) {
                        Set<R2RMLTripleConstraint> tripleConstraints = shape.getTripleConstraints().stream()
                                .filter(tc -> tc instanceof R2RMLTripleConstraint)
                                .map(tc -> (R2RMLTripleConstraint) tc)
                                .collect(Collectors.toSet());
                        for (R2RMLTripleConstraint tc: tripleConstraints) {
                            if (tc.getMappedType().equals(TripleConstraint.MappedTypes.RR_CLASSES))
                                classIRIs.addAll(tc.getClassIRIs());
                            else
                                derivedShape.addTripleConstraint(tc); // except for RR_CLASSES tripleConstraints
                        }
                    }

                    // RR_CLASSES tripleConstraint
                    derivedShape.addTripleConstraint(new R2RMLTripleConstraint(classIRIs));

                    shExSchema.addShape(derivedShape);
                }
            }
        }

        return shExSchema;
    }

    private static ID buildShapeID(String prefixLabel, URI prefixIRI, URI triplesMap) {
        String localPart = triplesMap.getFragment() + "Shape";
        return new ID(prefixLabel, prefixIRI, localPart);
    }

    private static ID buildShapeID(String prefixLabel, URI prefixIRI, Set<Shape> baseShapes) {
        StringBuffer localPart = new StringBuffer();

        baseShapes.stream()
                .map(baseShape -> (R2RMLShape) baseShape)
                .forEach(baseShape -> localPart.append(baseShape.getMappedTriplesMap().get().getFragment()));

        localPart.append("Shape");

        return new ID(prefixLabel, prefixIRI, localPart.toString());
    }
}
