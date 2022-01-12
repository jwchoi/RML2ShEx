package shaper.mapping.model.shex;

import shaper.Shaper;
import shaper.mapping.PrefixMap;
import shaper.mapping.model.ID;
import shaper.mapping.model.rml.*;

import java.net.URI;
import java.util.*;

class RML2ShExSchemaFactory {
    // RML
    static ShExSchema getShExSchemaModel(RMLModel rmlModel) {
        ShExSchema shExSchema = new ShExSchema(URI.create(Shaper.shapeBaseURI), Shaper.prefixForShapeBaseURI);

        addPrefixes(rmlModel, shExSchema);

        List<TriplesMap> triplesMaps = rmlModel.getTriplesMaps().stream().toList();

        for (TriplesMap triplesMap : triplesMaps) {

            SubjectMap subjectMap = triplesMap.getSubjectMap();

            // create a node constraint from subjectMap
            ID sm2ncID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "SM2NC" + NodeConstraint.getIncrementer());
            NodeConstraint sm2nc = new RMLNodeConstraint(sm2ncID, subjectMap);

            Set<TripleConstraint> tripleConstraints = new HashSet<>(); // temporarily

            // create a triple constraint from rr:class of subjectMap
            Set<URI> classes = subjectMap.getClasses();
            if (classes.size() > 0) {
                ID sm2tcID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "SM2TC" + TripleConstraint.getIncrementer());
                TripleConstraint sm2tc = new RMLTripleConstraint(sm2tcID, classes);

                tripleConstraints.add(sm2tc);
            }

            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                    if (predicateObjectPair.getRefObjectMap().isPresent()) {
                        // when referencing object map
                        RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();

                        ID pr2tcID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "PR2TC" + TripleConstraint.getIncrementer());
                        TripleConstraint pr2tc = new RMLTripleConstraint(pr2tcID, predicateMap, refObjectMap);

                        tripleConstraints.add(pr2tc);
                    }

                    if (predicateObjectPair.getObjectMap().isPresent()) {
                        // when object map
                        ObjectMap objectMap = predicateObjectPair.getObjectMap().get();

                        ID po2tcID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "PO2TC" + TripleConstraint.getIncrementer());
                        TripleConstraint po2tc = new RMLTripleConstraint(po2tcID, predicateMap, objectMap);

                        tripleConstraints.add(po2tc);
                    }
                }

            }


        }

//        for (TriplesMap triplesMap : triplesMaps) {
//
//            SubjectMap subjectMap = triplesMap.getSubjectMap();
//
//            // create a shape constraint
//            Shape shape = new Shape(triplesMap.getUri(), subjectMap);
//
//            // create Triple Constraint From rr:class
//            TripleConstraint tcFromClasses = new TripleConstraint(subjectMap.getClassIRIs());
//            shape.addTripleConstraint(tcFromClasses);
//            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//            int postfix = 0;
//
//            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
//            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
//                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
//                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
//
//                    // when referencing object map
//                    if (predicateObjectPair.getRefObjectMap().isPresent()) continue;
//
//                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();
//                    ObjectMap objectMap = predicateObjectPair.getObjectMap().get();
//
//                    // create Node Constraint From predicate-object map
//                    String nodeConstraintID = shape.getShapeID() + "_Obj" + (++postfix);
//                    NodeConstraint nodeConstraint = new NodeConstraint(nodeConstraintID, objectMap);
//                    shExSchema.addNodeConstraint(nodeConstraint);
//
//                    // create Triple Constraint From predicate-object map
//                    TripleConstraint tcFromPOMap = new TripleConstraint(predicateMap, objectMap);
//                    shape.addTripleConstraint(tcFromPOMap);
//                }
//
//            }
//
//            shExSchema.addShape(shape);
//        }
//
//        for (TriplesMap triplesMap : triplesMaps) {
//            // find the mapped shape
//            Shape shape = shExSchema.getMappedShape(triplesMap.getUri());
//            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
//            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
//            for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
//                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
//                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
//                    // when object map
//                    if (predicateObjectPair.getObjectMap().isPresent()) continue;
//
//                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();
//                    RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();
//
//                    // create Triple Constraint From referencing-object map
//                    TripleConstraint tcFromPROMap = new TripleConstraint(predicateMap, refObjectMap);
//                    shape.addTripleConstraint(tcFromPROMap);
//                }
//            }
//        }
//
//
//        Set<URI> checkedTriplesMaps = new HashSet<>();
//        for (TriplesMap triplesMap : triplesMaps) {
//            URI uriOfTriplesMap = triplesMap.getUri();
//            if (!checkedTriplesMaps.contains(uriOfTriplesMap)) {
//                // find the mapped shape
//                Shape mappedShape = shExSchema.getMappedShape(uriOfTriplesMap);
//                ////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                Set<Shape> baseShapes = shExSchema.getShapesToShareTheSameSubjects(mappedShape);
//                for (Shape baseShape : baseShapes)
//                    checkedTriplesMaps.add(baseShape.getMappedTriplesMap().get());
//
//                Set<Set<Shape>> setsForDerivedShapes = shExSchema.createSetsForDerivedShapes(baseShapes);
//                for (Set<Shape> set: setsForDerivedShapes) {
//                    Shape derivedShape = new Shape(set);
//
//                    // tripleConstraint
//                    Set<URI> classIRIs = new TreeSet<>();
//                    for (Shape shape: set) {
//                        Set<TripleConstraint> tripleConstraints = shape.getTripleConstraints();
//                        for (TripleConstraint tc: tripleConstraints) {
//                            if (tc.getMappedType().equals(TripleConstraint.MappedTypes.RR_CLASSES))
//                                classIRIs.addAll(tc.getClassIRIs());
//                            else
//                                derivedShape.addTripleConstraint(tc); // except for RR_CLASSES tripleConstraints
//                        }
//                    }
//
//                    // RR_CLASSES tripleConstraint
//                    derivedShape.addTripleConstraint(new TripleConstraint(classIRIs));
//
//                    shExSchema.addShape(derivedShape);
//                }
//            }
//        }

        return shExSchema;
    }

    // register namespaces in rmlModel to ShExModel
    private static void addPrefixes(RMLModel rmlModel, ShExSchema shExSchema) {
        Set<Map.Entry<String, String>> entrySet = rmlModel.getPrefixMap().entrySet();
        for (Map.Entry<String, String> entry: entrySet)
            shExSchema.addPrefixDecl(entry.getKey(), entry.getValue());

        shExSchema.addPrefixDecl("rdf", PrefixMap.getURI("rdf").toString());
    }
}
