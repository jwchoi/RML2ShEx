package rml2shex.mapping.model.shex;

import com.google.common.collect.Sets;
import rml2shex.util.PrefixMap;
import rml2shex.util.Id;
import rml2shex.mapping.model.rml.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class ShExSchemaFactory {
    public static ShExSchema getShExSchema(RMLModel rmlModel, String shexBasePrefix, URI shexBaseIRI) {
        ShExSchema shExSchema = new ShExSchema(shexBasePrefix, shexBaseIRI);

        addPrefixes(rmlModel, shExSchema);

        Map<TriplesMap, ConversionResult> tmcrMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        for (TriplesMap triplesMap : triplesMaps) tmcrMap.put(triplesMap, new ConversionResult());

        for (TriplesMap triplesMap : triplesMaps) {
            convertSubjectMap2NodeConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // subject map -> node constraint
            convertSubjectMap2TripleConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // rr:class of subject map -> triple constraint
            convertPredicateObjectMaps2TripleConstraints(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // predicate-object map -> triple constraints
        }

        Set<Set<TriplesMap>> triplesMapGroup = groupTriplesMapWithSameSubject(tmcrMap);
        assignReferenceIdPerGroup(shexBasePrefix, shexBaseIRI, triplesMapGroup, tmcrMap);

        for (TriplesMap triplesMap : triplesMaps) {
            convertPredicateRefObjectMaps2TripleConstraints(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap); // predicate-referencing-object map -> triple constraints
        }

        convertTriplesMap2ShapeExpr(shexBasePrefix, shexBaseIRI, tmcrMap.values().stream().collect(Collectors.toSet()));

        Set<ShapeExpr> inferredShapeExprs = getInferredShapeExprs(shexBasePrefix, shexBaseIRI, triplesMapGroup, tmcrMap);

        inferredShapeExprs.stream().forEach(inferredShapeExpr -> shExSchema.addShapeExpr(inferredShapeExpr));

        inferredShapeExprs.stream().forEach(System.out::println);

        return shExSchema;
    }

    // register namespaces in rmlModel to ShExModel
    private static void addPrefixes(RMLModel rmlModel, ShExSchema shExSchema) {
        Set<Map.Entry<String, String>> entrySet = rmlModel.getPrefixMap().entrySet();
        for (Map.Entry<String, String> entry : entrySet)
            shExSchema.addPrefixDecl(entry.getKey(), entry.getValue());

        shExSchema.addPrefixDecl("rdf", PrefixMap.getURI("rdf").toString());
    }

    // subject map -> node constraint
    private static void convertSubjectMap2NodeConstraint(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, ConversionResult conversionResult) {
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        Id sm2ncId = NodeConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SM2NC");
        NodeConstraint sm2nc = new NodeConstraint(sm2ncId, subjectMap);
        conversionResult.nodeConstraint = sm2nc;
    }

    private static Set<Set<TriplesMap>> groupTriplesMapWithSameSubject(Map<TriplesMap, ConversionResult> tcMap) {
        Map<NodeConstraint, TriplesMap> ntMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = tcMap.keySet();
        for (TriplesMap triplesMap : triplesMaps) {
            ntMap.put(tcMap.get(triplesMap).nodeConstraint, triplesMap);
        }

        Set<NodeConstraint> nodeConstraints = ntMap.keySet();

        Set<Set<NodeConstraint>> ncGroup = new HashSet<>();
        // build group
        for (NodeConstraint nc1 : nodeConstraints) {
            Set<NodeConstraint> ncSubgroup = new HashSet<>();
            for (NodeConstraint nc2 : nodeConstraints) {
                if (nc1.isEquivalent(nc2)) ncSubgroup.add(nc2);
            }
            ncGroup.add(ncSubgroup);
        }

        Set<Set<TriplesMap>> tmGroup = new HashSet<>();
        for (Set<NodeConstraint> ncSubgroup : ncGroup) {
            Set<TriplesMap> tmSubgroup = new HashSet<>();
            for (NodeConstraint nc : ncSubgroup) {
                tmSubgroup.add(ntMap.get(nc));
            }
            tmGroup.add(tmSubgroup);
        }

        return tmGroup;
    }

    private static void assignReferenceIdPerGroup(String shexBasePrefix, URI shexBaseIRI, Set<Set<TriplesMap>> tmGroup, Map<TriplesMap, ConversionResult> tmcrMap) {
        for (Set<TriplesMap> subgroup : tmGroup) {
            int sizeOfSubgroup = subgroup.size();

            if (sizeOfSubgroup == 1) {
                TriplesMap triplesMap = subgroup.stream().findAny().get();
                int countOfTripleConstraints = getCountOfTripleConstraints(triplesMap);

                ConversionResult conversionResult = tmcrMap.get(triplesMap);
                if (countOfTripleConstraints == 0) {
                    conversionResult.referenceId = conversionResult.nodeConstraint.getID();
                } else {
                    Id tm2saId = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TM2SA");
                    conversionResult.referenceId = tm2saId; // node constraint + triple constraint
                }
            } else {
                Id tg2soId = ShapeOr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TG2SO");
                for (TriplesMap triplesMap : subgroup) {
                    ConversionResult conversionResult = tmcrMap.get(triplesMap);
                    conversionResult.referenceId = tg2soId; // group id
                }
            }
        }
    }

    private static int getCountOfTripleConstraints(TriplesMap triplesMap) {
        int count = 0;

        // from rr:class
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        Set<URI> classes = subjectMap.getClasses();
        if (classes.size() > 0) count++;

        // from predicate object maps
        List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
        for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
            List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

            count += predicateObjectPairs.size();
        }

        return count;
    }

    // rr:class of subject map -> triple constraint
    private static void convertSubjectMap2TripleConstraint(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, ConversionResult conversionResult) {
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        Set<URI> classes = subjectMap.getClasses();
        if (classes.size() > 0) {
            Id sm2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SM2TC");
            TripleConstraint sm2tc = new TripleConstraint(sm2TcId, classes);
            conversionResult.tripleConstraints.add(sm2tc);
        }
    }

    // predicate object map -> triple constraints
    private static void convertPredicateObjectMaps2TripleConstraints(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, ConversionResult conversionResult) {
        List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
        for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
            List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

            for (PredicateObjectMap.PredicateObjectPair predicateObjectPair : predicateObjectPairs) {
                PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                // when object map
                if (predicateObjectPair.getObjectMap().isPresent()) {
                    ObjectMap objectMap = predicateObjectPair.getObjectMap().get();

                    Id po2tcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "PO2TC");
                    TripleConstraint po2tc = new TripleConstraint(po2tcId, predicateMap, objectMap);

                    conversionResult.tripleConstraints.add(po2tc);
                }
            }
        }
    }

    // predicate referencing object map -> triple constraints
    private static void convertPredicateRefObjectMaps2TripleConstraints(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, Map<TriplesMap, ConversionResult> tmcrMap) {
        List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
        for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
            List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

            for (PredicateObjectMap.PredicateObjectPair predicateObjectPair : predicateObjectPairs) {
                PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                // when referencing object map
                if (predicateObjectPair.getRefObjectMap().isPresent()) {
                    RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();
                    URI parentTriplesMap = refObjectMap.getParentTriplesMap();
                    Id referenceIdFromParentTriplesMap = getReferenceIdFromParentTriplesMap(parentTriplesMap, tmcrMap);

                    Id pr2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "PR2TC");
                    TripleConstraint pr2tc = new TripleConstraint(pr2TcId, predicateMap, referenceIdFromParentTriplesMap);

                    ConversionResult conversionResult = tmcrMap.get(triplesMap);
                    conversionResult.tripleConstraints.add(pr2tc);
                }
            }
        }
    }

    private static Id getReferenceIdFromParentTriplesMap(URI uriOfParentTriplesMap, Map<TriplesMap, ConversionResult> tmcrMap) {
        Set<TriplesMap> triplesMaps = tmcrMap.keySet();

        TriplesMap parentTriplesMap = triplesMaps.stream()
                .filter(triplesMap -> triplesMap.getUri().equals(uriOfParentTriplesMap))
                .findFirst()
                .get();

        ConversionResult conversionResultCorrespondingToParentTriplesMap = tmcrMap.get(parentTriplesMap);

        return conversionResultCorrespondingToParentTriplesMap.referenceId;
    }

    private static void convertTriplesMap2ShapeExpr(String shexBasePrefix, URI shexBaseIRI, Set<ConversionResult> conversionResults) {
        for (ConversionResult conversionResult: conversionResults) {
            NodeConstraint nodeConstraint = conversionResult.nodeConstraint; // for subject in RDF graph
            Set<TripleConstraint> tripleConstraints = conversionResult.tripleConstraints; // for predicate & object in RDF graph

            int countOfTripleConstraint = tripleConstraints.size();

            if (countOfTripleConstraint == 0) {
                conversionResult.convertedShapeExprId = nodeConstraint.getID();
                conversionResult.convertedShapeExpr = nodeConstraint;
            } else {
                Id tm2ShId = Shape.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TM2Sh");
                Shape tm2sh;

                if (countOfTripleConstraint == 1) {
                    tm2sh = new Shape(tm2ShId, tripleConstraints.stream().findAny().get()); // one triple constraint
                } else {
                    Id tm2EoId = EachOf.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TM2EO");
                    List<TripleConstraint> list = tripleConstraints.stream().collect(Collectors.toList());
                    EachOf tm2eo = new EachOf(tm2EoId, list.remove(0), list.remove(0));
                    list.stream().forEach(tc -> tm2eo.addTripleExpr(tc));

                    tm2sh = new Shape(tm2ShId, tm2eo); // EachOf as expression
                }

                Id tm2SaId = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TM2SA");
                ShapeAnd tm2sa = new ShapeAnd(tm2SaId, nodeConstraint, tm2sh); // node constraint + (EachOf)triple constraints

                conversionResult.convertedShapeExprId = tm2SaId;
                conversionResult.convertedShapeExpr = tm2sa;
            }
        }
    }

    private static Set<ShapeExpr> getInferredShapeExprs(String shexBasePrefix, URI shexBaseIRI, Set<Set<TriplesMap>> triplesMapGroup, Map<TriplesMap, ConversionResult> tmcrMap) {
        Set<ShapeExpr> inferredShapeExprs = new HashSet<>();

        for (Set<TriplesMap> triplesMapSubgroup: triplesMapGroup) {
            Set<ConversionResult> conversionResultSubgroup = triplesMapSubgroup.stream()
                    .map(triplesMap -> tmcrMap.get(triplesMap))
                    .collect(Collectors.toSet());

            int n = conversionResultSubgroup.size();

            if (n == 1) {
                inferredShapeExprs.add(conversionResultSubgroup.stream().findAny().get().convertedShapeExpr); // converted shapeExpr
                continue;
            }

            Set<Id> inferredShapeExprIdsOfSubgroup = new HashSet<>();

            for (int r = 1; r <= n; r++) {
                Set<Set<ConversionResult>> combinations = Sets.combinations(conversionResultSubgroup, r);

                for (Set<ConversionResult> combination: combinations) {
                    if (r == 1) {
                        ConversionResult conversionResult = combination.stream().findAny().get();
                        inferredShapeExprIdsOfSubgroup.add(conversionResult.convertedShapeExprId);
                        inferredShapeExprs.add(conversionResult.convertedShapeExpr); // converted shapeExpr
                    } else {
                        Id id = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "InSA");
                        List<ConversionResult> list = combination.stream().collect(Collectors.toList());
                        ShapeAnd shapeAnd = new ShapeAnd(id, new ShapeExprRef(list.remove(0).convertedShapeExprId), new ShapeExprRef(list.remove(0).convertedShapeExprId));
                        list.stream().forEach(conversionResult -> shapeAnd.addShapeExpr(new ShapeExprRef(conversionResult.convertedShapeExprId)));
                        inferredShapeExprIdsOfSubgroup.add(id);
                        inferredShapeExprs.add(shapeAnd);
                    }
                }
            }

            Id groupId = conversionResultSubgroup.stream().findAny().get().referenceId;
            List<Id> ids = inferredShapeExprIdsOfSubgroup.stream().collect(Collectors.toList());
            ShapeOr shapeOr = new ShapeOr(groupId, new ShapeExprRef(ids.remove(0)), new ShapeExprRef(ids.remove(0)));
            ids.stream().forEach(id -> shapeOr.addShapeExpr(new ShapeExprRef(id)));

            inferredShapeExprs.add(shapeOr);
        }

        return inferredShapeExprs;
    }

    private static class ConversionResult {
        private NodeConstraint nodeConstraint; // from the subject map
        private Set<TripleConstraint> tripleConstraints = new HashSet<>(); // from rr:class, predicate object maps, predicate ref object maps

        private Id convertedShapeExprId;
        private ShapeExpr convertedShapeExpr; // (nodeConstraint + triplesConstraints) or nodeConstraint
        private Id referenceId; // if (groupSize > 1) groupId or if (groupSize == 1) convertedShapeExprId
    }
}