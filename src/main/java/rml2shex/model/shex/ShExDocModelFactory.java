package rml2shex.model.shex;

import com.google.common.collect.Sets;
import rml2shex.util.IRI;
import rml2shex.model.rml.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class ShExDocModelFactory {
    public static ShExDocModel getShExDocModel(RMLModel rmlModel, String shexBasePrefix, URI shexBaseIRI) {
        ShExDocModel shExDocModel = new ShExDocModel(shexBasePrefix, shexBaseIRI);

        addPrefixes(rmlModel, shExDocModel);

        Map<TriplesMap, ConversionResult> tmcrMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        for (TriplesMap triplesMap : triplesMaps) tmcrMap.put(triplesMap, new ConversionResult());

        for (TriplesMap triplesMap : triplesMaps) {
            convertSubjectMap2NodeConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // subject map -> node constraint
            convertSubjectMap2TripleConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // rr:class of subject map -> triple constraint
            convertPredicateObjectMaps2TripleConstraints(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // predicate-object map -> triple constraints
        }

        Set<Set<TriplesMap>> triplesMapGroup = groupTriplesMapWithSameSubject(tmcrMap);
        assignReferenceId(shexBasePrefix, shexBaseIRI, triplesMapGroup, tmcrMap);

        for (TriplesMap triplesMap : triplesMaps) {
            convertPredicateRefObjectMaps2TripleConstraints(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap); // predicate-referencing-object map -> triple constraints
        }

        convertTriplesMap2ShapeExpr(shexBasePrefix, shexBaseIRI, tmcrMap.values().stream().collect(Collectors.toSet()));

        Set<DeclarableShapeExpr> inferredDeclarableShapeExprs = getInferredDeclarableShapeExprs(shexBasePrefix, shexBaseIRI, triplesMapGroup, tmcrMap);

        inferredDeclarableShapeExprs.stream().forEach(inferredShapeExpr -> shExDocModel.addDeclarableShapeExpr(inferredShapeExpr));

        return shExDocModel;
    }

    // register namespaces in rmlModel to ShExModel
    private static void addPrefixes(RMLModel rmlModel, ShExDocModel shExDocModel) {
        Set<Map.Entry<String, String>> entrySet = rmlModel.getPrefixMap().entrySet();
        for (Map.Entry<String, String> entry : entrySet)
            shExDocModel.addPrefixDecl(entry.getKey(), entry.getValue());

        shExDocModel.addPrefixDecl("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    }

    // subject map -> node constraint
    private static void convertSubjectMap2NodeConstraint(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, ConversionResult conversionResult) {
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        IRI sm2ncId = NodeConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "NC");
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

    private static void assignReferenceId(String shexBasePrefix, URI shexBaseIRI, Set<Set<TriplesMap>> tmGroup, Map<TriplesMap, ConversionResult> tmcrMap) {
        for (Set<TriplesMap> subgroup : tmGroup) {
            int sizeOfSubgroup = subgroup.size();

            if (sizeOfSubgroup == 1) {
                TriplesMap triplesMap = subgroup.stream().findAny().get();
                int countOfTripleConstraints = getCountOfTripleConstraints(triplesMap);

                ConversionResult conversionResult = tmcrMap.get(triplesMap);
                if (countOfTripleConstraints == 0) {
                    conversionResult.referenceId = conversionResult.nodeConstraint.getId();
                } else {
                    IRI tm2saId = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SA");
                    conversionResult.referenceId = tm2saId; // node constraint + triple constraint
                }
                conversionResult.convertedShapeExprId = conversionResult.referenceId;
            } else {
                for (TriplesMap triplesMap : subgroup) {
                    IRI tg2soId = ShapeOr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SO");
                    ConversionResult conversionResult = tmcrMap.get(triplesMap);
                    conversionResult.referenceId = tg2soId; // ShapeOr
                }
            }
        }
    }

    private static int getCountOfTripleConstraints(TriplesMap triplesMap) {
        int count = 0;

        // from rr:class
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        Set<IRI> classes = subjectMap.getClasses();
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
        Set<IRI> classes = subjectMap.getClasses();
        if (classes.size() > 0) {
            IRI sm2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TC");
            IRI predicate = new IRI("rdf", URI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns#"), "type");
            TripleConstraint sm2tc = new TripleConstraint(sm2TcId, predicate, classes);
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

                    IRI po2tcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TC");
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
                    IRI referenceIdFromParentTriplesMap = getReferenceIdFromParentTriplesMap(parentTriplesMap, tmcrMap);

                    IRI pr2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TC");
                    TripleConstraint pr2tc = new TripleConstraint(pr2TcId, predicateMap, referenceIdFromParentTriplesMap);

                    ConversionResult conversionResult = tmcrMap.get(triplesMap);
                    conversionResult.tripleConstraints.add(pr2tc);
                }
            }
        }
    }

    private static IRI getReferenceIdFromParentTriplesMap(URI uriOfParentTriplesMap, Map<TriplesMap, ConversionResult> tmcrMap) {
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
            Set<TripleConstraint> tripleConstraintSet = conversionResult.tripleConstraints; // for predicate & object in RDF graph

            int countOfTripleConstraint = tripleConstraintSet.size();

            if (countOfTripleConstraint == 0) {
                conversionResult.convertedShapeExprId = nodeConstraint.getId();
                conversionResult.convertedDeclarableShapeExpr = nodeConstraint;
            } else {
                IRI tm2ShId = Shape.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "Sh");
                Shape tm2sh;

                if (countOfTripleConstraint == 1) {
                    tm2sh = new Shape(tm2ShId, tripleConstraintSet.stream().findAny().get()); // one triple constraint
                } else {
                    IRI tm2EoId = EachOf.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "EO");
                    List<TripleConstraint> tripleConstraintList = tripleConstraintSet.stream().collect(Collectors.toList());
                    EachOf tm2eo = new EachOf(tm2EoId, tripleConstraintList.remove(0), tripleConstraintList.remove(0));
                    tripleConstraintList.stream().forEach(tc -> tm2eo.addTripleExpr(tc));

                    tm2sh = new Shape(tm2ShId, tm2eo); // EachOf as expression
                }
                IRI tm2SaId = conversionResult.convertedShapeExprId != null ? conversionResult.convertedShapeExprId : ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SA");
                ShapeAnd tm2sa = new ShapeAnd(tm2SaId, nodeConstraint, tm2sh); // node constraint + (EachOf)triple constraints

                conversionResult.convertedShapeExprId = tm2SaId;
                conversionResult.convertedDeclarableShapeExpr = tm2sa;
            }
        }
    }

    private static Set<DeclarableShapeExpr> getInferredDeclarableShapeExprs(String shexBasePrefix, URI shexBaseIRI, Set<Set<TriplesMap>> triplesMapGroup, Map<TriplesMap, ConversionResult> tmcrMap) {
        Set<DeclarableShapeExpr> inferredDeclarableShapeExprs = new HashSet<>();

        for (Set<TriplesMap> triplesMapSubgroup: triplesMapGroup) {
            Set<ConversionResult> conversionResultSubgroup = triplesMapSubgroup.stream()
                    .map(triplesMap -> tmcrMap.get(triplesMap))
                    .collect(Collectors.toSet());

            int n = conversionResultSubgroup.size();

            if (n == 1) {
                inferredDeclarableShapeExprs.add(conversionResultSubgroup.stream().findAny().get().convertedDeclarableShapeExpr); // converted shapeExpr
                continue;
            }

            Map<ConversionResult, Set<IRI>> shapeExprIdsInferredFromConversionResult = new HashMap<>();
            conversionResultSubgroup.stream().forEach(conversionResult -> shapeExprIdsInferredFromConversionResult.put(conversionResult, new HashSet<>()));

            for (int r = 1; r <= n; r++) {
                Set<Set<ConversionResult>> combinations = Sets.combinations(conversionResultSubgroup, r);

                for (Set<ConversionResult> combination: combinations) {
                    if (r == 1) {
                        ConversionResult conversionResult = combination.stream().findAny().get();
                        shapeExprIdsInferredFromConversionResult.get(conversionResult).add(conversionResult.convertedShapeExprId);
                        inferredDeclarableShapeExprs.add(conversionResult.convertedDeclarableShapeExpr); // converted shapeExpr
                    } else {
                        IRI id = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SA");
                        List<ConversionResult> listFromCombination = combination.stream().collect(Collectors.toList());
                        ShapeAnd shapeAnd = new ShapeAnd(id, new ShapeExprRef(listFromCombination.remove(0).convertedShapeExprId), new ShapeExprRef(listFromCombination.remove(0).convertedShapeExprId));
                        listFromCombination.stream().forEach(conversionResult -> shapeAnd.addShapeExpr(new ShapeExprRef(conversionResult.convertedShapeExprId)));
                        combination.stream().forEach(conversionResult -> shapeExprIdsInferredFromConversionResult.get(conversionResult).add(id));
                        inferredDeclarableShapeExprs.add(shapeAnd);
                    }
                }
            }

            for (ConversionResult conversionResult: conversionResultSubgroup) {
                IRI referenceId = conversionResult.referenceId;
                List<IRI> ids = shapeExprIdsInferredFromConversionResult.get(conversionResult).stream().collect(Collectors.toList());
                ShapeOr shapeOr = new ShapeOr(referenceId, new ShapeExprRef(ids.remove(0)), new ShapeExprRef(ids.remove(0)));
                ids.stream().forEach(id -> shapeOr.addShapeExpr(new ShapeExprRef(id)));

                inferredDeclarableShapeExprs.add(shapeOr);
            }
        }

        return inferredDeclarableShapeExprs;
    }

    private static class ConversionResult {
        private NodeConstraint nodeConstraint; // from the subject map
        private Set<TripleConstraint> tripleConstraints = new HashSet<>(); // from rr:class, predicate object maps, predicate ref object maps

        private IRI convertedShapeExprId;
        private DeclarableShapeExpr convertedDeclarableShapeExpr; // (nodeConstraint + triplesConstraints) or nodeConstraint
        private IRI referenceId; // if (groupSize > 1) id of ShapeOr or if (groupSize == 1) convertedShapeExprId
    }
}