package rml2shex.model.shex;

import com.google.common.collect.Sets;
import rml2shex.commons.IRI;
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
            convertSubjectMap2NodeConstraint(triplesMap, tmcrMap.get(triplesMap)); // subject map -> node constraint
        }

        Set<Set<TriplesMap>> triplesMapGroup = groupTriplesMapWithSameSubject(tmcrMap);
        assignReferenceId(shexBasePrefix, shexBaseIRI, triplesMapGroup, tmcrMap);

        for (TriplesMap triplesMap : triplesMaps) {
            convertSubjectMap2TripleConstraint(triplesMap, tmcrMap.get(triplesMap)); // rr:class of subject map -> triple constraint
            convertPredicateObjectMaps2TripleConstraints(triplesMap, tmcrMap.get(triplesMap)); // predicate-object map -> triple constraints
        }

        convertPredicateRefObjectMaps2TripleConstraints(tmcrMap); // predicate-referencing-object map -> triple constraints

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
    private static void convertSubjectMap2NodeConstraint(TriplesMap triplesMap, ConversionResult conversionResult) {
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        NodeConstraint sm2nc = new NodeConstraint(subjectMap);
        conversionResult.nodeConstraint = sm2nc;
    }

    // rr:class of subject map -> triple constraint
    private static void convertSubjectMap2TripleConstraint(TriplesMap triplesMap, ConversionResult conversionResult) {
        SubjectMap subjectMap = triplesMap.getSubjectMap();
        Set<IRI> classes = subjectMap.getClasses();
        if (classes.size() > 0) {
            IRI predicate = new IRI("rdf", URI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns#"), "type");
            TripleConstraint sm2tc = new TripleConstraint(predicate, classes);
            conversionResult.tripleConstraints.add(sm2tc);
        }
    }

    // predicate object map -> triple constraints
    private static void convertPredicateObjectMaps2TripleConstraints(TriplesMap triplesMap, ConversionResult conversionResult) {
        List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
        for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
            List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

            for (PredicateObjectMap.PredicateObjectPair predicateObjectPair : predicateObjectPairs) {
                PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                // when object map
                if (predicateObjectPair.getObjectMap().isPresent()) {
                    ObjectMap objectMap = predicateObjectPair.getObjectMap().get();

                    Optional<Long> maxOccurs = predicateObjectPair.getMaxOccurs();

                    TripleConstraint po2tc = new TripleConstraint(predicateMap, objectMap, maxOccurs);

                    conversionResult.tripleConstraints.add(po2tc);
                }
            }
        }
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

            for (TriplesMap triplesMap : subgroup) {
                ConversionResult conversionResult = tmcrMap.get(triplesMap);

                IRI referenceId = DeclarableShapeExpr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "S");

                conversionResult.referenceId4ShapeExpr = referenceId;
                conversionResult.countOfTheGroupMembers = sizeOfSubgroup;

                if (sizeOfSubgroup == 1)
                    conversionResult.convertedShapeExprId = conversionResult.referenceId4ShapeExpr;
            }

        }
    }

    // predicate referencing object map -> triple constraints
    private static void convertPredicateRefObjectMaps2TripleConstraints(Map<TriplesMap, ConversionResult> tmcrMap) {
        Set<TriplesMap> triplesMaps = tmcrMap.keySet();

        for (TriplesMap triplesMap: triplesMaps) {

            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair : predicateObjectPairs) {
                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                    // when referencing object map
                    if (predicateObjectPair.getRefObjectMap().isPresent()) {
                        RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();
                        URI uriOfParentTriplesMap = refObjectMap.getParentTriplesMap();
                        IRI referenceIdFromParentTriplesMap = getReferenceIdFromTriplesMap(uriOfParentTriplesMap, tmcrMap);

                        Optional<Long> minOccurs = predicateObjectPair.getMinOccurs();
                        Optional<Long> maxOccurs = predicateObjectPair.getMaxOccurs();

                        TripleConstraint pr2tc = new TripleConstraint(predicateMap, referenceIdFromParentTriplesMap, minOccurs, maxOccurs, false);

                        ConversionResult conversionResult = tmcrMap.get(triplesMap);
                        conversionResult.tripleConstraints.add(pr2tc);

                        // for inverse
                        URI uriOfChildTriplesMap = triplesMap.getUri();
                        IRI referenceIdFromChildTriplesMap = getReferenceIdFromTriplesMap(uriOfChildTriplesMap, tmcrMap);

                        Optional<Long> inverseMinOccurs = predicateObjectPair.getInverseMinOccurs();
                        Optional<Long> inverseMaxOccurs = predicateObjectPair.getInverseMaxOccurs();

                        TripleConstraint pr2InverseTc = new TripleConstraint(predicateMap, referenceIdFromChildTriplesMap, inverseMinOccurs, inverseMaxOccurs, true);

                        TriplesMap parentTriplesMap = triplesMaps.stream().filter(tm -> tm.getUri().equals(uriOfParentTriplesMap)).findAny().get();
                        ConversionResult conversionResultCorrespondingToParentTriplesMap = tmcrMap.get(parentTriplesMap);
                        conversionResultCorrespondingToParentTriplesMap.tripleConstraints.add(pr2InverseTc);
                    }
                }
            }

        }
    }

    private static IRI getReferenceIdFromTriplesMap(URI uriOfTriplesMap, Map<TriplesMap, ConversionResult> tmcrMap) {
        Set<TriplesMap> triplesMaps = tmcrMap.keySet();

        TriplesMap foundTriplesMap = triplesMaps.stream()
                .filter(triplesMap -> triplesMap.getUri().equals(uriOfTriplesMap))
                .findFirst()
                .get();

        ConversionResult conversionResult = tmcrMap.get(foundTriplesMap);

        return conversionResult.referenceId4ShapeExpr;
    }

    private static void convertTriplesMap2ShapeExpr(String shexBasePrefix, URI shexBaseIRI, Set<ConversionResult> conversionResults) {
        for (ConversionResult conversionResult: conversionResults) {
            NodeConstraint nodeConstraint = conversionResult.nodeConstraint; // for subject in RDF graph
            Set<TripleConstraint> tripleConstraints = conversionResult.tripleConstraints; // for predicate & object in RDF graph

            int countOfTripleConstraints = tripleConstraints.size();

            if (countOfTripleConstraints == 0) {
                conversionResult.convertedShapeExpr = nodeConstraint;

                if (conversionResult.convertedShapeExprId == null) {
                    IRI ncId = DeclarableShapeExpr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "S");
                    conversionResult.convertedShapeExprId = ncId;
                }

                nodeConstraint.setId(conversionResult.convertedShapeExprId);
            } else {
                Shape tm2sh;

                if (countOfTripleConstraints == 1) {
                    TripleConstraint tc = tripleConstraints.stream().findAny().get();

                    if (conversionResult.countOfTheGroupMembers > 1) {
                        IRI tcId = DeclarableTripleExpr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "T");
                        tc.setId(tcId);
                        conversionResult.referenceId4TripleExpr = tcId;
                    }

                    tm2sh = new Shape(true, tc); // one triple constraint
                } else {
                    List<TripleConstraint> tcList = tripleConstraints.stream().collect(Collectors.toList());
                    EachOf tm2eo = new EachOf(tcList.remove(0), tcList.remove(0));
                    tcList.stream().forEach(tc -> tm2eo.addTripleExpr(tc));

                    if (conversionResult.countOfTheGroupMembers > 1) {
                        IRI eoId = DeclarableTripleExpr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "T");
                        tm2eo.setId(eoId);
                        conversionResult.referenceId4TripleExpr = eoId;
                    }

                    tm2sh = new Shape(true, tm2eo); // EachOf as expression
                }

                if (conversionResult.convertedShapeExprId == null) {
                    IRI tm2SaId = DeclarableShapeExpr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "S");
                    conversionResult.convertedShapeExprId = tm2SaId;
                }
                ShapeAnd tm2sa = new ShapeAnd(conversionResult.convertedShapeExprId, nodeConstraint, tm2sh); // node constraint + (EachOf)triple constraints
                conversionResult.convertedShapeExpr = tm2sa;
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
                inferredDeclarableShapeExprs.add(conversionResultSubgroup.stream().findAny().get().convertedShapeExpr); // converted shapeExpr
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
                        inferredDeclarableShapeExprs.add(conversionResult.convertedShapeExpr); // converted shapeExpr
                    } else {
                        IRI id = DeclarableShapeExpr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "S");

                        NodeConstraint nodeConstraint = combination.stream().findAny().get().nodeConstraint;

                        List<IRI> refIds = combination.stream()
                                .filter(conversionResult -> conversionResult.referenceId4TripleExpr != null)
                                .map(conversionResult -> conversionResult.referenceId4TripleExpr)
                                .collect(Collectors.toList());

                        int sizeOfRefIds = refIds.size();

                        if (sizeOfRefIds > 1) {
                            EachOf eachOf = new EachOf(new TripleExprRef(refIds.remove(0)), new TripleExprRef(refIds.remove(0)));
                            refIds.stream().forEach(refId -> eachOf.addTripleExpr(new TripleExprRef(refId)));
                            Shape shape = new Shape(true, eachOf);
                            ShapeAnd shapeAnd = new ShapeAnd(id, nodeConstraint, shape);
                            combination.stream().forEach(conversionResult -> shapeExprIdsInferredFromConversionResult.get(conversionResult).add(id));
                            inferredDeclarableShapeExprs.add(shapeAnd);
                        }

                    }
                }
            }

            for (ConversionResult conversionResult: conversionResultSubgroup) {
                IRI referenceId = conversionResult.referenceId4ShapeExpr;
                List<IRI> ids = shapeExprIdsInferredFromConversionResult.get(conversionResult).stream().collect(Collectors.toList());
                ShapeOr shapeOr = new ShapeOr(referenceId, new ShapeExprRef(ids.remove(0)), new ShapeExprRef(ids.remove(0)));
                ids.stream().forEach(id -> shapeOr.addShapeExpr(new ShapeExprRef(id)));

                inferredDeclarableShapeExprs.add(shapeOr);
            }
        }

        return inferredDeclarableShapeExprs;
    }

    private static Optional<Long> getMinOccurs(List<JoinCondition> joinConditions) {
        return Optional.empty();
    }

    private static class ConversionResult {
        private NodeConstraint nodeConstraint; // from the subject map
        private Set<TripleConstraint> tripleConstraints = new HashSet<>(); // from rr:class, predicate object maps, predicate ref object maps

        private IRI convertedShapeExprId;
        private DeclarableShapeExpr convertedShapeExpr; // (nodeConstraint + triplesConstraints) or nodeConstraint

        private int countOfTheGroupMembers;
        private IRI referenceId4ShapeExpr; // if (groupSize > 1) id of ShapeOr or if (groupSize == 1) convertedShapeExprId
        private IRI referenceId4TripleExpr;
    }
}