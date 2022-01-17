package rml2shex.mapping.model.shex;

import rml2shex.util.PrefixMap;
import rml2shex.util.Id;
import rml2shex.mapping.model.rml.*;

import java.net.URI;
import java.util.*;

public class ShExSchemaFactory {
    public static ShExSchema getShExSchema(RMLModel rmlModel, String shexBasePrefix, URI shexBaseIRI) {
        ShExSchema shExSchema = new ShExSchema(shexBasePrefix, shexBaseIRI);

        addPrefixes(rmlModel, shExSchema);

        Map<TriplesMap, ConversionResult> tmcrMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        for (TriplesMap triplesMap: triplesMaps) tmcrMap.put(triplesMap, new ConversionResult());

        for (TriplesMap triplesMap: triplesMaps) {
            convertSubjectMap2NodeConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // subject map -> node constraint
            convertSubjectMap2TripleConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // rr:class of subject map -> triple constraint
            convertPredicateObjectMap2TripleConstraint(shexBasePrefix, shexBaseIRI, triplesMap, tmcrMap.get(triplesMap)); // predicate-object map -> triple constraints
        }

        Set<Set<TriplesMap>> triplesMapGroup = groupTriplesMapWithSameSubject(tmcrMap);
        assignReferenceIdPerGroup(shexBasePrefix, shexBaseIRI, triplesMapGroup, tmcrMap);






        for (TriplesMap triplesMap : triplesMaps) {

            SubjectMap subjectMap = triplesMap.getSubjectMap();

            // create a node constraint from subjectMap
            Id sm2NcId = NodeConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SM2NC");
            NodeConstraint sm2nc = new NodeConstraint(sm2NcId, subjectMap);

            Set<TripleConstraint> tripleConstraints = new HashSet<>(); // temporarily

            // create a triple constraint from rr:class of subjectMap
            Set<URI> classes = subjectMap.getClasses();
            if (classes.size() > 0) {
                Id sm2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "SM2TC");
                TripleConstraint sm2tc = new TripleConstraint(sm2TcId, classes);

                tripleConstraints.add(sm2tc);
            }

            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair : predicateObjectPairs) {
                    PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                    if (predicateObjectPair.getRefObjectMap().isPresent()) {
                        // when referencing object map
                        RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();

                        Id pr2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "PR2TC");
                        TripleConstraint pr2tc = new TripleConstraint(pr2TcId, predicateMap, refObjectMap);

                        tripleConstraints.add(pr2tc);
                    }

                    if (predicateObjectPair.getObjectMap().isPresent()) {
                        // when object map
                        ObjectMap objectMap = predicateObjectPair.getObjectMap().get();

                        Id po2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "PO2TC");
                        TripleConstraint po2tc = new TripleConstraint(po2TcId, predicateMap, objectMap);

                        tripleConstraints.add(po2tc);
                    }
                }

            }

            int sizeOfTCs = tripleConstraints.size();

            if (sizeOfTCs == 0) {
                // only NodeConstrain from SubjectMap
                shExSchema.addShapeExpr(sm2nc);
            } else {
                Id tm2ShId = new Id(shexBasePrefix, shexBaseIRI, "TM2Sh" + Shape.getIncrementer());
                Shape tm2sh;

                if (sizeOfTCs == 1) {
                    tm2sh = new Shape(tm2ShId, tripleConstraints.stream().findAny().get()); // one triple constraint
                } else {
                    Id tm2EoId = new Id(shexBasePrefix, shexBaseIRI, "TM2EO" + EachOf.getIncrementer());
                    List<TripleConstraint> list = tripleConstraints.stream().limit(2).toList();
                    EachOf tm2eo = new EachOf(tm2EoId, list.get(0), list.get(1));
                    tripleConstraints.removeAll(list);
                    tripleConstraints.stream().forEach(tc -> tm2eo.addTripleExpr(tc));

                    tm2sh = new Shape(tm2ShId, tm2eo); // EachOf as expression
                }

                Id tm2SaId = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TM2SA");
                ShapeAnd tm2sa = new ShapeAnd(tm2SaId, sm2nc, tm2sh); // node constraint + triple constraint

                shExSchema.addShapeExpr(tm2sa);
            }
        }

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
        conversionResult.nodeConstraint4Subject = sm2nc;
    }

    private static Set<Set<TriplesMap>> groupTriplesMapWithSameSubject(Map<TriplesMap, ConversionResult> tcMap) {
        Map<NodeConstraint, TriplesMap> ntMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = tcMap.keySet();
        for (TriplesMap triplesMap : triplesMaps) {
            ntMap.put(tcMap.get(triplesMap).nodeConstraint4Subject, triplesMap);
        }

        Set<NodeConstraint> nodeConstraints = ntMap.keySet();

        Set<Set<NodeConstraint>> ncGroup = new HashSet<>();
        // build group
        for (NodeConstraint nc1: nodeConstraints) {
            Set<NodeConstraint> ncSubgroup = new HashSet<>();
            for (NodeConstraint nc2: nodeConstraints) { if (nc1.isEquivalent(nc2)) ncSubgroup.add(nc2); }
            ncGroup.add(ncSubgroup);
        }

        Set<Set<TriplesMap>> tmGroup = new HashSet<>();
        for (Set<NodeConstraint> ncSubgroup: ncGroup) {
            Set<TriplesMap> tmSubgroup = new HashSet<>();
            for (NodeConstraint nc: ncSubgroup) { tmSubgroup.add(ntMap.get(nc)); }
            tmGroup.add(tmSubgroup);
        }

        return tmGroup;
    }

    private static void assignReferenceIdPerGroup(String shexBasePrefix, URI shexBaseIRI, Set<Set<TriplesMap>> tmGroup, Map<TriplesMap, ConversionResult> tmcrMap) {
        for (Set<TriplesMap> subgroup: tmGroup) {
            int sizeOfSubgroup = subgroup.size();

            if (sizeOfSubgroup == 1) {
                TriplesMap triplesMap = subgroup.stream().findAny().get();
                int countOfTripleConstraints = getCountOfTripleConstraints(triplesMap);

                ConversionResult conversionResult = tmcrMap.get(triplesMap);
                if (countOfTripleConstraints == 0) {
                    conversionResult.referenceId = conversionResult.nodeConstraint4Subject.getID();
                } else {
                    Id tm2saId = ShapeAnd.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TM2SA");
                    conversionResult.referenceId = tm2saId; // node constraint + triple constraint
                }
            } else {
                Id tg2soId = ShapeOr.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "TG2SO");
                for (TriplesMap triplesMap: subgroup) {
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
            conversionResult.typePO = sm2tc;
        }
    }

    // predicate object map -> triple constraints
    private static void convertPredicateObjectMap2TripleConstraint(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, ConversionResult conversionResult) {
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

                    conversionResult.POs.add(po2tc);
                }
            }
        }
    }

    // predicate referencing object map -> triple constraints
    private static void convertPredicateRefObjectMap2TripleConstraint(String shexBasePrefix, URI shexBaseIRI, TriplesMap triplesMap, ConversionResult conversionResult) {
        List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
        for (PredicateObjectMap predicateObjectMap : predicateObjectMaps) {
            List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();

            for (PredicateObjectMap.PredicateObjectPair predicateObjectPair : predicateObjectPairs) {
                PredicateMap predicateMap = predicateObjectPair.getPredicateMap();

                // when referencing object map
                if (predicateObjectPair.getRefObjectMap().isPresent()) {
                    RefObjectMap refObjectMap = predicateObjectPair.getRefObjectMap().get();

                    Id pr2TcId = TripleConstraint.IdGenerator.generateId(shexBasePrefix, shexBaseIRI, "PR2TC");
                    TripleConstraint pr2tc = new TripleConstraint(pr2TcId, predicateMap, refObjectMap);

                    //tripleConstraints.add(pr2tc);
                }
            }
        }
    }

    private static class ConversionResult {
        private NodeConstraint nodeConstraint4Subject; // from the subject map
        private TripleConstraint typePO; // from rr:class in the subject map
        private Set<TripleConstraint> POs = new HashSet<>(); // from predicate object maps
        private Set<TripleConstraint> refPOs; // from predicate ref object maps

        private Id convertedShapeExprId; // id of subject node constraint or shapeAnd
        private ShapeExpr convertedShapeExpr;
        private Set<ShapeExpr> inferredShapeExpr;
        private Id referenceId;
        private ShapeExpr groupShapeExpr; // convertedShapeExpr or one of inferredShapeExprs
    }
}