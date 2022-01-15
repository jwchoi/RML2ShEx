package rml2shex.mapping.model.shex;

import rml2shex.util.PrefixMap;
import rml2shex.util.ID;
import rml2shex.mapping.model.rml.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class ShExSchemaFactory {
    public static ShExSchema getShExSchema(RMLModel rmlModel, String shexBasePrefix, URI shexBaseIRI) {
        ShExSchema shExSchema = new ShExSchema(shexBasePrefix, shexBaseIRI);

        addPrefixes(rmlModel, shExSchema);

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        Map<TriplesMap, NodeConstraint> tnMap = new HashMap<>();
        for (TriplesMap triplesMap : triplesMaps) {
            SubjectMap subjectMap = triplesMap.getSubjectMap();

            // create a node constraint from subjectMap
            ID sm2ncID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "SM2NC" + NodeConstraint.getIncrementer());
            NodeConstraint sm2nc = new NodeConstraint(sm2ncID, subjectMap);

            tnMap.put(triplesMap, sm2nc);
        }

        groupTriplesMapWithSameSubject(tnMap);

        for (TriplesMap triplesMap : triplesMaps) {

            SubjectMap subjectMap = triplesMap.getSubjectMap();

            // create a node constraint from subjectMap
            ID sm2ncID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "SM2NC" + NodeConstraint.getIncrementer());
            NodeConstraint sm2nc = new NodeConstraint(sm2ncID, subjectMap);

            tnMap.put(triplesMap, sm2nc);

            Set<TripleConstraint> tripleConstraints = new HashSet<>(); // temporarily

            // create a triple constraint from rr:class of subjectMap
            Set<URI> classes = subjectMap.getClasses();
            if (classes.size() > 0) {
                ID sm2tcID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "SM2TC" + TripleConstraint.getIncrementer());
                TripleConstraint sm2tc = new TripleConstraint(sm2tcID, classes);

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

                        ID pr2tcID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "PR2TC" + TripleConstraint.getIncrementer());
                        TripleConstraint pr2tc = new TripleConstraint(pr2tcID, predicateMap, refObjectMap);

                        tripleConstraints.add(pr2tc);
                    }

                    if (predicateObjectPair.getObjectMap().isPresent()) {
                        // when object map
                        ObjectMap objectMap = predicateObjectPair.getObjectMap().get();

                        ID po2tcID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "PO2TC" + TripleConstraint.getIncrementer());
                        TripleConstraint po2tc = new TripleConstraint(po2tcID, predicateMap, objectMap);

                        tripleConstraints.add(po2tc);
                    }
                }

            }

            int sizeOfTCs = tripleConstraints.size();

            if (sizeOfTCs == 0) {
                // only NodeConstrain from SubjectMap
                shExSchema.addShapeExpr(sm2nc);
            } else {
                ID tm2shID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "TM2Sh" + Shape.getIncrementer());
                Shape tm2sh;

                if (sizeOfTCs == 1) {
                    tm2sh = new Shape(tm2shID, tripleConstraints.stream().findAny().get()); // one triple constraint
                } else {
                    ID tm2eoID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "TM2EO" + EachOf.getIncrementer());
                    List<TripleConstraint> list = tripleConstraints.stream().limit(2).toList();
                    EachOf tm2eo = new EachOf(tm2eoID, list.get(0), list.get(1));
                    tripleConstraints.removeAll(list);
                    tripleConstraints.stream().forEach(tc -> tm2eo.addTripleExpr(tc));

                    tm2sh = new Shape(tm2shID, tm2eo); // EachOf as expression
                }

                ID tm2saID = new ID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), "TM2SA" + ShapeAnd.getIncrementer());
                ShapeAnd tm2sa = new ShapeAnd(tm2saID, sm2nc, tm2sh); // node constraint + triple constraint

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

    private static void groupTriplesMapWithSameSubject(Map<TriplesMap, NodeConstraint> tnMap) {
        Set<NodeConstraint> set = tnMap.values().stream().collect(Collectors.toSet());

        Set<Set<NodeConstraint>> group = new HashSet<>();

        for (NodeConstraint nc1: set) {
            Set<NodeConstraint> subgroup = new HashSet<>();
            for (NodeConstraint nc2: set) { if (nc1.isEquivalent(nc2)) subgroup.add(nc2); }
            group.add(subgroup);
        }
    }
}
