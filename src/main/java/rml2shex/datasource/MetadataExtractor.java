package rml2shex.datasource;

import rml2shex.model.rml.*;

import java.net.URI;
import java.util.*;

public class MetadataExtractor {
    public static void injectMetadataInto(RMLModel rmlModel, String dataSourceDir) {

        Session session = Session.createSession();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        Map<TriplesMap, DataFrame> tmdfMap = loadLogicalSources(triplesMaps, dataSourceDir, session);

        Map<TriplesMap, Metadata> tmmMap = createMetadata(tmdfMap);
    }

    private static Map<TriplesMap, DataFrame> loadLogicalSources(Set<TriplesMap> triplesMaps, String dataSourceDir, Session session) {
        Map<TriplesMap, DataFrame> tmdfMap = new HashMap<>();

        for (TriplesMap triplesMap: triplesMaps) {
            LogicalTable logicalTable = triplesMap.getLogicalTable();
            if (logicalTable != null) {
                tmdfMap.put(triplesMap, session.createDataFrameFrom(logicalTable));
                continue;
            }

            LogicalSource logicalSource = triplesMap.getLogicalSource();
            if (logicalSource != null) {
                DataFrame df = session.createDataFrameFrom(logicalSource, dataSourceDir);
                tmdfMap.put(triplesMap, df);
            }
        }

        return  tmdfMap;
    }

    private static Map<TriplesMap, Metadata> createMetadata(Map<TriplesMap, DataFrame> tmdfMap) {
        Map<TriplesMap, Metadata> tmmMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = tmdfMap.keySet();
        for(TriplesMap triplesMap: triplesMaps) {
            Metadata metadata = new Metadata();
            // SubjectMap
            SubjectMap subjectMap = triplesMap.getSubjectMap();

            Optional<Template> template = subjectMap.getTemplate();
            if (template.isPresent()) metadata.subjectColumns = template.get().getLogicalReferences();

            Optional<Column> column = subjectMap.getColumn();
            if (column.isPresent()) metadata.subjectColumns.add(column.get());
            // PredicateObjectMap
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    // ObjectMap
                    Optional<ObjectMap> objectMap = predicateObjectPair.getObjectMap();
                    if (objectMap.isPresent()) {
                        template = objectMap.get().getTemplate();
                        if (template.isPresent()) metadata.objectColumns.add(template.get().getLogicalReferences());

                        column = objectMap.get().getColumn();
                        if (column.isPresent()) metadata.objectColumns.add(Arrays.asList(column.get()));
                    }
                }
            }

            tmmMap.put(triplesMap, metadata);
        }

        for(TriplesMap triplesMap: triplesMaps) {
            Metadata metadata = tmmMap.get(triplesMap);

            // PredicateObjectMap
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    // RefObjectMap
                    Optional<RefObjectMap> refObjectMap = predicateObjectPair.getRefObjectMap();
                    if (refObjectMap.isPresent()) {
                        URI parentTriplesMapURI = refObjectMap.get().getParentTriplesMap();
                        TriplesMap parentTriplesMap = triplesMaps.stream().filter(other -> parentTriplesMapURI.equals(other.getUri())).findAny().get();
                        metadata.parents.add(tmmMap.get(parentTriplesMap));
                    }
                }
            }

            tmmMap.put(triplesMap, metadata);
        }

        return tmmMap;
    }

    private static class Metadata {
        private List<Column> subjectColumns;
        private List<List<Column>> objectColumns;
        private List<Metadata> parents;

        private Metadata() {
            subjectColumns = new ArrayList<>();
            objectColumns = new ArrayList<>();
            parents = new ArrayList<>();
        }
    }
}
