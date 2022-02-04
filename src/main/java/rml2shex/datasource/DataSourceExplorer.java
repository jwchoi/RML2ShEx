package rml2shex.datasource;

import rml2shex.model.rml.*;

import java.net.URI;
import java.util.*;

public class DataSourceExplorer {
    public static void injectMetadataInto(RMLModel rmlModel, String dataSourceDir) {

        Session session = Session.createSession();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        Map<TriplesMap, DataFrame> tmdfMap = loadLogicalSources(triplesMaps, dataSourceDir, session);

        assignColumns(tmdfMap);

        acquireColumnMetadata(tmdfMap);

        acquireOccurrenceCount(tmdfMap);
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

    private static void assignColumns(Map<TriplesMap, DataFrame> tmdfMap) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();
        for(TriplesMap triplesMap: triplesMaps) {
            DataFrame df = tmdfMap.get(triplesMap);

            // SubjectMap
            SubjectMap subjectMap = triplesMap.getSubjectMap();

            Optional<Template> template = subjectMap.getTemplate();
            if (template.isPresent()) df.setSubjectColumns(template.get().getLogicalReferences());

            Optional<Column> column = subjectMap.getColumn();
            if (column.isPresent()) df.setSubjectColumns(Arrays.asList(column.get()));

            Optional<Column> reference = subjectMap.getReference();
            if (reference.isPresent()) df.setSubjectColumns(Arrays.asList(reference.get()));

            // PredicateObjectMap
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    // ObjectMap
                    Optional<ObjectMap> objectMap = predicateObjectPair.getObjectMap();
                    if (objectMap.isPresent()) {
                        template = objectMap.get().getTemplate();
                        if (template.isPresent()) df.addObjectColumns(template.get().getLogicalReferences());

                        column = objectMap.get().getColumn();
                        if (column.isPresent()) df.addObjectColumns(Arrays.asList(column.get()));

                        reference = objectMap.get().getReference();
                        if (reference.isPresent()) df.addObjectColumns(Arrays.asList(reference.get()));
                    }
                }
            }
        }

        for(TriplesMap triplesMap: triplesMaps) {
            DataFrame df = tmdfMap.get(triplesMap);

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
                        df.addParent(tmdfMap.get(parentTriplesMap));
                    }
                }
            }
        }
    }

    private static void acquireColumnMetadata(Map<TriplesMap, DataFrame> tmdfMap) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();

        for(TriplesMap triplesMap: triplesMaps) {
            DataFrame df = tmdfMap.get(triplesMap);

            List<Column> subjectColumns = df.getSubjectColumns();
            subjectColumns.stream().forEach(df::assignMetadata);

            List<List<Column>> objectColumnsList = df.getObjectColumnsList();
            objectColumnsList.stream().forEach(list -> list.forEach(df::assignMetadata));
        }
    }

    private static void acquireOccurrenceCount(Map<TriplesMap, DataFrame> tmdfMap) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();

        for(TriplesMap triplesMap: triplesMaps) {
            DataFrame df = tmdfMap.get(triplesMap);


            List<Column> subjectColumns = df.getSubjectColumns();

            List<List<Column>> objectColumnsList = df.getObjectColumnsList();
            for (List<Column> objectColumns: objectColumnsList) df.countOccurrence(subjectColumns, objectColumns);
        }
    }
}
