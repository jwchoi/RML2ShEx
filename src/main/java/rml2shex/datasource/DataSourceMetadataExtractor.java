package rml2shex.datasource;

import rml2shex.model.rml.*;

import java.net.URI;
import java.util.*;

public class DataSourceMetadataExtractor {
    public static void acquireMetadataFor(RMLModel rmlModel, Optional<String> dataSourceDir, Optional<Database> database) {

        Session session = Session.createSession();
        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        Map<TriplesMap, DataSource> tmdfMap = loadLogicalSources(session, triplesMaps, dataSourceDir, database);

        acquireMetadataForSubjectMap(tmdfMap);
        acquireMetadataForPredicateObjectMap(tmdfMap);
        acquireMetadataForPredicateRefObjectMap(tmdfMap);
    }

    private static Map<TriplesMap, DataSource> loadLogicalSources(Session session, Set<TriplesMap> triplesMaps, Optional<String> dataSourceDir, Optional<Database> database) {
        Map<TriplesMap, DataSource> tmdfMap = new HashMap<>();

        for (TriplesMap triplesMap: triplesMaps) {
            LogicalTable logicalTable = triplesMap.getLogicalTable();
            if (logicalTable != null) {
                tmdfMap.put(triplesMap, DataSourceFactory.createDataSource(session, logicalTable, database));
                continue;
            }

            LogicalSource logicalSource = triplesMap.getLogicalSource();
            if (logicalSource != null) {
                DataSource df = DataSourceFactory.createDataSource(session, logicalSource, dataSourceDir);
                tmdfMap.put(triplesMap, df);
            }
        }

        return  tmdfMap;
    }

    private static void acquireMetadataForSubjectMap(Map<TriplesMap, DataSource> tmdfMap) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();

        for (TriplesMap triplesMap : triplesMaps) {
            DataSource df = tmdfMap.get(triplesMap);

            // SubjectMap
            SubjectMap subjectMap = triplesMap.getSubjectMap();

            List<Column> subjectColumns = new ArrayList<>();

            Optional<Template> template = subjectMap.getTemplate();
            if (template.isPresent()) {
                template.get().getLogicalReferences().forEach(subjectColumns::add);
                subjectColumns.forEach(df::acquireMinAndMaxLength);
            }

            subjectMap.getColumn().stream().forEach(subjectColumns::add);

            subjectMap.getReference().stream().forEach(subjectColumns::add);

            df.setSubjectColumns(subjectColumns);
        }
    }

    private static void acquireMetadataForPredicateObjectMap(Map<TriplesMap, DataSource> tmdfMap) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();

        for(TriplesMap triplesMap: triplesMaps) {
            DataSource df = tmdfMap.get(triplesMap);

            // PredicateObjectMap
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    // ObjectMap
                    Optional<ObjectMap> objectMap = predicateObjectPair.getObjectMap();
                    if (objectMap.isPresent()) {

                        List<Column> objectColumns = new ArrayList<>();

                        Optional<Template> template = objectMap.get().getTemplate();
                        if (template.isPresent()) {
                            template.get().getLogicalReferences().forEach(objectColumns::add);
                            objectColumns.forEach(df::acquireMinAndMaxLength);
                        }

                        Optional<Column> column = objectMap.get().getColumn();
                        if (column.isPresent()) {
                            objectColumns.add(column.get());
                            df.acquireMetadata(objectColumns.get(0));
                        }

                        Optional<Column> reference = objectMap.get().getReference();
                        if (reference.isPresent()) {
                            objectColumns.add(reference.get());
                            df.acquireMetadata(objectColumns.get(0));
                        }

                        predicateObjectPair.setMinOccurs(df.acquireMinOccurs(objectColumns));
                        predicateObjectPair.setMaxOccurs(df.acquireMaxOccurs(objectColumns));
                    }
                }
            }
        }
    }

    private static void acquireMetadataForPredicateRefObjectMap(Map<TriplesMap, DataSource> tmdfMap) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();

        for(TriplesMap triplesMap: triplesMaps) {
            DataSource df = tmdfMap.get(triplesMap);

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
                        DataSource parentDF = tmdfMap.get(parentTriplesMap);

                        List<JoinCondition> joinConditions = refObjectMap.get().getJoinConditions();

                        predicateObjectPair.setMinOccurs(df.acquireMinOccurs(parentDF, joinConditions, false));
                        predicateObjectPair.setInverseMinOccurs(df.acquireMinOccurs(parentDF, joinConditions, true));

                        predicateObjectPair.setMaxOccurs(df.acquireMaxOccurs(parentDF, joinConditions, false));
                        predicateObjectPair.setInverseMaxOccurs(df.acquireMaxOccurs(parentDF, joinConditions, true));
                    }
                }
            }
        }
    }
}
