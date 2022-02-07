package rml2shex.datasource;

import rml2shex.model.rml.*;

import java.net.URI;
import java.util.*;

public class DataSourceExplorer {
    public static void injectMetadataInto(RMLModel rmlModel, String dataSourceDir) {

        Session session = Session.createSession();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        Map<TriplesMap, DataSource> tmdfMap = loadLogicalSources(triplesMaps, dataSourceDir, session);

        acquireMetadata(tmdfMap, session);
    }

    private static Map<TriplesMap, DataSource> loadLogicalSources(Set<TriplesMap> triplesMaps, String dataSourceDir, Session session) {
        Map<TriplesMap, DataSource> tmdfMap = new HashMap<>();

        for (TriplesMap triplesMap: triplesMaps) {
            LogicalTable logicalTable = triplesMap.getLogicalTable();
            if (logicalTable != null) {
                tmdfMap.put(triplesMap, DataSourceFactory.createDataSource(session, logicalTable));
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

    private static void acquireMetadata(Map<TriplesMap, DataSource> tmdfMap, Session session) {
        Set<TriplesMap> triplesMaps = tmdfMap.keySet();

        for(TriplesMap triplesMap: triplesMaps) {
            DataSource df = tmdfMap.get(triplesMap);

            // SubjectMap
            SubjectMap subjectMap = triplesMap.getSubjectMap();

            List<Column> subjectColumns = new ArrayList<>();

            Optional<Template> template = subjectMap.getTemplate();
            if (template.isPresent()) {
                template.get().getLogicalReferences().forEach(subjectColumns::add);
                subjectColumns.forEach(df::acquireMetadata);
            }

            Optional<Column> column = subjectMap.getColumn();
            if (column.isPresent()) {
                subjectColumns.add(column.get());
                df.acquireMetadata(subjectColumns.get(0));
            }

            Optional<Column> reference = subjectMap.getReference();
            if (reference.isPresent()) {
                subjectColumns.add(reference.get());
                df.acquireMetadata(subjectColumns.get(0));
            }

            df.setSubjectColumns(subjectColumns);

            // PredicateObjectMap
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    // ObjectMap
                    Optional<ObjectMap> objectMap = predicateObjectPair.getObjectMap();
                    if (objectMap.isPresent()) {

                        List<Column> objectColumns = new ArrayList<>();

                        template = objectMap.get().getTemplate();
                        if (template.isPresent()) {
                            template.get().getLogicalReferences().forEach(objectColumns::add);
                            objectColumns.forEach(df::acquireMetadata);
                        }

                        column = objectMap.get().getColumn();
                        if (column.isPresent()) {
                            objectColumns.add(column.get());
                            df.acquireMetadata(objectColumns.get(0));
                        }

                        reference = objectMap.get().getReference();
                        if (reference.isPresent()) {
                            objectColumns.add(reference.get());
                            df.acquireMetadata(objectColumns.get(0));
                        }

                        predicateObjectPair.setMaxOccurs(df.acquireMaxOccurs(objectColumns));
                    }
                }
            }
        }

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
                        for (JoinCondition joinCondition: joinConditions) {
                            Column childColumn = joinCondition.getChild();
                            df.acquireMetadata(childColumn);
                            Column parentColumn = joinCondition.getParent();
                            parentDF.acquireMetadata(parentColumn);
                        }

                        predicateObjectPair.setMaxOccurs(df.acquireMinOccurs(parentDF, joinConditions, false));

                        predicateObjectPair.setMaxOccurs(df.acquireMaxOccurs(parentDF, joinConditions, false));
                        predicateObjectPair.setInverseMaxOccurs(df.acquireMaxOccurs(parentDF, joinConditions, true));
                    }
                }
            }
        }
    }
}
