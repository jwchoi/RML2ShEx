package shaper.mapping.model.rml;

import shaper.mapping.rml.RMLParser;

import java.net.URI;
import java.util.*;

public class RMLModelFactory {

    public static RMLModel getRMLModel(RMLParser parser) {
        RMLModel rmlModel = new RMLModel();

        // prefixes
        Map<String, String> prefixMap = parser.getPrefixes();
        Set<String> keySet = prefixMap.keySet();
        for (String key : keySet)
            rmlModel.addPrefixMap(key, prefixMap.get(key));

        // databases
        Set<String> databases = parser.getDatabases();
        for (String databaseAsResource : databases) {
            Database database = new Database(URI.create(databaseAsResource),
                    parser.getJdbcDSN(databaseAsResource),
                    parser.getJdbcDriver(databaseAsResource),
                    parser.getUsername(databaseAsResource),
                    parser.getPassword(databaseAsResource));

            rmlModel.addDatabase(database);
        }

        // triples maps
        Set<String> triplesMaps = parser.getTriplesMaps();
        for (String triplesMapAsResource : triplesMaps) {
            TriplesMap triplesMap = new TriplesMap(URI.create(triplesMapAsResource));

            // logical source or logical table

            // logical table
            String logicalTableAsResource = parser.getLogicalTable(triplesMapAsResource);
            if (logicalTableAsResource != null) {
                LogicalTable logicalTable = new LogicalTable();

                buildLogicalTable(parser, logicalTableAsResource, logicalTable);

                triplesMap.setLogicalTable(logicalTable);
            }

            // logical source
            String logicalSourceAsResource = parser.getLogicalSource(triplesMapAsResource);
            if (logicalSourceAsResource != null) {
                LogicalSource logicalSource = new LogicalSource();

                buildLogicalTable(parser, logicalSourceAsResource, logicalSource);

                // rml:source -> string or URI
                Source source = new Source(parser.getSource(logicalSourceAsResource));
                logicalSource.setSource(source);

                // rml:referenceFormulation
                logicalSource.setReferenceFormulation(parser.getReferenceFormulation(logicalSourceAsResource));

                // rml:iterator
                logicalSource.setIterator(parser.getIterator(logicalSourceAsResource));

                // rml:query
                logicalSource.setQuery(parser.getQuery(logicalSourceAsResource));

                triplesMap.setLogicalSource(logicalSource);
            }

            // subject map
            Set<URI> subjects = parser.getSubjects(triplesMapAsResource); // ?x rr:subject ?y
            Set<String> subjectMaps = parser.getSubjectMaps(triplesMapAsResource); // rr:subjectMap

            if (subjects.size() + subjectMaps.size() != 1) {
                System.err.println("A triples map must have exactly one subject map.");
                return null;
            }

            // ?x rr:subject ?y
            for (URI subject: subjects) {
                SubjectMap subjectMap = new SubjectMap();
                subjectMap.setConstant(subject);

                triplesMap.setSubjectMap(subjectMap);
            }

            // rr:subjectMap
            for (String subjectMapAsResource: subjectMaps) {

                SubjectMap subjectMap = new SubjectMap();

                buildTermMap(parser, subjectMapAsResource, subjectMap);

                // rr:class
                Set<URI> classes = parser.getClasses(subjectMapAsResource); // the size of classes could be zero.
                subjectMap.setClassIRIs(classes);

                // rr:graphMap and rr:graph
                Set<GraphMap> graphMaps = getGraphMapsAssociatedWith(subjectMapAsResource, parser);
                subjectMap.setGraphMaps(graphMaps);

                triplesMap.setSubjectMap(subjectMap);
            }

            // predicate object map
            Set<String> predicateObjectMaps = parser.getPredicateObjectMaps(triplesMapAsResource);
            for (String predicateObjectMapAsResource : predicateObjectMaps) {
                PredicateObjectMap predicateObjectMap = new PredicateObjectMap();

                // predicate or predicate map

                // ?x rr:predicate ?y.
                Set<URI> predicates = parser.getPredicates(predicateObjectMapAsResource);
                for (URI predicate : predicates) {
                    PredicateMap predicateMap = new PredicateMap();
                    predicateMap.setConstant(predicate);

                    predicateObjectMap.addPredicateMap(predicateMap);
                }

                Set<String> predicateMapsAsResource = parser.getPredicateMaps(predicateObjectMapAsResource);
                for (String predicateMapAsResource : predicateMapsAsResource) {
                    PredicateMap predicateMap = new PredicateMap();

                    buildTermMap(parser, predicateMapAsResource, predicateMap);

                    predicateObjectMap.addPredicateMap(predicateMap);
                }

                // rr:object

                // ?x rr:object ?y.
                Set<URI> IRIObjects = parser.getIRIObjects(predicateObjectMapAsResource);
                for (URI IRIObject : IRIObjects) {
                    ObjectMap objectMap = new ObjectMap();
                    objectMap.setConstant(IRIObject);

                    predicateObjectMap.addObjectMap(objectMap);
                }

                // ?x rr:object ?y.
                Set<String> literalObjects = parser.getLiteralObjects(predicateObjectMapAsResource);
                for (String literalObject : literalObjects) {
                    ObjectMap objectMap = new ObjectMap();
                    objectMap.setConstant(literalObject);

                    predicateObjectMap.addObjectMap(objectMap);
                }

                // rr:objectMap
                Set<String> objectMapsAsResource = parser.getObjectMaps(predicateObjectMapAsResource);
                for (String objectMapAsResource : objectMapsAsResource) {
                    String parentTriplesMap = parser.getParentTriplesMap(objectMapAsResource);
                    if (parentTriplesMap != null) {

                        // referencing object map
                        RefObjectMap refObjectMap = new RefObjectMap(URI.create(parentTriplesMap));

                        Set<String> joinConditions = parser.getJoinConditions(objectMapAsResource);
                        for (String joinCondition : joinConditions) {
                            String child = parser.getChild(joinCondition);
                            String parent = parser.getParent(joinCondition);

                            refObjectMap.addJoinCondition(child, parent);
                        }

                        predicateObjectMap.addRefObjectMap(refObjectMap);
                    } else {
                        // object map
                        ObjectMap objectMap = new ObjectMap();

                        buildTermMap(parser, objectMapAsResource, objectMap);

                        // rr:language
                        String language = parser.getLanguage(objectMapAsResource);
                        if (language != null) {
                            LanguageMap languageMap = new LanguageMap();
                            languageMap.setConstant(language);

                            objectMap.setLanguageMap(languageMap);
                        }

                        // rml:languageMap
                        String languageMapAsResource = parser.getLanguageMap(objectMapAsResource);
                        if (languageMapAsResource != null) {
                            LanguageMap languageMap = new LanguageMap();

                            buildTermMap(parser, languageMapAsResource, languageMap);

                            objectMap.setLanguageMap(languageMap);
                        }


                        // rr:datatype
                        URI datatype = parser.getDatatype(objectMapAsResource);
                        objectMap.setDatatype(datatype);

                        predicateObjectMap.addObjectMap(objectMap);
                    }
                }

                // rr:graphMap and rr:graph
                Set<GraphMap> graphMaps = getGraphMapsAssociatedWith(predicateObjectMapAsResource, parser);
                predicateObjectMap.setGraphMaps(graphMaps);

                triplesMap.addPredicateObjectMap(predicateObjectMap);
            }

            rmlModel.addTriplesMap(triplesMap);
        }

        return rmlModel;
    }

    private static void buildLogicalTable(RMLParser parser, String logicalTableAsResource, LogicalTable logicalTable) {
        logicalTable.setUri(URI.create(logicalTableAsResource));

        // rr:tableName
        logicalTable.setTableName(parser.getTableName(logicalTableAsResource));

        // rr:sqlVersion
        logicalTable.setSqlVersions(parser.getSQLVersions(logicalTableAsResource));

        // rr:sqlQuery
        logicalTable.setSqlQuery(parser.getSQLQuery(logicalTableAsResource));
    }

    private static Set<GraphMap> getGraphMapsAssociatedWith(String subjectMapOrPredicateObjectMapAsResource, RMLParser parser) {
        Set<GraphMap> graphMaps = new HashSet<>();

        // rr:graphMap
        Set<String> graphMapsAsResource = parser.getGraphMaps(subjectMapOrPredicateObjectMapAsResource);
        for (String graphMapAsResource : graphMapsAsResource) {
            GraphMap graphMap = new GraphMap();

            buildTermMap(parser, graphMapAsResource, graphMap);

            graphMaps.add(graphMap);
        }

        // rr:graph
        Set<URI> graphs = parser.getGraphs(subjectMapOrPredicateObjectMapAsResource);
        for (URI graph : graphs) {
            GraphMap graphMap = new GraphMap();

            graphMap.setConstant(graph);

            graphMaps.add(graphMap);
        }

        return graphMaps;
    }

    private static void buildTermMap(RMLParser parser, String termMapAsResource, TermMap termMap) {
        // rr:constant -> IRI
        URI IRIConstant = parser.getIRIConstant(termMapAsResource);
        termMap.setConstant(IRIConstant);

        // rr:constant -> Literal
        String literalConstant = parser.getLiteralConstant(termMapAsResource);
        termMap.setConstant(literalConstant);

        // rr:column
        String column = parser.getColumn(termMapAsResource);
        termMap.setColumn(column);

        // rml:reference
        String reference = parser.getReference(termMapAsResource);
        termMap.setReference(reference);

        // rr:template
        String template = parser.getTemplate(termMapAsResource);
        termMap.setTemplate(new Template(template));

        // rr:inverseExpression
        if (column != null || template != null)
            termMap.setInverseExpression(parser.getInverseExpression(termMapAsResource));

        // rr:termType
        URI termType = parser.getTermType(termMapAsResource);
        termMap.setTermType(termType);
    }
}
