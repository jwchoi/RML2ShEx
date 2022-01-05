package shaper.mapping.model.rml;

import janus.database.SQLResultSet;
import janus.database.SQLSelectField;
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
            Set<String> subjectMaps = parser.getSubjectMaps(triplesMapAsResource);
            if (subjectMaps.size() == 1) {
                String subjectMapAsResource = subjectMaps.stream().toList().get(0);

                SubjectMap subjectMap = new SubjectMap();

                buildTermMap(parser, subjectMapAsResource, subjectMap);

                // rr:termType <- to check error
                if (subjectMap.getTermType().equals(TermMap.TermTypes.LITERAL)) {
                    System.err.println("the presence of rr:termType rr:Literal on rr:subjectMap");
                    return null;
                }

                // rr:class
                Set<URI> classes = parser.getClasses(subjectMapAsResource); // the size of classes could be zero.
                subjectMap.setClassIRIs(classes);

                // rml:reference
                String referenceOfSubjectMap = parser.getReference(subjectMapAsResource);
                if (referenceOfSubjectMap != null)
                    subjectMap.setReference(referenceOfSubjectMap);

                // rr:subject
                URI constantOfSubjectMap = parser.getIRIConstant(subjectMapAsResource);
                if (constantOfSubjectMap != null)
                    subjectMap.setConstant(constantOfSubjectMap.toString());

                // rr:graphMap and rr:graph
                Set<GraphMap> graphMaps = getGraphMapsAssociatedWith(subjectMapAsResource, parser);
                subjectMap.setGraphMaps(graphMaps);

                triplesMap.setSubjectMap(subjectMap);

            } else {
                System.err.println("A triples map must have exactly one subject map.");
                return null;
            }

            // predicate object map
            Set<String> predicateObjectMaps = parser.getPredicateObjectMaps(triplesMapAsResource);
            for (String predicateObjectMapAsResource : predicateObjectMaps) {
                PredicateObjectMap predicateObjectMap = new PredicateObjectMap();

                // predicate or predicate map
                // ?x rr:predicate ?y.
                Set<URI> predicates = parser.getPredicates(predicateObjectMapAsResource);

                // ?x rr:predicateMap [ rr:constant ?y ].
                Set<String> predicateMapsAsResource = parser.getPredicateMaps(predicateObjectMapAsResource);
                for (String predicateMapAsResource : predicateMapsAsResource)
                    predicates.add(parser.getIRIConstant(predicateMapAsResource));

                Set<PredicateMap> predicateMaps = new TreeSet<>();
                for (URI predicate : predicates) {
                    PredicateMap predicateMap = new PredicateMap(predicate);
                    predicateMaps.add(predicateMap);
                }

                predicateObjectMap.setPredicateMaps(predicateMaps);

                // object

                // ?x rr:object ?y.
                Set<URI> IRIObjects = parser.getIRIObjects(predicateObjectMapAsResource);
                for (URI IRIObject : IRIObjects) {
                    ObjectMap objectMap = new ObjectMap();
                    objectMap.setConstant(IRIObject.toString());
                    objectMap.setTermType(TermMap.TermTypes.IRI);

                    predicateObjectMap.addObjectMap(objectMap);
                }

                // ?x rr:object ?y.
                Set<String> literalObjects = parser.getLiteralObjects(predicateObjectMapAsResource);
                for (String literalObject : literalObjects) {
                    ObjectMap objectMap = new ObjectMap();
                    objectMap.setConstant(literalObject);
                    objectMap.setTermType(TermMap.TermTypes.LITERAL);

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

                        // ?x rr:objectMap [ rr:constant ?y ].
                        URI IRIConstant = parser.getIRIConstant(objectMapAsResource);
                        if (IRIConstant != null) {
                            objectMap.setConstant(IRIConstant.toString());
                            objectMap.setTermType(TermMap.TermTypes.IRI);
                        }

                        // ?x rr:objectMap [ rr:constant ?y ].
                        String literalConstant = parser.getLiteralConstant(objectMapAsResource);
                        if (literalConstant != null) {
                            objectMap.setConstant(literalConstant);
                            objectMap.setTermType(TermMap.TermTypes.LITERAL);
                        }

                        // rml:reference
                        String referenceOfObjectMap = parser.getReference(objectMapAsResource);
                        if (referenceOfObjectMap != null) {
                            URI termTypeOfObjectMap = parser.getTermType(objectMapAsResource);
                            if (termTypeOfObjectMap != null)
                                objectMap.setTermType(termTypeOfObjectMap);
                            else
                                objectMap.setTermType(TermMap.TermTypes.LITERAL);

                            // rr:inverseExpression
                            objectMap.setinverseExpression(parser.getInverseExpression(objectMapAsResource));
                        }

                        // rr:template
                        String templateOfObjectMap = parser.getTemplate(objectMapAsResource);
                        if (templateOfObjectMap != null) {
                            objectMap.setTemplate(new Template(templateOfObjectMap));

                            URI termTypeOfObjectMap = parser.getTermType(objectMapAsResource);
                            if (termTypeOfObjectMap != null)
                                objectMap.setTermType(termTypeOfObjectMap);
                            else
                                objectMap.setTermType(TermMap.TermTypes.IRI);

                            // rr:inverseExpression
                            objectMap.setinverseExpression(parser.getInverseExpression(objectMapAsResource));
                        }

                        // rr:language
                        String language = parser.getLanguage(objectMapAsResource);
                        if (language != null) {
                            objectMap.setLanguage(language);
                            objectMap.setTermType(TermMap.TermTypes.LITERAL);
                        }

                        // rr:datatype
                        URI datatype = parser.getDatatype(objectMapAsResource);
                        if (datatype != null) {
                            objectMap.setDatatype(datatype);
                            objectMap.setTermType(TermMap.TermTypes.LITERAL);
                        }

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

            // rr:constant
            URI constantOfGraphMap = parser.getIRIConstant(graphMapAsResource);
            if (constantOfGraphMap != null)
                graphMap.setConstant(constantOfGraphMap.toString());

            buildTermMap(parser, graphMapAsResource, graphMap);

            graphMaps.add(graphMap);
        }

        // rr:graph
        Set<URI> graphs = parser.getGraphs(subjectMapOrPredicateObjectMapAsResource);
        for (URI graph : graphs) {
            GraphMap graphMap = new GraphMap();

            graphMap.setConstant(graph.toString());

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

        // rr:template
        String template = parser.getTemplate(termMapAsResource);
        termMap.setTemplate(new Template(template));

        // rr:termType
        URI termType = parser.getTermType(termMapAsResource);
        termMap.setTermType(termType);
    }
}
