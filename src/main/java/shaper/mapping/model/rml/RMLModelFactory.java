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

            // logical source
            String logicalSourceAsResource = parser.getLogicalSource(triplesMapAsResource);
            if (logicalSourceAsResource != null) {
                LogicalSource logicalSource = new LogicalSource();

                logicalSource.setUri(URI.create(logicalSourceAsResource));

                // rml:source -> string or URI
                Source source = new Source(parser.getSource(logicalSourceAsResource));
                logicalSource.setSource(source);

                // rml:referenceFormulation
                logicalSource.setReferenceFormulation(parser.getReferenceFormulation(logicalSourceAsResource));

                // rml:iterator
                logicalSource.setIterator(parser.getIterator(logicalSourceAsResource));

                // rr:tableName
                logicalSource.setTableName(parser.getTableName(logicalSourceAsResource));

                // rml:query
                logicalSource.setQuery(parser.getQuery(logicalSourceAsResource));

                // rr:sqlVersion
                logicalSource.setSqlVersions(parser.getSQLVersions(logicalSourceAsResource));

                triplesMap.setLogicalSource(logicalSource);
            }

            // logical table
            String logicalTableAsResource = parser.getLogicalTable(triplesMapAsResource);
            if (logicalTableAsResource != null) {
                LogicalTable logicalTable = new LogicalTable();

                logicalTable.setUri(URI.create(logicalTableAsResource));
                logicalTable.setTableName(parser.getTableName(logicalTableAsResource));

                triplesMap.setLogicalTable(logicalTable);
            }

            // subject map
            Set<String> subjectMaps = parser.getSubjectMaps(triplesMapAsResource);
            if (subjectMaps.size() == 1) {
                String subjectMapAsResource = subjectMaps.stream().toList().get(0);

                SubjectMap subjectMap = new SubjectMap();

                // rr:template
                String templateOfSubjectMap = parser.getTemplate(subjectMapAsResource);
                if (templateOfSubjectMap != null) {
                    subjectMap.setTemplate(new Template(templateOfSubjectMap));
                }

                // rr:termType
                URI termTypeOfSubjectMap = parser.getTermType(subjectMapAsResource);
                if (termTypeOfSubjectMap != null) {

                    if (termTypeOfSubjectMap.equals(TermMap.TermTypes.LITERAL)) {
                        System.err.println("the presence of rr:termType rr:Literal on rr:subjectMap");
                        return null;
                    }

                    subjectMap.setTermType(termTypeOfSubjectMap);
                } else
                    subjectMap.setTermType(TermMap.TermTypes.IRI);

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

                // rr:graphMap
                Set<String> graphMaps = parser.getGraphMaps(subjectMapAsResource);
                for (String graphMapAsResource: graphMaps) {
                    GraphMap graphMap = new GraphMap();

                    // rr:constant
                    URI constantOfGraphMap = parser.getIRIConstant(graphMapAsResource);
                    if (constantOfGraphMap != null)
                        graphMap.setConstant(constantOfGraphMap.toString());

                    // rr:template
                    String templateOfGraphMap = parser.getTemplate(graphMapAsResource);
                    if (templateOfGraphMap != null) {
                        graphMap.setTemplate(new Template(templateOfGraphMap));
                    }

                    subjectMap.addGraphMap(graphMap);
                }

                // rr:graph
                Set<URI> graphs = parser.getGraphs(subjectMapAsResource);
                for (URI graph: graphs) {
                    GraphMap graphMap = new GraphMap();
                    graphMap.setConstant(graph.toString());

                    subjectMap.addGraphMap(graphMap);
                }

                triplesMap.setSubjectMap(subjectMap);

            } else {
                System.err.println("A triples map must have exactly one subject map.");
                return null;
            }

            // predicate object map
            Set<String> predicateObjectMaps = parser.getPredicateObjectMaps(triplesMapAsResource);
            for (String predicateObjectMapAsResource: predicateObjectMaps) {
                PredicateObjectMap predicateObjectMap = new PredicateObjectMap();

                // predicate or predicate map
                // ?x rr:predicate ?y.
                Set<URI> predicates = parser.getPredicates(predicateObjectMapAsResource);

                // ?x rr:predicateMap [ rr:constant ?y ].
                Set<String> predicateMapsAsResource = parser.getPredicateMaps(predicateObjectMapAsResource);
                for (String predicateMapAsResource: predicateMapsAsResource)
                    predicates.add(parser.getIRIConstant(predicateMapAsResource));

                Set<PredicateMap> predicateMaps = new TreeSet<>();
                for (URI predicate: predicates) {
                    PredicateMap predicateMap = new PredicateMap(predicate.toString());
                    predicateMap.setTermType(TermMap.TermTypes.IRI);
                    predicateMaps.add(predicateMap);
                }

                predicateObjectMap.setPredicateMaps(predicateMaps);

                // object

                // ?x rr:object ?y.
                Set<URI> IRIObjects = parser.getIRIObjects(predicateObjectMapAsResource);
                for (URI IRIObject: IRIObjects) {
                    ObjectMap objectMap = new ObjectMap();
                    objectMap.setConstant(IRIObject.toString());
                    objectMap.setTermType(TermMap.TermTypes.IRI);

                    predicateObjectMap.addObjectMap(objectMap);
                }

                // ?x rr:object ?y.
                Set<String> literalObjects = parser.getLiteralObjects(predicateObjectMapAsResource);
                for (String literalObject: literalObjects) {
                    ObjectMap objectMap = new ObjectMap();
                    objectMap.setConstant(literalObject);
                    objectMap.setTermType(TermMap.TermTypes.LITERAL);

                    predicateObjectMap.addObjectMap(objectMap);
                }

                // rr:objectMap
                Set<String> objectMapsAsResource = parser.getObjectMaps(predicateObjectMapAsResource);
                for (String objectMapAsResource: objectMapsAsResource) {
                    String parentTriplesMap = parser.getParentTriplesMap(objectMapAsResource);
                    if (parentTriplesMap != null) {

                        // referencing object map
                        RefObjectMap refObjectMap = new RefObjectMap(URI.create(parentTriplesMap));

                        Set<String> joinConditions = parser.getJoinConditions(objectMapAsResource);
                        for (String joinCondition: joinConditions) {
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

                // rr:graphMap
                Set<String> graphMaps = parser.getGraphMaps(predicateObjectMapAsResource);
                for (String graphMapAsResource: graphMaps) {
                    GraphMap graphMap = new GraphMap();

                    // rr:constant
                    URI constantOfGraphMap = parser.getIRIConstant(graphMapAsResource);
                    if (constantOfGraphMap != null)
                        graphMap.setConstant(constantOfGraphMap.toString());

                    // rr:template
                    String templateOfGraphMap = parser.getTemplate(graphMapAsResource);
                    if (templateOfGraphMap != null) {
                        graphMap.setTemplate(new Template(templateOfGraphMap));
                    }

                    predicateObjectMap.addGraphMap(graphMap);
                }

                // rr:graph
                Set<URI> graphs = parser.getGraphs(predicateObjectMapAsResource);
                for (URI graph: graphs) {
                    GraphMap graphMap = new GraphMap();
                    graphMap.setConstant(graph.toString());

                    predicateObjectMap.addGraphMap(graphMap);
                }

                triplesMap.addPredicateObjectMap(predicateObjectMap);
            }

            rmlModel.addTriplesMap(triplesMap);
        }

        return rmlModel;
    }

    private static List<SQLSelectField> createSQLSelectFields(List<String> selectFields, String selectQuery, SQLResultSet sqlResultSet) {
        List<SQLSelectField> selectFieldList = new ArrayList<>();
        for (String selectField : selectFields)
            selectFieldList.add(createSQLSelectField(selectField, selectQuery, sqlResultSet));

        return selectFieldList;
    }

    private static SQLSelectField createSQLSelectField(String selectField, String selectQuery, SQLResultSet sqlResultSet) {
        SQLSelectField sqlSelectField = new SQLSelectField(selectField, selectQuery);

        if (selectField.startsWith("\"") && selectField.endsWith("\""))
            selectField = selectField.substring(1, selectField.length() - 1);

        // nullable
        Optional<Integer> nullable = sqlResultSet.isNullable(selectField);
        if (nullable.isPresent())
            sqlSelectField.setNullable(nullable.get());

        // sql type
        Optional<Integer> columnType = sqlResultSet.getColumnType(selectField);
        if (columnType.isPresent())
            sqlSelectField.setSqlType(columnType.get());

        // display size
        sqlSelectField.setDisplaySize(sqlResultSet.getColumnDisplaySize(selectField));

        return sqlSelectField;
    }

    private static List<String> getLogicalReferencesIn(String template) {
        // because backslashes need to be escaped by a second backslash in the Turtle syntax,
        // a double backslash is needed to escape each curly brace,
        // and to get one literal backslash in the output one needs to write four backslashes in the template.
        template = template.replace("\\\\", "\\");

        List<String> logicalReferences = new LinkedList<>();

        int length = template.length();
        int fromIndex = 0;
        while (fromIndex < length) {
            int openBrace = template.indexOf("{", fromIndex);
            if (openBrace == -1) break;
            while (openBrace > 0 && template.charAt(openBrace - 1) == '\\')
                openBrace = template.indexOf("{", openBrace + 1);

            int closeBrace = template.indexOf("}", fromIndex);
            if (closeBrace == -1) break;
            while (closeBrace > 0 && template.charAt(closeBrace - 1) == '\\')
                closeBrace = template.indexOf("}", closeBrace + 1);

            String reference = template.substring(openBrace + 1, closeBrace);
            reference = reference.replace("\\{", "{");
            reference = reference.replace("\\}", "}");

            logicalReferences.add(reference);
            fromIndex = closeBrace + 1;
        }

        return logicalReferences;
    }
}
