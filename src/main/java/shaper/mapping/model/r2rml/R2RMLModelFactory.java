package shaper.mapping.model.r2rml;

import janus.database.SQLResultSet;
import janus.database.SQLSelectField;
import shaper.Shaper;
import shaper.mapping.r2rml.R2RMLParser;

import java.net.URI;
import java.util.*;

public class R2RMLModelFactory {

    public static R2RMLModel getR2RMLModel(R2RMLParser parser) {
        R2RMLModel r2rmlModel = new R2RMLModel();

        // prefixes
        Map<String, String> prefixMap = parser.getPrefixes();
        Set<String> keySet = prefixMap.keySet();
        for (String key: keySet)
            r2rmlModel.addPrefixMap(key, prefixMap.get(key));

        // triples maps
        Set<String> triplesMaps = parser.getTriplesMaps();
        for (String triplesMapAsResource: triplesMaps) {

            // logical table
            String logicalTableAsResource = parser.getLogicalTable(triplesMapAsResource);

            LogicalTable logicalTable = new LogicalTable();

            logicalTable.setUri(URI.create(logicalTableAsResource));
            logicalTable.setSqlQuery(parser.getSQLQuery(logicalTableAsResource));
            logicalTable.setTableName(parser.getTableName(logicalTableAsResource));

            Set<URI> sqlVersions = parser.getSQLVersions(logicalTableAsResource);
            for (URI sqlVersion: sqlVersions)
                logicalTable.addSqlVersion(sqlVersion);

            // subject map
            String subjectMapAsResource = parser.getSubjectMap(triplesMapAsResource);

            // rr:class
            Set<URI> classes = parser.getClasses(subjectMapAsResource); // the size of classes could be zero.

            SubjectMap subjectMap = new SubjectMap(classes);

            // ?x rr:subject ?y.
            URI constant = parser.getIRIConstant(subjectMapAsResource);
            if (constant != null)
                subjectMap.setConstant(constant.toString());

            String query = logicalTable.getSqlQuery(Shaper.DBMSType);
            SQLResultSet resultSet = Shaper.dbBridge.executeQuery(query);

            // rr:column
            String column = parser.getColumn(subjectMapAsResource);
            if (column != null)
                subjectMap.setColumn(createSQLSelectField(column, query, resultSet));

            // rr:template
            String template = parser.getTemplate(subjectMapAsResource);
            if (template != null) {
                List<SQLSelectField> columnNames = createSQLSelectFields(getColumnNamesIn(template), query, resultSet);
                subjectMap.setTemplate(new Template(template, columnNames));
            }

            // rr:inverseExpression
            subjectMap.setinverseExpression(parser.getInverseExpression(subjectMapAsResource));

            // rr:termType
            URI termType = parser.getTermType(subjectMapAsResource);
            if (termType != null)
                subjectMap.setTermType(termType);
            else
                subjectMap.setTermType(TermMap.TermTypes.IRI);

            // triples map
            TriplesMap triplesMap = new TriplesMap(URI.create(triplesMapAsResource), logicalTable, subjectMap);

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
                    ObjectMap objectMap = new ObjectMap(IRIObject.toString());
                    objectMap.setTermType(TermMap.TermTypes.IRI);

                    predicateObjectMap.addObjectMap(objectMap);
                }

                // ?x rr:object ?y.
                Set<String> literalObjects = parser.getLiteralObjects(predicateObjectMapAsResource);
                for (String literalObject: literalObjects) {
                    ObjectMap objectMap = new ObjectMap(literalObject);
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
                        ObjectMap objectMap = null;

                        // ?x rr:objectMap [ rr:constant ?y ].
                        URI IRIConstant = parser.getIRIConstant(objectMapAsResource);
                        if (IRIConstant != null) {
                            objectMap = new ObjectMap(IRIConstant.toString());
                            objectMap.setTermType(TermMap.TermTypes.IRI);
                        }

                        // ?x rr:objectMap [ rr:constant ?y ].
                        String literalConstant = parser.getLiteralConstant(objectMapAsResource);
                        if (literalConstant != null) {
                            objectMap = new ObjectMap(literalConstant);
                            objectMap.setTermType(TermMap.TermTypes.LITERAL);
                        }

                        // column
                        column = parser.getColumn(objectMapAsResource);
                        if (column != null) {
                            objectMap = new ObjectMap(createSQLSelectField(column, query, resultSet));

                            termType = parser.getTermType(objectMapAsResource);
                            if (termType != null)
                                objectMap.setTermType(termType);
                            else
                                objectMap.setTermType(TermMap.TermTypes.LITERAL);

                            // rr:inverseExpression
                            objectMap.setinverseExpression(parser.getInverseExpression(objectMapAsResource));
                        }

                        // rr:template
                        template = parser.getTemplate(objectMapAsResource);
                        if (template != null) {
                            List<SQLSelectField> columnNames = createSQLSelectFields(getColumnNamesIn(template), query, resultSet);
                            objectMap = new ObjectMap(new Template(template, columnNames));

                            termType = parser.getTermType(objectMapAsResource);
                            if (termType != null)
                                objectMap.setTermType(termType);
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

                triplesMap.addPredicateObjectMap(predicateObjectMap);
            }

            r2rmlModel.addTriplesMap(triplesMap);
        }

        return r2rmlModel;
    }

    private static List<SQLSelectField> createSQLSelectFields(List<String> selectFields, String selectQuery, SQLResultSet sqlResultSet) {
        List<SQLSelectField> selectFieldList = new ArrayList<>();
        for (String selectField: selectFields)
            selectFieldList.add(createSQLSelectField(selectField, selectQuery, sqlResultSet));

        return selectFieldList;
    }

    private static SQLSelectField createSQLSelectField(String selectField, String selectQuery, SQLResultSet sqlResultSet) {
        SQLSelectField sqlSelectField = new SQLSelectField(selectField, selectQuery);

        if (selectField.startsWith("\"") && selectField.endsWith("\""))
            selectField = selectField.substring(1, selectField.length() - 1);

        // nullable
        Optional<Integer> nullable =  sqlResultSet.isNullable(selectField);
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

    private static List<String> getColumnNamesIn(String template) {
        // because backslashes need to be escaped by a second backslash in the Turtle syntax,
        // a double backslash is needed to escape each curly brace,
        // and to get one literal backslash in the output one needs to write four backslashes in the template.
        template = template.replace("\\\\", "\\");

        List<String> columnNames = new LinkedList<>();

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

            String columnName = template.substring(openBrace + 1, closeBrace);
            columnName = columnName.replace("\\{", "{");
            columnName = columnName.replace("\\}", "}");

            columnNames.add(columnName);
            fromIndex = closeBrace + 1;
        }

        return columnNames;
    }
}
