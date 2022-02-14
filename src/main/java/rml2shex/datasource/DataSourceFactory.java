package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rml2shex.model.rml.LogicalSource;
import rml2shex.model.rml.LogicalTable;
import rml2shex.model.rml.Source;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class DataSourceFactory {
    static DataSource createDataSource(Session session, LogicalSource logicalSource, Optional<String> dataSourceDir) {
        DataSource.DataSourceKinds dataSourceKind = detectDataSourceKind(logicalSource);

        Dataset<Row> df = null;

        switch(dataSourceKind) {
            case CSV: {
                String fileName = logicalSource.getSource().getSource().toString();
                df = session.loadCSV(dataSourceDir.orElseThrow(), fileName);
                break;
            }
            case JSON: {
                String fileName = logicalSource.getSource().getSource().toString();
                String jsonPathExpression = logicalSource.getIterator();
                df = session.loadJSON(dataSourceDir.orElseThrow(), fileName, jsonPathExpression);
                return new HierarchicalDataSource(session, df, ".");
            }
            case XML: {
                String fileName = logicalSource.getSource().getSource().toString();
                String xPathExpression = logicalSource.getIterator();
                df = session.loadXML(dataSourceDir.orElseThrow(), fileName, xPathExpression);
                return new HierarchicalDataSource(session, df, "/");
            }
            case DATABASE: {
                Optional<Database> database = logicalSource.getSource().getDatabase();
                String tableName = logicalSource.getTableName();
                String query = logicalSource.getQuery();
                df = session.loadDatabase(database.orElseThrow(), tableName, query);
                break;
            }
        }

        return new DataSource(session, df);
    }

    private static DataSource.DataSourceKinds detectDataSourceKind(LogicalSource logicalSource) {
        Source source = logicalSource.getSource();
        URI referenceFormulation = logicalSource.getReferenceFormulation();
        String query = logicalSource.getQuery();
        Set<URI> sqlVersions = logicalSource.getSqlVersions();
        String tableName = logicalSource.getTableName();

        String sourceAsString = source.getSource().toString();

        if (source.getDatabase().isPresent() || sqlVersions.size() > 0 || query != null || tableName != null)
            return DataSource.DataSourceKinds.DATABASE;

        List<String> CSVExtensions = Arrays.asList(".csv", ".tsv", ".tab");
        if ((referenceFormulation != null && referenceFormulation.equals(URI.create("http://semweb.mmlab.be/ns/ql#CSV")))
                || CSVExtensions.contains(sourceAsString.substring(sourceAsString.lastIndexOf(".")).toLowerCase()))
            return DataSource.DataSourceKinds.CSV;

        if ((referenceFormulation != null && referenceFormulation.equals(URI.create("http://semweb.mmlab.be/ns/ql#JSONPath")))
                || sourceAsString.toLowerCase().endsWith(".json"))
            return DataSource.DataSourceKinds.JSON;

        if ((referenceFormulation != null && referenceFormulation.equals(URI.create("http://semweb.mmlab.be/ns/ql#XPath")))
                || sourceAsString.toLowerCase().endsWith(".xml"))
            return DataSource.DataSourceKinds.XML;

        return null;
    }

    static DataSource createDataSource(Session session, LogicalTable logicalTable, Optional<Database> database) {
        String tableName = logicalTable.getTableName();
        String sqlQuery = logicalTable.getSqlQuery();
        Dataset<Row> df = session.loadDatabase(database.orElseThrow(), tableName, sqlQuery);

        return new DataSource(session, df);
    }
}
