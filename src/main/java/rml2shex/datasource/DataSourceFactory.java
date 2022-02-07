package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rml2shex.model.rml.LogicalSource;
import rml2shex.model.rml.LogicalTable;
import rml2shex.model.rml.Source;

import java.io.File;
import java.net.URI;

class DataSourceFactory {
    static DataSource createDataSource(Session session, LogicalSource logicalSource, String dataSourceDir) {
        DataSource.DataSourceKinds dataSourceKind = detectDataSourceKind(logicalSource);
        String path = new File(dataSourceDir, logicalSource.getSource().getSource().toString()).getAbsolutePath();

        Dataset<Row> df = session.load(dataSourceKind, path);

        return new DataSource(session, df);
    }

    private static DataSource.DataSourceKinds detectDataSourceKind(LogicalSource logicalSource) {
        Source source = logicalSource.getSource();
        URI referenceFormulation = logicalSource.getReferenceFormulation();
        String iterator = logicalSource.getIterator();
        String query = logicalSource.getQuery();

        if ((referenceFormulation != null && referenceFormulation.equals(URI.create("http://semweb.mmlab.be/ns/ql#CSV")))
                || source.getSource().toString().toLowerCase().endsWith(".csv"))
            return DataSource.DataSourceKinds.CSV;

        return null;
    }

    static DataSource createDataSource(Session session, LogicalTable logicalTable) {
        return null;
    }
}
