package rml2shex.datasource;

import rml2shex.model.rml.LogicalSource;
import rml2shex.model.rml.LogicalTable;
import rml2shex.model.rml.RMLModel;
import rml2shex.model.rml.TriplesMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataSourceAnalyzer {
    public static void injectMetadataInto(RMLModel rmlModel, String dataSourceDir) {

        Session session = Session.createSession();

        Map<TriplesMap, DataFrame> tmdfMap = new HashMap<>();

        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

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

        for(TriplesMap triplesMap: triplesMaps) {
            tmdfMap.get(triplesMap).show();
        }
    }
}
