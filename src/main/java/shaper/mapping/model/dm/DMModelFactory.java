package shaper.mapping.model.dm;

import janus.database.DBSchema;
import shaper.Shaper;

import java.net.URI;
import java.util.List;
import java.util.Set;

public class DMModelFactory {
	public static DMModel generateMappingModel(DBSchema dbSchema) {
		DMModel mappingMD = new DMModel(URI.create(Shaper.rdfBaseURI), dbSchema.getCatalog());
		URI baseIRI = mappingMD.getBaseIRI();
		
		Set<String> tables = dbSchema.getTableNames();
		
		for(String table : tables) {
			TableIRI tableIRIMD = new TableIRI(baseIRI, table);

            List<String> columns = dbSchema.getColumns(table);
			for(String column: columns) {
				LiteralProperty lpMD = new LiteralProperty(baseIRI, table, column);
				
				mappingMD.addLiteralProperty(lpMD);
			} // END COLUMN

            Set<String> refConstraints = dbSchema.getRefConstraints(table);
			for(String refConstraint: refConstraints) {
                ReferenceProperty rpMD = new ReferenceProperty(baseIRI, table, refConstraint);

                mappingMD.addReferenceProperty(rpMD);
            } // END REFERENTIAL CONSTRAINT

			mappingMD.addTableIRI(tableIRIMD);
		} // END TABLE
		
		return mappingMD;
	}
}
