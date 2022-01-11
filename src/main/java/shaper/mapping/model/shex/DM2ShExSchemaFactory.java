package shaper.mapping.model.shex;

import janus.database.DBColumn;
import janus.database.DBRefConstraint;
import janus.database.DBSchema;
import shaper.Shaper;

import java.net.URI;
import java.util.List;
import java.util.Set;

class DM2ShExSchemaFactory {
    //Direct Mapping
    static ShExSchema getShExSchemaModel(DBSchema dbSchema) {
        ShExSchema shExSchema = new ShExSchema(URI.create(Shaper.shapeBaseURI), Shaper.prefixForShapeBaseURI);

        Set<String> tables = dbSchema.getTableNames();

        for(String table : tables) {
            //-> node constraints
            List<String> columns = dbSchema.getColumns(table);
            for(String column: columns) {
                NodeConstraint nc = new DMNodeConstraint(buildNodeConstraintID(table, column), table, column);

                shExSchema.addNodeConstraint(nc);
            } // END COLUMN
            //<- node constraints

            //-> shape
            Shape shape = new DMShape(buildShapeID(table), table);

            // Triple Constraint From Table
            TripleConstraint tcFromTable = new DMTripleConstraint(table);
            shape.addTripleConstraint(tcFromTable);

            // Triple Constraint From Column
            for(String column: columns) {
                TripleConstraint tcFromColumn = new DMTripleConstraint(new DBColumn(table, column));

                shape.addTripleConstraint(tcFromColumn);
            } // END COLUMN

            // Triple Constraint From Referential Constraint
            Set<String> refConstraints = dbSchema.getRefConstraints(table);
            for(String refConstraint: refConstraints) {
                TripleConstraint tcFromRefConstraint = new DMTripleConstraint(new DBRefConstraint(table, refConstraint), false);

                shape.addTripleConstraint(tcFromRefConstraint);
            } // END REFERENTIAL CONSTRAINT

            // Inverse Triple Constraint From Referential Constraint
            Set<DBRefConstraint> refConstraintsPointingTo = dbSchema.getRefConstraintsPointingTo(table);
            for(DBRefConstraint refConstraint: refConstraintsPointingTo) {
                TripleConstraint tcFromRefConstraint = new DMTripleConstraint(refConstraint, true);

                shape.addTripleConstraint(tcFromRefConstraint);
            } // END Inverse Triple Constraint From Referential Constraint

            shExSchema.addShape(shape);
            //<- shape
        } // END TABLE

        return shExSchema;
    }

    private static String buildShapeID(String mappedTable) { return mappedTable + "Shape"; }

    private static String buildNodeConstraintID(String mappedTable, String mappedColumn) {
        return mappedTable + Character.toUpperCase(mappedColumn.charAt(0)) + mappedColumn.substring(1);
    }
}
