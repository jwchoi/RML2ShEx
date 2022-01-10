package shaper.mapping.model.shex;

import janus.database.DBColumn;
import janus.database.DBRefConstraint;
import janus.database.DBSchema;
import shaper.Shaper;
import shaper.mapping.model.dm.DMModel;

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
                NodeConstraint nc = new NodeConstraint(table, column);

                shExSchema.addNodeConstraint(nc);
            } // END COLUMN
            //<- node constraints

            //-> shape
            Shape shape = new Shape(buildShapeID(table), table);

            // Triple Constraint From Table
            TripleConstraint tcFromTable = new TripleConstraint(table);
            shape.addTripleConstraint(tcFromTable);

            // Triple Constraint From Column
            for(String column: columns) {
                TripleConstraint tcFromColumn = new TripleConstraint(new DBColumn(table, column));

                shape.addTripleConstraint(tcFromColumn);
            } // END COLUMN

            // Triple Constraint From Referential Constraint
            Set<String> refConstraints = dbSchema.getRefConstraints(table);
            for(String refConstraint: refConstraints) {
                TripleConstraint tcFromRefConstraint = new TripleConstraint(new DBRefConstraint(table, refConstraint), false);

                shape.addTripleConstraint(tcFromRefConstraint);
            } // END REFERENTIAL CONSTRAINT

            // Inverse Triple Constraint From Referential Constraint
            Set<DBRefConstraint> refConstraintsPointingTo = dbSchema.getRefConstraintsPointingTo(table);
            for(DBRefConstraint refConstraint: refConstraintsPointingTo) {
                TripleConstraint tcFromRefConstraint = new TripleConstraint(refConstraint, true);

                shape.addTripleConstraint(tcFromRefConstraint);
            } // END Inverse Triple Constraint From Referential Constraint

            shExSchema.addShape(shape);
            //<- shape
        } // END TABLE

        return shExSchema;
    }

    private static String buildShapeID(String mappedTable) { return mappedTable + "Shape"; }
}
