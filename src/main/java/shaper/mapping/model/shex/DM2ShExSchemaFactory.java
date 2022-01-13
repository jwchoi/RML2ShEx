package shaper.mapping.model.shex;

import janus.database.DBColumn;
import janus.database.DBRefConstraint;
import janus.database.DBSchema;
import shaper.Shaper;
import shaper.mapping.model.ID;

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
                ID ncID = buildNodeConstraintID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), table, column);
                NodeConstraint nc = new DMNodeConstraint(ncID, table, column);

                shExSchema.addShapeExpr(nc);
            } // END COLUMN
            //<- node constraints

            //-> shape
            ID shapeID = buildShapeID(shExSchema.getBasePrefix(), shExSchema.getBaseIRI(), table);
            Shape shape = new DMShape(shapeID, table);

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

            shExSchema.addShapeExpr(shape);
            //<- shape
        } // END TABLE

        return shExSchema;
    }

    private static ID buildShapeID(String prefixLabel, URI prefixIRI, String mappedTable) {
        String localPart = mappedTable + "Shape";
        return new ID(prefixLabel, prefixIRI, localPart);
    }

    private static ID buildNodeConstraintID(String prefixLabel, URI prefixIRI, String mappedTable, String mappedColumn) {
        String localPart = mappedTable + Character.toUpperCase(mappedColumn.charAt(0)) + mappedColumn.substring(1);
        return new ID(prefixLabel, prefixIRI, localPart);
    }
}
