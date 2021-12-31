package shaper.mapping.model.shex;

import janus.database.DBColumn;
import janus.database.DBRefConstraint;
import janus.database.DBSchema;
import shaper.Shaper;

import java.net.URI;
import java.util.List;
import java.util.Set;

class DirectMapping2ShExSchemaFactory {
    //Direct Mapping
    static ShExSchema getShExSchemaModel(DBSchema dbSchema) {
        ShExSchema schemaMD = new ShExSchema(URI.create(Shaper.shapeBaseURI), Shaper.prefixForShapeBaseURI);

        Set<String> tables = dbSchema.getTableNames();

        for(String table : tables) {
            //-> node constraints
            List<String> columns = dbSchema.getColumns(table);
            for(String column: columns) {
                NodeConstraint ncMD = new NodeConstraint(table, column);

                schemaMD.addNodeConstraint(ncMD);
            } // END COLUMN
            //<- node constraints

            //-> shape
            Shape shapeMD = new Shape(table);

            // Triple Constraint From Table
            TripleConstraint tcFromTable = new TripleConstraint(table);
            shapeMD.addTripleConstraint(tcFromTable);

            // Triple Constraint From Column
            for(String column: columns) {
                TripleConstraint tcFromColumn = new TripleConstraint(new DBColumn(table, column));

                shapeMD.addTripleConstraint(tcFromColumn);
            } // END COLUMN

            // Triple Constraint From Referential Constraint
            Set<String> refConstraints = dbSchema.getRefConstraints(table);
            for(String refConstraint: refConstraints) {
                TripleConstraint tcFromRefConstraint = new TripleConstraint(new DBRefConstraint(table, refConstraint), false);

                shapeMD.addTripleConstraint(tcFromRefConstraint);
            } // END REFERENTIAL CONSTRAINT

            // Inverse Triple Constraint From Referential Constraint
            Set<DBRefConstraint> refConstraintsPointingTo = dbSchema.getRefConstraintsPointingTo(table);
            for(DBRefConstraint refConstraint: refConstraintsPointingTo) {
                TripleConstraint tcFromRefConstraint = new TripleConstraint(refConstraint, true);

                shapeMD.addTripleConstraint(tcFromRefConstraint);
            } // END Inverse Triple Constraint From Referential Constraint

            schemaMD.addShape(shapeMD);
            //<- shape
        } // END TABLE

        return schemaMD;
    }
}
