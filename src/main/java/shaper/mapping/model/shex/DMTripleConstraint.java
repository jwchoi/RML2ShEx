package shaper.mapping.model.shex;

import janus.database.DBColumn;
import janus.database.DBRefConstraint;
import shaper.Shaper;
import shaper.mapping.Symbols;

import java.util.List;

public class DMTripleConstraint extends TripleConstraint {
    private String mappedTable;
    private DBColumn mappedColumn;
    private DBRefConstraint mappedRefConstraint;

    DMTripleConstraint(String mappedTable) {
        super(MappedTypes.TABLE);
        this.mappedTable = mappedTable;
    }

    DMTripleConstraint(DBColumn mappedColumn) {
        super(MappedTypes.COLUMN);
        this.mappedColumn = mappedColumn;
    }

    DMTripleConstraint(DBRefConstraint mappedRefConstraint, boolean isInverse) {
        super(MappedTypes.REF_CONSTRAINT);
        this.mappedRefConstraint = mappedRefConstraint;
        setIsInverse(isInverse);
    }

    private String buildTripleConstraint() {
        String tripleConstraint = null;
        switch (getMappedType()) {
            case COLUMN:
                tripleConstraint = buildTripleConstraintFromColumn();
                break;
            case REF_CONSTRAINT:
                tripleConstraint = isInverse().get() ? buildInverseTripleConstraintFromRefConstraint() : buildTripleConstraintFromRefConstraint();
                break;
            case TABLE:
                tripleConstraint = buildTripleConstraintFromTable();
                break;
        }
        return tripleConstraint;
    }

    private String buildTripleConstraintFromColumn() {
        String table = mappedColumn.getTableName();
        String column = mappedColumn.getColumnName();
        String litProperty = Symbols.LT + Shaper.shexMapper.dmModel.getMappedLiteralProperty(table, column) + Symbols.GT;

        String prefix = Shaper.shexMapper.shExSchema.getBasePrefix();
        String nodeConstraint = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedNodeConstraintID(table, column);

        boolean nullable = false;
        if (!Shaper.dbSchema.isNotNull(table, column))
            nullable = true;

        setCardinality(nullable ? "?" : "");

        return litProperty + Symbols.SPACE + Symbols.AT + nodeConstraint + getCardinality();
    }

    private String buildInverseTripleConstraintFromRefConstraint() {
        String table = mappedRefConstraint.getTableName();
        String refConstraint = mappedRefConstraint.getRefConstraintName();
        String refProperty = Symbols.LT + Shaper.shexMapper.dmModel.getMappedReferenceProperty(table, refConstraint) + Symbols.GT;

        String prefix = Shaper.shexMapper.shExSchema.getBasePrefix();
        String referencedShape = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedShapeID(table);

        setCardinality(Symbols.ASTERISK);

        return Symbols.CARET + refProperty + Symbols.SPACE + Symbols.AT + referencedShape + getCardinality();
    }

    private String buildTripleConstraintFromRefConstraint() {
        String table = mappedRefConstraint.getTableName();
        String refConstraint = mappedRefConstraint.getRefConstraintName();
        String refProperty = Symbols.LT + Shaper.shexMapper.dmModel.getMappedReferenceProperty(table, refConstraint) + Symbols.GT;

        String referencedTable = Shaper.dbSchema.getReferencedTableBy(table, refConstraint);
        String prefix = Shaper.shexMapper.shExSchema.getBasePrefix();
        String referencedShape = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedShapeID(referencedTable);

        List<String> columns = Shaper.dbSchema.getReferencingColumnsByOrdinalPosition(table, refConstraint);
        boolean nullable = false;
        for (String column: columns) {
            if (!Shaper.dbSchema.isNotNull(mappedTable, column)) {
                nullable = true;
                break;
            }
        }
        setCardinality(nullable ? "?" : "");

        return refProperty + Symbols.SPACE + Symbols.AT + referencedShape + getCardinality();
    }

    private String buildTripleConstraintFromTable() {
        String rdfType = Symbols.A;

        String tableIRI = Symbols.LT + Shaper.shexMapper.dmModel.getMappedTableIRILocalPart(mappedTable) + Symbols.GT;

        setCardinality("");

        return rdfType + Symbols.SPACE + Symbols.OPEN_BRACKET + tableIRI + Symbols.CLOSE_BRACKET + getCardinality();
    }

    @Override
    public String toString() {
        if (getSerializedTripleConstraint() == null)
            setSerializedTripleConstraint(buildTripleConstraint());

        return getSerializedTripleConstraint();
    }
}
