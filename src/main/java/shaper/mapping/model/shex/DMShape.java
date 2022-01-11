package shaper.mapping.model.shex;

import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.ID;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DMShape extends Shape {
    private String mappedTable;

    private NodeKinds nodeKind;
    private String regex;

    DMShape(ID id, String mappedTable) {
        super(id);

        this.mappedTable = mappedTable;

        List<String> pk = Shaper.dbSchema.getPrimaryKey(mappedTable);
        nodeKind = pk.isEmpty() ? NodeKinds.BNODE : NodeKinds.IRI;
        regex = pk.isEmpty() ? "" : buildRegex(pk);
    }

    String getMappedTableName() {
        return mappedTable;
    }

    private String buildRegex(List<String> pk) {
        String regex = Shaper.rdfBaseURI;
        regex = regex.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        regex = regex.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        regex = regex.concat(mappedTable + Symbols.BACKSLASH + Symbols.SLASH);

        for (String column: pk) {
            regex = regex.concat(column + Symbols.EQUAL);

            // value
            regex = regex.concat("(.*)");

            regex = regex.concat(Symbols.SEMICOLON);
        }
        regex = regex.substring(0, regex.length()-1);

        return Symbols.SLASH + Symbols.CARET + regex + Symbols.DOLLAR + Symbols.SLASH;
    }

    private String buildShape() {
        String id = getID().getPrefixedName();

        StringBuffer shape = new StringBuffer(id + Symbols.SPACE + nodeKind);

        if (!regex.isEmpty())
            shape.append(Symbols.NEWLINE + Symbols.SPACE + regex);

        shape.append(Symbols.SPACE + Symbols.OPEN_BRACE + Symbols.NEWLINE);

        Set<TripleConstraint> tripleConstraints = getTripleConstraints();
        for (TripleConstraint tripleConstraint : tripleConstraints) {
            Optional<Boolean> isInverse = tripleConstraint.isInverse();
            if (isInverse.isPresent() && isInverse.get())
                shape.append(getSpaces(1));
            else
                shape.append(getSpaces(2));

            shape.append(tripleConstraint + Symbols.SPACE + Symbols.SEMICOLON + Symbols.NEWLINE);
        }
        shape.deleteCharAt(shape.lastIndexOf(Symbols.SEMICOLON));
        shape.append(Symbols.CLOSE_BRACE);

        return shape.toString();
    }

    private String getSpaces(int count) {
        StringBuffer spaces = new StringBuffer();
        for (int i = 0; i < count; i++)
            spaces.append(Symbols.SPACE);

        return spaces.toString();
    }

    @Override
    public String toString() {
        String serializedShape = getSerializedShape();
        if (serializedShape == null) {
            serializedShape = buildShape();
            setSerializedShape(serializedShape);
        }

        return serializedShape;
    }
}
