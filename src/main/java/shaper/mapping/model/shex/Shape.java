package shaper.mapping.model.shex;

import janus.database.SQLSelectField;
import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.r2rml.RefObjectMap;
import shaper.mapping.model.r2rml.SubjectMap;
import shaper.mapping.model.r2rml.Template;
import shaper.mapping.model.r2rml.TermMap;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class Shape implements Comparable<Shape> {
    private String id;
    private String shape;

    private String mappedTable;

    private Set<TripleConstraint> tripleConstraints;

    Shape(String mappedTable) {
        this.mappedTable = mappedTable;
        id = buildShapeID(mappedTable);

        List<String> pk = Shaper.dbSchema.getPrimaryKey(mappedTable);
        nodeKind = pk.isEmpty() ? NodeKinds.BNODE : NodeKinds.IRI;
        regex = pk.isEmpty() ? "" : buildRegex(pk);

        tripleConstraints = new CopyOnWriteArraySet<>();
    }

    String getShapeID() { return id; }

    String getMappedTableName() {
        return mappedTable;
    }

    @Override
    public int compareTo(Shape o) {
        return id.compareTo(o.getShapeID());
    }

    private String buildShapeID(String mappedTable) { return mappedTable + "Shape"; }

    private String getSpaces(int count) {
        StringBuffer spaces = new StringBuffer();
        for (int i = 0; i < count; i++)
            spaces.append(Symbols.SPACE);

        return spaces.toString();
    }

    private String buildShape() {
        String id = Shaper.shexMapper.shExSchema.getPrefix() + Symbols.COLON + this.id;

        StringBuffer shape = new StringBuffer(id + Symbols.SPACE + nodeKind);

        if (!regex.isEmpty())
            shape.append(Symbols.NEWLINE + Symbols.SPACE + regex);

        shape.append(Symbols.SPACE + Symbols.OPEN_BRACE + Symbols.NEWLINE);

        for (TripleConstraint tripleConstraint : tripleConstraints) {

            if (tripleConstraint.getMappedType().equals(TripleConstraint.MappedTypes.REF_OBJECT_MAP)) {
                Optional<String> oneOfTripleExpr = buildOneOfTripleExpr(tripleConstraint);
                if (oneOfTripleExpr.isPresent()) {
                    shape.append(oneOfTripleExpr.get() + Symbols.SPACE + Symbols.SEMICOLON + Symbols.NEWLINE);
                    continue;
                }
            }

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

    void addTripleConstraint(TripleConstraint tripleConstraint) {
        tripleConstraints.add(tripleConstraint);
    }

    @Override
    public String toString() {
        if (shape == null)
            shape = buildShape();

        return shape;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private Optional<URI> mappedTriplesMap; // mapped rr:TriplesMap for generating id

    private NodeKinds nodeKind;
    private String regex;

    private Set<Shape> baseShapes; // only for derived shape

    //-> for base shape
    //private boolean areTripleConstraintsReferenced = false;
    //private String tripleExpreLabel; // IRI, this is not null when areTripleConstraintsReferenced is true.
    //<- for base shape

    Shape(URI mappedTriplesMap, SubjectMap subjectMap) {
        this.mappedTriplesMap = Optional.of(mappedTriplesMap);

        id = buildShapeID(mappedTriplesMap);
        nodeKind = decideNodeKind(subjectMap);
        regex = buildRegex(subjectMap);

        tripleConstraints = new CopyOnWriteArraySet<>();
    }

    Shape(Set<Shape> baseShapes) {
        this.baseShapes = baseShapes;
        mappedTriplesMap = Optional.empty();

        id = buildShapeID(baseShapes);
        nodeKind = NodeKinds.IRI;
        regex = baseShapes.toArray(new Shape[0])[0].regex;

        tripleConstraints = new CopyOnWriteArraySet<>();
    }

    private Optional<String> buildOneOfTripleExpr(TripleConstraint tripleConstraint) {
        Optional<RefObjectMap> refObjectMap = tripleConstraint.getRefObjectMap();

        if (!refObjectMap.isPresent())
            return Optional.empty();

        URI parentTriplesMap = refObjectMap.get().getParentTriplesMap();
        Shape parentShape = Shaper.shexMapper.shExSchema.getMappedShape(parentTriplesMap);
        Set<Shape> derivedShapes = Shaper.shexMapper.shExSchema.getDerivedShapesFrom(Collections.singleton(parentShape));

        if (derivedShapes.size() < 1)
            return Optional.empty();

        String tcStr = tripleConstraint.toString();
        String[] tokens = tcStr.split(Symbols.SPACE);
        String property = tokens[0];
        String cardinality = tokens[2];

        Set<String> triplesConstraints = new CopyOnWriteArraySet<>();
        triplesConstraints.add(tcStr);

        String prefix = Shaper.shexMapper.shExSchema.getPrefix();
        for (Shape derivedShape: derivedShapes) {
            String shapeRef = prefix + Symbols.COLON + derivedShape.getShapeID();
            triplesConstraints.add(property + Symbols.SPACE + Symbols.AT + shapeRef + Symbols.SPACE + cardinality);
        }

        StringBuffer oneOfTripleExpr = new StringBuffer(getSpaces(2) + Symbols.OPEN_PARENTHESIS + Symbols.NEWLINE);
        for (String tc: triplesConstraints) {
            oneOfTripleExpr.append(getSpaces(5) + tc + Symbols.NEWLINE);
            oneOfTripleExpr.append(getSpaces(4) + Symbols.OR + Symbols.NEWLINE);
        }
        oneOfTripleExpr.delete(oneOfTripleExpr.lastIndexOf(getSpaces(4) + Symbols.OR + Symbols.NEWLINE), oneOfTripleExpr.length());
        oneOfTripleExpr.append(getSpaces(2) + Symbols.CLOSE_PARENTHESIS);

        return Optional.of(oneOfTripleExpr.toString());
    }

    boolean containsInBaseShapes(Shape shape) {
        if (baseShapes != null)
            return baseShapes.contains(shape);

        return false;
    }

    private NodeKinds decideNodeKind(SubjectMap subjectMap) {
        Optional<TermMap.TermTypes> termType = subjectMap.getTermType();
        if (termType.isPresent()) {
            if (termType.get().equals(TermMap.TermTypes.BLANKNODE))
                return NodeKinds.BNODE;
        }

        return NodeKinds.IRI;
    }

//    void beBaseShape() {
//        areTripleConstraintsReferenced = true;
//        tripleExpreLabel = buildTripleExpreID(mappedTriplesMap.get());
//    }
//
//    String getTripleExpreLabel() { return tripleExpreLabel; }

    private String buildShapeID(URI triplesMap) { return triplesMap.getFragment() + "Shape"; }

//    private String buildTripleExpreID(URI triplesMap) { return triplesMap.getFragment() + "Entity"; }

    private String buildShapeID(Set<Shape> baseShapes) {
        StringBuffer id = new StringBuffer();
        for (Shape baseShape: baseShapes)
            id.append(baseShape.getMappedTriplesMap().get().getFragment());
        id.append("Shape");

        return id.toString();
    }

    private String buildRegex(SubjectMap subjectMap) {
        if (nodeKind.equals(NodeKinds.BNODE))
            return null;

        Optional<Template> template = subjectMap.getTemplate();
        if (template.isPresent()) {
            if (template.get().getLengthExceptColumnName() < 1)
                return null;
        } else
            return null;

        String regex = template.get().getFormat();

        regex = regex.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        regex = regex.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        // column names
        List<SQLSelectField> columnNames = template.get().getColumnNames();
        for (SQLSelectField columnName: columnNames)
            regex = regex.replace("{" + columnName.getColumnNameOrAlias() + "}", "(.+)");

        return Symbols.SLASH + Symbols.CARET + regex + Symbols.DOLLAR + Symbols.SLASH;
    }

    Optional<URI> getMappedTriplesMap() { return mappedTriplesMap; }

    NodeKinds getNodeKind() { return nodeKind; }

    String getRegex() { return regex; }

    Set<TripleConstraint> getTripleConstraints() { return tripleConstraints; }
}
