package shaper.mapping.model.shex;

import shaper.mapping.model.ID;
import shaper.mapping.model.r2rml.ObjectMap;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class ShExSchema {
    private URI baseIRI;
    private String basePrefix;

    private Map<URI, String> prefixMap;

    private Set<ShapeExpr> shapes;

    ShExSchema(URI baseIRI, String basePrefix) {
        this.baseIRI = baseIRI;
        this.basePrefix = basePrefix;

        prefixMap = new TreeMap<>();
        prefixMap.put(baseIRI, basePrefix); // prefix newly created by base

        shapes = new HashSet<>();
    }

    void addPrefixDecl(String prefix, String IRIString) {
        prefixMap.put(URI.create(IRIString), prefix);
    }

    void addShapeExpr(ShapeExpr shapeExpr) { shapes.add(shapeExpr); }

    public ID getMappedShapeID(String table) {
        Optional<Shape> mappedShape = shapes.stream()
                .filter(se -> se instanceof DMShape)
                .filter(se -> ((DMShape) se).getMappedTableName().equals(table))
                .map(se -> (Shape) se)
                .findAny();

        return mappedShape.isEmpty() ? null : mappedShape.get().getID();
    }

    public String getMappedShape(String table) {
        Optional<ShapeExpr> mappedShape = shapes.stream()
                .filter(shape -> shape instanceof DMShape)
                .filter(shape -> ((DMShape) shape).getMappedTableName().equals(table))
                .findAny();

        return mappedShape.isEmpty() ? null : mappedShape.get().toString();
    }

    public ID getMappedNodeConstraintID(String table, String column) {
        Optional<DMNodeConstraint> mappedNodeConstraint = shapes.stream()
                .filter(nc -> nc instanceof DMNodeConstraint)
                .map(nc -> (DMNodeConstraint) nc)
                .filter(nc -> nc.getMappedTable().equals(table) && nc.getMappedColumn().equals(column))
                .findAny();

        return mappedNodeConstraint.isPresent() ? mappedNodeConstraint.get().getID() : null;
    }

    public String getMappedNodeConstraint(String table, String column) {
        Optional<DMNodeConstraint> mappedNodeConstraint = shapes.stream()
                .filter(nc -> nc instanceof DMNodeConstraint)
                .map(nc -> (DMNodeConstraint) nc)
                .filter(nc -> nc.getMappedTable().equals(table) && nc.getMappedColumn().equals(column))
                .findAny();

        return mappedNodeConstraint.isPresent() ? mappedNodeConstraint.get().toString() : null;
    }

    public String getMappedNodeConstraint(ObjectMap objectMap) {
        Optional<R2RMLNodeConstraint> nodeConstraint = shapes.stream()
                .filter(nc -> nc instanceof R2RMLNodeConstraint)
                .map(nc -> (R2RMLNodeConstraint) nc)
                .filter(nc -> nc.getMappedObjectMap().equals(objectMap))
                .findAny();

        return nodeConstraint.isPresent() ? nodeConstraint.get().toString() : null;
    }

    public ID getMappedNodeConstraintID(ObjectMap objectMap) {
        Optional<R2RMLNodeConstraint> nodeConstraint = shapes.stream()
                .filter(nc -> nc instanceof R2RMLNodeConstraint)
                .map(nc -> (R2RMLNodeConstraint) nc)
                .filter(nc -> nc.getMappedObjectMap().equals(objectMap))
                .findAny();

        return nodeConstraint.isPresent() ? nodeConstraint.get().getID() : null;
    }

    public URI getBaseIRI() {
        return baseIRI;
    }
    public String getBasePrefix() { return basePrefix; }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Shape getMappedShape(URI triplesMap) {
        Optional<R2RMLShape> mappedShape = shapes.stream()
                .filter(se -> se instanceof R2RMLShape)
                .map(se -> (R2RMLShape) se)
                .filter(shape -> shape.getMappedTriplesMap().isPresent())
                .filter(shape -> shape.getMappedTriplesMap().get().equals(triplesMap))
                .findAny();

        return mappedShape.isEmpty() ? null : mappedShape.get();
    }

    Set<Shape> getShapesToShareTheSameSubjects(Shape shape) {
        if (!(shape instanceof R2RMLShape)) return Collections.EMPTY_SET;

        R2RMLShape r2RMLShape = (R2RMLShape) shape;

        if (!r2RMLShape.getNodeKind().equals(NodeKinds.IRI))
            return Collections.EMPTY_SET;

        return shapes.stream()
                .filter(e -> e instanceof R2RMLShape)
                .filter(e -> ((R2RMLShape) e).getNodeKind().equals(NodeKinds.IRI)
                        && ((R2RMLShape) e).getRegex().equals(r2RMLShape.getRegex()))
                .map(e -> (Shape) e)
                .collect(Collectors.toSet());
    }

    static Set<Set<Shape>> createSetsForDerivedShapes(Set<Shape> baseShapes) {
        List<Shape> baseShapeList = new ArrayList<>(baseShapes);
        int size = baseShapeList.size();

        Set<Set<Shape>> setsForDerivedShapes = new CopyOnWriteArraySet<>();

        for (int i = 0; i < (1 << size); i++) {
            int oneBitsCount = Integer.bitCount(i);
            if (Integer.bitCount(i) > 1) {
                Set<Shape> set = new CopyOnWriteArraySet<>();
                StringBuffer binaryString = new StringBuffer(Integer.toBinaryString(i)).reverse();
                for (int j = 0, fromIndex = 0; j < oneBitsCount; j++) {
                    fromIndex = binaryString.indexOf("1", fromIndex);
                    set.add(baseShapeList.get(fromIndex++));
                }
                setsForDerivedShapes.add(set);
            }
        }

        return setsForDerivedShapes;
    }

    public Set<Shape> getDerivedShapes() {
        return shapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .filter(shape -> ((R2RMLShape) shape).getMappedTriplesMap().isEmpty())
                .map(shape -> (Shape) shape)
                .collect(Collectors.toSet());
    }

    Set<Shape> getDerivedShapesFrom(Set<Shape> baseShapes) {
        Set<Shape> derivedShapes = new CopyOnWriteArraySet<>();

        Set<R2RMLShape> shapes = this.shapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .map(shape -> (R2RMLShape) shape)
                .collect(Collectors.toSet());

        Set<R2RMLShape> r2rmlBaseShapes = baseShapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .map(shape -> (R2RMLShape) shape)
                .collect(Collectors.toSet());

        for (R2RMLShape baseShape: r2rmlBaseShapes) {
            for (R2RMLShape shape: shapes) {
                if (derivedShapes.contains(shape)) continue;

                if (shape.getMappedTriplesMap().isEmpty()) {
                    if (shape.containsInBaseShapes(baseShape))
                        derivedShapes.add(shape);
                }
            }
        }

        return derivedShapes;
    }
}
