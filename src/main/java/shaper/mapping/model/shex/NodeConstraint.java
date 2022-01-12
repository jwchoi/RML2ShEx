package shaper.mapping.model.shex;

import shaper.mapping.Symbols;
import shaper.mapping.model.ID;

import java.util.Optional;

public abstract class NodeConstraint extends ShapeExpr implements Comparable<NodeConstraint> {

    protected enum XSFacets {
        MAX_LENGTH("MAXLENGTH"),
        MIN_INCLUSIVE("MININCLUSIVE"), MAX_INCLUSIVE("MAXINCLUSIVE"),
        TOTAL_DIGITS("TOTALDIGITS"), FRACTION_DIGITS("FRACTIONDIGITS");

        private final String facet;

        XSFacets(String facet) {
            this.facet = facet;
        }

        @Override
        public String toString() { return facet; }
    }

    private String serializedNodeConstraint;

    private Optional<NodeKinds> nodeKind;
    private Optional<String> values;
    private Optional<String> datatype;
    private Optional<String> xsFacet;

    NodeConstraint(ID id) {
        super(id);

        nodeKind = Optional.empty();
        values = Optional.empty();
        datatype = Optional.empty();
        xsFacet = Optional.empty();
    }

    protected String getSerializedNodeConstraint() { return serializedNodeConstraint; }
    protected void setSerializedNodeConstraint(String serializedNodeConstraint) {
        this.serializedNodeConstraint = serializedNodeConstraint;
    }

    protected Optional<NodeKinds> getNodeKind() { return nodeKind; }
    protected void setNodeKind(Optional<NodeKinds> nodeKind) { this.nodeKind = nodeKind; }

    protected Optional<String> getValues() { return values; }
    protected void setValues(Optional<String> values) { this.values = values; }

    protected Optional<String> getDatatype() { return datatype; }
    protected void setDatatype(Optional<String> datatype) { this.datatype = datatype; }

    protected Optional<String> getXsFacet() { return xsFacet; }
    protected void setXsFacet(Optional<String> xsFacet) { this.xsFacet = xsFacet; }

    protected void buildSerializedNodeConstraint() {
        StringBuffer nodeConstraint = new StringBuffer();

        if (values.isPresent())
            nodeConstraint.append(values.get());
        else if (datatype.isPresent())
            nodeConstraint.append(datatype.get());
        else if (nodeKind.isPresent())
            nodeConstraint.append(nodeKind.get());

        if (xsFacet.isPresent())
            nodeConstraint.append(Symbols.SPACE + xsFacet.get());

        setSerializedNodeConstraint(nodeConstraint.toString());
    }

    @Override
    public int compareTo(NodeConstraint o) {
        return serializedNodeConstraint.compareTo(o.toString());
    }
}
