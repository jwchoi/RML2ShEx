package shaper.mapping.model.rml;

import java.net.URI;
import java.util.Optional;

public abstract class TermMap {

    public enum TermTypes {
        IRI(URI.create("http://www.w3.org/ns/r2rml#IRI")),
        BLANKNODE(URI.create("http://www.w3.org/ns/r2rml#BlankNode")),
        LITERAL(URI.create("http://www.w3.org/ns/r2rml#Literal"));

        private URI uri;

        TermTypes(URI uri) {
            this.uri = uri;
        }

        public URI getUri() {
            return uri;
        }
    }

    private Optional<String> constant; // rr:constant -> IRI(in subject map, predicate map, object map or graph map) or literal(in object map)
    //    private Optional<SQLSelectField> column; // rr:column
    private Optional<String> column; // rr:column
    private Optional<Template> template; // rr:template
    private TermTypes termType; // rr:termType
    private Optional<String> inverseExpression; // rr:inverseExpression

    private Optional<String> reference; // rml:reference overrides rr:column

    TermMap() {
        constant = Optional.empty();
        column = Optional.empty();
        template = Optional.empty();
        inverseExpression = Optional.empty();

        reference = Optional.empty();
    }

    void setConstant(String constant) {
        if (constant != null) {
            this.constant = Optional.of(constant);
            setTermType(TermTypes.LITERAL);
        }
    }

    void setConstant(URI constant) {
        if (constant != null) {
            this.constant = Optional.of(constant.toString());
            setTermType(TermTypes.IRI);
        }
    }

//    void setColumn(SQLSelectField column) { this.column = Optional.ofNullable(column); }
    void setColumn(String column) {
        if (column != null) {
            this.column = Optional.of(column);
            setTermType(TermTypes.IRI);
        }
    }


    void setTemplate(Template template) {
        if (template != null) {
            this.template = Optional.of(template);
            setTermType(TermTypes.IRI);
        }
    }

    void setTermType(URI uri) {
        if (uri == null) return;

        if (uri.equals(TermTypes.BLANKNODE.getUri()))
            termType = TermTypes.BLANKNODE;
        else if (uri.equals(TermTypes.IRI.getUri()))
            termType = TermTypes.IRI;
        else if (uri.equals(TermTypes.LITERAL.getUri()))
            termType = TermTypes.LITERAL;
    }

    void setTermType(TermTypes termType) {
        if (termType != null) this.termType = termType;
    }

    void setInverseExpression(String inverseExpression) {
        this.inverseExpression = Optional.ofNullable(inverseExpression);
    }

    void setReference(String reference) {
        if (reference != null) {
            this.reference = Optional.of(reference);
            setTermType(TermTypes.IRI);
        }
    }

    public Optional<Template> getTemplate() {
        return template;
    }

    public TermTypes getTermType() {
        return termType;
    }

    public Optional<String> getConstant() {
        return constant;
    }

//    public Optional<SQLSelectField> getColumn() { return column; }
    public Optional<String> getColumn() { return column; }
}
