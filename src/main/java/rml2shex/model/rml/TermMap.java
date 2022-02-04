package rml2shex.model.rml;

import rml2shex.commons.IRI;
import rml2shex.datasource.Column;

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

    private Optional<String> literalConstant; // rr:constant -> IRI or literal(in object map)
    private Optional<IRI> iriConstant; // rr:constant -> IRI(in subject map, predicate map, object map or graph map)
    private Optional<Column> column; // rr:column
    private Optional<Template> template; // rr:template
    private Optional<TermTypes> termType; // rr:termType
    private Optional<String> inverseExpression; // rr:inverseExpression

    private Optional<Column> reference; // rml:reference overrides rr:column

    TermMap() {
        literalConstant = Optional.empty();
        iriConstant = Optional.empty();
        column = Optional.empty();
        template = Optional.empty();
        inverseExpression = Optional.empty();

        reference = Optional.empty();
    }

    void setConstant(String constant) {
        if (constant != null) {
            literalConstant = Optional.of(constant);
            setTermType(TermTypes.LITERAL);
        }
    }

    void setConstant(IRI constant) {
        if (constant != null) {
            iriConstant = Optional.of(constant);
            setTermType(TermTypes.IRI);
        }
    }

    void setColumn(String column) {
        if (column != null) {
            this.column = Optional.of(new Column(column));
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
            termType = Optional.of(TermTypes.BLANKNODE);
        else if (uri.equals(TermTypes.IRI.getUri()))
            termType = Optional.of(TermTypes.IRI);
        else if (uri.equals(TermTypes.LITERAL.getUri()))
            termType = Optional.of(TermTypes.LITERAL);
    }

    void setTermType(TermTypes termType) {
        if (termType != null) this.termType = Optional.of(termType);
    }

    void setInverseExpression(String inverseExpression) {
        this.inverseExpression = Optional.ofNullable(inverseExpression);
    }

    void setReference(String reference) {
        if (reference != null) {
            this.reference = Optional.of(new Column(reference));
            setTermType(TermTypes.IRI);
        }
    }

    public Optional<Template> getTemplate() {
        return template;
    }

    public Optional<TermTypes> getTermType() {
        return termType;
    }

    public Optional<String> getLiteralConstant() {
        return literalConstant;
    }
    public Optional<IRI> getIRIConstant() {
        return iriConstant;
    }

    public Optional<Column> getColumn() { return column; }
    public Optional<Column> getReference() { return reference; }
}
