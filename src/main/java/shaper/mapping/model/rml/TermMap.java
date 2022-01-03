package shaper.mapping.model.rml;

import janus.database.SQLSelectField;

import java.net.URI;
import java.util.Optional;

public abstract class TermMap {

    public enum TermTypes {
        IRI(URI.create("http://www.w3.org/ns/r2rml#IRI")),
        BLANKNODE(URI.create("http://www.w3.org/ns/r2rml#BlankNode")),
        LITERAL(URI.create("http://www.w3.org/ns/r2rml#Literal"));

        private URI uri;

        TermTypes(URI uri) { this.uri = uri; }

        public URI getUri() { return uri; }
    }

    private Optional<String> constant; // rr:constant //  IRI in subject map
    private Optional<SQLSelectField> column; // rr:column
    private Optional<Template> template; // rr:template
    private Optional<TermTypes> termType; // rr:termType
    private Optional<String> language; // rr:language
    private Optional<URI> datatype; // rr:datatype
    private Optional<String> inverseExpression; // rr:inverseExpression

    private Optional<String> reference; // rml:reference

    TermMap() {
        constant = Optional.empty();
        column = Optional.empty();
        template = Optional.empty();
        termType = Optional.empty();
        language = Optional.empty();
        datatype = Optional.empty();
        inverseExpression = Optional.empty();

        reference = Optional.empty();
    }

    public void setConstant(String constant) { this.constant = Optional.ofNullable(constant); }
    public void setColumn(SQLSelectField column) { this.column = Optional.ofNullable(column); }
    public void setTemplate(Template template) { this.template = Optional.ofNullable(template); }

    public void setTermType(URI uri) {
        if (uri.equals(TermTypes.BLANKNODE.getUri()))
            termType = Optional.of(TermTypes.BLANKNODE);
        else if (uri.equals(TermTypes.IRI.getUri()))
            termType = Optional.of(TermTypes.IRI);
        else if (uri.equals(TermTypes.LITERAL.getUri()))
            termType = Optional.of(TermTypes.LITERAL);
        else
            termType = Optional.empty();
    }

    public void setTermType(TermTypes termType) { this.termType = Optional.of(termType); }

    public void setLanguage(String language) { this.language = Optional.ofNullable(language); }
    public void setDatatype(URI datatype) { this.datatype = Optional.ofNullable(datatype); }
    public void setinverseExpression(String inverseExpression) { this.inverseExpression = Optional.ofNullable(inverseExpression); }

    void setReference(String reference) { this.reference = Optional.ofNullable(reference); }

    public Optional<Template> getTemplate() { return template; }
    public Optional<TermTypes> getTermType() { return termType; }
    public Optional<String> getConstant() { return constant; }
    public Optional<String> getLanguage() { return language; }
    public Optional<URI> getDatatype() { return datatype; }
    public Optional<SQLSelectField> getColumn() { return column; }
}
