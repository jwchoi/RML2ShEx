package shaper.mapping.model.shex;

import janus.database.SQLSelectField;
import shaper.Shaper;
import shaper.mapping.SqlXsdMap;
import shaper.mapping.Symbols;
import shaper.mapping.model.ID;
import shaper.mapping.model.r2rml.ObjectMap;
import shaper.mapping.model.r2rml.Template;
import shaper.mapping.model.r2rml.TermMap;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public class R2RMLNodeConstraint extends NodeConstraint {
    private ObjectMap mappedObjectMap;

    R2RMLNodeConstraint(ID id, ObjectMap mappedObjectMap) {
        super(id);

        this.mappedObjectMap = mappedObjectMap;
    }

    ObjectMap getMappedObjectMap() { return mappedObjectMap; }

    private void buildNodeConstraint(ObjectMap objectMap) {
        // nodeKind
        Optional<TermMap.TermTypes> termType = objectMap.getTermType();
        if (termType.isPresent()) {
            if (termType.get().equals(TermMap.TermTypes.BLANKNODE))
                setNodeKind(Optional.of(NodeKinds.BNODE));
            else if (termType.get().equals(TermMap.TermTypes.IRI))
                setNodeKind(Optional.of(NodeKinds.IRI));
            else if (termType.get().equals(TermMap.TermTypes.LITERAL))
                setNodeKind(Optional.of(NodeKinds.LITERAL));
        }

        // language
        Optional<String> language = objectMap.getLanguage();
        if (language.isPresent()) {
            String languageTag = Symbols.OPEN_BRACKET + Symbols.AT + language.get() + Symbols.CLOSE_BRACKET;
            setValueSet(Optional.of(languageTag));
        }

        // datatype
        Optional<URI> datatype = objectMap.getDatatype();
        if (datatype.isPresent()) { // from rr:column
            String dt = datatype.get().toString();
            Optional<String> relativeDatatype = Shaper.shexMapper.r2rmlModel.getRelativeIRI(datatype.get());
            if (relativeDatatype.isPresent())
                dt = relativeDatatype.get();

            setDatatype(Optional.of(dt));
        } else { // Natural Mapping of SQL Values
            Optional<SQLSelectField> sqlSelectField = objectMap.getColumn();
            if (sqlSelectField.isPresent())
                setDatatype(Optional.of(SqlXsdMap.getMappedXSD(sqlSelectField.get().getSqlType()).getRelativeIRI()));
        }

        // xsFacet, only REGEXP
        if (isPossibleToHaveXSFacet(objectMap))
            setXsFacet(Optional.of(buildRegex(objectMap.getTemplate().get())));
    }

    private String buildRegex(Template template) {
        String regex = template.getFormat();

        // replace meta-characters in XPath
        regex = regex.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        regex = regex.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        // column names
        List<SQLSelectField> columnNames = template.getColumnNames();
        for (SQLSelectField columnName: columnNames)
            regex = regex.replace("{" + columnName.getColumnNameOrAlias() + "}", "(.*)");

        return Symbols.SLASH + Symbols.CARET + regex + Symbols.DOLLAR + Symbols.SLASH;
    }

    public static boolean isPossibleToHaveXSFacet(ObjectMap objectMap) {
        Optional<Template> template = objectMap.getTemplate();
        if (template.isPresent()) {
            if (template.get().getLengthExceptColumnName() > 0)
                return true;
        }

        return false;
    }

    @Override
    public String toString() {
        String serializedNodeConstraint = getSerializedNodeConstraint();

        if (serializedNodeConstraint == null) {
            buildNodeConstraint(mappedObjectMap);
            buildSerializedNodeConstraint();
            serializedNodeConstraint = getSerializedNodeConstraint();

        }

        return serializedNodeConstraint;
    }
}
