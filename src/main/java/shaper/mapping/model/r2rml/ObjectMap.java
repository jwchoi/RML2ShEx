package shaper.mapping.model.r2rml;

import janus.database.SQLSelectField;

public class ObjectMap extends TermMap {
    // column or template
    ObjectMap(SQLSelectField column) {
        setColumn(column);
    }

    ObjectMap(Template template) {
        setTemplate(template);
    }

    ObjectMap(String constant) { setConstant(constant); }
}
