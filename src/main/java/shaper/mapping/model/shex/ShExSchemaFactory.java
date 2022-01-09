package shaper.mapping.model.shex;

import janus.database.DBSchema;
import shaper.mapping.model.r2rml.R2RMLModel;
import shaper.mapping.model.rml.RMLModel;

public class ShExSchemaFactory {
    //Direct Mapping
    public static ShExSchema getShExSchemaModel(DBSchema dbSchema) {
        return DM2ShExSchemaFactory.getShExSchemaModel(dbSchema);
    }

    // R2RML
    public static ShExSchema getShExSchemaModel(R2RMLModel r2rmlModel) {
        return R2RML2ShExSchemaFactory.getShExSchemaModel(r2rmlModel);
    }

    // RML
    public static ShExSchema getShExSchemaModel(RMLModel rmlModel) {
        return RML2ShExSchemaFactory.getShExSchemaModel(rmlModel);
    }
}
