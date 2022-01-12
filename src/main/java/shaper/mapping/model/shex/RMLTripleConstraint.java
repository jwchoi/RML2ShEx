package shaper.mapping.model.shex;

import shaper.mapping.model.ID;
import shaper.mapping.model.rml.SubjectMap;

public class RMLTripleConstraint extends TripleConstraint {
    private RMLTripleConstraint(ID id, MappedTypes mappedType) {
        super(id, mappedType);
    }

    RMLTripleConstraint(ID id, SubjectMap subjectMap) {
        this(id, MappedTypes.SUBJECT_MAP);
    }
}
