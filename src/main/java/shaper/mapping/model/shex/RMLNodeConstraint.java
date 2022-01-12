package shaper.mapping.model.shex;

import shaper.mapping.model.ID;
import shaper.mapping.model.rml.SubjectMap;

import java.util.Optional;

public class RMLNodeConstraint extends NodeConstraint {
    Optional<SubjectMap> mappedSubjectMap;

    RMLNodeConstraint(ID id, SubjectMap subjectMap) {
        super(id);

        this.mappedSubjectMap = Optional.of(subjectMap);
    }
}
