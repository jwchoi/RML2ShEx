package rml2shex.model.shex;

public class NumericFacet extends XSFacet {

    enum NumericRange {
        MIN_INCLUSIVE("MININCLUSIVE"), MIN_EXCLUSIVE("MINEXCLUSIVE"), MAX_INCLUSIVE("MAXINCLUSIVE"), MAX_EXCLUSIVE("MAXEXCLUSIVE");

        private final String numericRange;

        NumericRange(String numericRange) {
            this.numericRange = numericRange;
        }

        @Override
        public String toString() {
            return numericRange;
        }
    }

    private NumericRange numericRange;
    private String numericalLiteral;

    private NumericFacet(NumericRange numericRange, String numericalLiteral) {
        super(Kinds.numericalFacet);

        this.numericRange = numericRange;
        this.numericalLiteral = numericalLiteral;
    }
}
