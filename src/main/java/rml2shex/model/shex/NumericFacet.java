package rml2shex.model.shex;

import rml2shex.commons.Symbols;

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

    NumericFacet(NumericRange numericRange, String numericalLiteral) {
        super(Kinds.numericalFacet);

        this.numericRange = numericRange;
        this.numericalLiteral = numericalLiteral;
    }

    private String getSerializedStringFacet() { return numericRange + Symbols.SPACE + numericalLiteral; }

    @Override
    public String toString() { return getSerializedStringFacet(); }

    @Override
    public int compareTo(XSFacet o) {
        int resultFromSuper = super.compareTo(o);
        if (resultFromSuper != 0) return resultFromSuper;

        return Integer.compare(numericRange.ordinal(), ((NumericFacet) o).numericRange.ordinal());
    }
}
