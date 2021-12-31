package shaper.mapping.model.dm;

@SuppressWarnings("serial")
class IndividualParsingException extends Exception {

	@Override
	public String getMessage() {
		return "Can't Parse Individual.";
	}
}
