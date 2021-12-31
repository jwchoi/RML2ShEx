package shaper.mapping.model.dm;

import shaper.mapping.model.Utils;

import java.net.URI;

public class TableIRI implements Comparable<TableIRI> {
	private URI tableIRI;
	private String tableIRILocalPart;

	private String mappedTable;
	
	TableIRI(URI baseIRI, String mappedTable) {
		this.mappedTable = mappedTable;

		tableIRILocalPart = buildTableIRILocalPart();
		tableIRI = buildTableIRI(baseIRI, tableIRILocalPart);
	}

	public URI getTableIRI() { return tableIRI; }

	public String getTableIRILocalPart() { return tableIRILocalPart; }
	
	public String getMappedTableName() {
		return mappedTable;
	}

	@Override
	public int compareTo(TableIRI o) {
		return tableIRI.compareTo(o.getTableIRI());
	}

	private URI buildTableIRI(URI baseIRI, String tableIRIFragment) {
		return URI.create(baseIRI + tableIRIFragment);
	}

	private String buildTableIRILocalPart() { return Utils.encode(mappedTable); }

	@Override
	public String toString() {
		return getTableIRI().toString();
	}
}
