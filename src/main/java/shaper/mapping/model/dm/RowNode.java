package shaper.mapping.model.dm;

import janus.database.DBField;
import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.Utils;

import java.net.URI;
import java.util.List;

public class RowNode {
	
	// generates a row node with ordered column names.
	static String getMappedRowNodeAfterBase(String table, List<DBField> pkFields) {
		StringBuffer rowNode = new StringBuffer(Utils.encode(table));
		rowNode.append(Symbols.SLASH);
		
		for (DBField pkField: pkFields) {
			String column = pkField.getColumnName();
			String value = pkField.getValue();
			
			rowNode.append(Utils.encode(column));
			rowNode.append(Symbols.EQUAL);
			rowNode.append(Utils.encode(value));
			rowNode.append(Symbols.SEMICOLON);
		}
		rowNode.deleteCharAt(rowNode.lastIndexOf(Symbols.SEMICOLON));
		
		return rowNode.toString();
	}
	
	static URI getRowNode(String rowNodeAfterBase) {
	    return URI.create(getRowNodeIncludingBase(rowNodeAfterBase));
	}
	
	private static String getRowNodeIncludingBase(String rowNodeAfterBase) {
		return Shaper.rdfMapper.dmModel.getBaseIRI() + rowNodeAfterBase;
	}
}