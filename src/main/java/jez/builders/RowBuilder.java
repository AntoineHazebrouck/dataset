package jez.builders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jez.FieldValue;
import jez.Row;

public class RowBuilder
{
	private final List<FieldValue> fields;

	public RowBuilder(List<FieldValue> fields)
	{
		this.fields = fields;
	}

	public Row withColumns(List<String> columns)
	{
		if (columns.size() != fields.size())
			throw new IllegalArgumentException("columns and fields difer in sizes");


		Map<String, FieldValue> data = new HashMap<>();
		for (int index = 0; index < fields.size(); index++)
		{
			FieldValue currentField = fields.get(index);
			String currentColumn = columns.get(index);

			data.put(currentColumn, currentField);
		}

		return new Row(data);
	}

}
