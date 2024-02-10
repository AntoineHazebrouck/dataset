package jez;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowBuilder
{
	private final List<String> fields;

	protected RowBuilder(List<String> fields)
	{
		this.fields = fields;
	}

	public Row withColumns(List<String> columns)
	{
		if (columns.size() != fields.size())
			throw new IllegalArgumentException("columns and fields difer in sizes");


		Map<String, String> data = new HashMap<>();
		for (int index = 0; index < fields.size(); index++)
		{
			String currentField = fields.get(index);
			String currentColumn = columns.get(index);

			data.put(currentColumn, currentField);
		}

		return new Row(data);
	}

}
