package jez;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class Row {
	private final Map<String, String> data; // column name, field

	protected Row(Map<String, String> data) {
		this.data = data;
	}

	public static RowBuilder of(List<String> fields)
	{
		return new RowBuilder(fields);
	}

	public Iterable<String> columns()
	{
		return data.keySet();
	}

	public Iterable<String> fields()
	{
		return data.values();
	}

	public String get(String columnName)
	{
		return data.get(columnName);
	}

	public List<String> get(List<String> columnNames) {
		return data.entrySet().stream()
			.filter(entry -> columnNames.contains(entry.getKey()))
			.map(entry -> entry.getValue())
			.toList();
	}
}
