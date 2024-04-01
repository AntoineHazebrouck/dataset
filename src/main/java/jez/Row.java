package jez;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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

	public List<String> columns()
	{
		return new ArrayList<>(data.keySet());
	}

	public List<String> fields()
	{
		return new ArrayList<>(data.values());
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

	public Row transform(String onColumn, Function<String, String> transformation)
	{
		String toTransform = get(onColumn);
		String transformed = transformation.apply(toTransform);
		Map<String, String> transformedData = new HashMap<>(data);
		transformedData.replace(onColumn, toTransform, transformed);
		
		return new Row(transformedData);
	}
}
