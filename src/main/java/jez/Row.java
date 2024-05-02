package jez;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import jez.builders.RowBuilder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@EqualsAndHashCode
public class Row
{
	private final Map<String, FieldValue> data;

	public static RowBuilder of(List<FieldValue> fields)
	{
		return new RowBuilder(fields);
	}

	public List<String> columns()
	{
		return new ArrayList<>(data.keySet());
	}

	public List<FieldValue> fields()
	{
		return new ArrayList<>(data.values());
	}

	public FieldValue get(String columnName)
	{
		return data.get(columnName);
	}

	public List<FieldValue> get(List<String> columnNames)
	{
		List<FieldValue> fields = new ArrayList<>();
		Set<Entry<String, FieldValue>> entryset = data.entrySet();
		for (Entry<String, FieldValue> entry : entryset)
		{
			if (columnNames.contains(entry.getKey()))
			{
				fields.add(entry.getValue());
			}
		}
		return fields;
	}

	public Row transform(String onColumn, Function<FieldValue, FieldValue> transformation)
	{
		FieldValue toTransform = get(onColumn);
		FieldValue transformed = transformation.apply(toTransform);
		Map<String, FieldValue> transformedData = new HashMap<>(data);
		transformedData.replace(onColumn, toTransform, transformed);

		return new Row(transformedData);
	}
}
