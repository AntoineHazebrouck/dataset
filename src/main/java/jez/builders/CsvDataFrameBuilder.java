package jez.builders;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import jez.DataFrame;
import jez.FieldValue;
import jez.Row;
import jez.utils.ClasspathFileReader;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CsvDataFrameBuilder
{
	private final String path;
	private String delimiter;
	private boolean headers;
	protected Map<String, DataType> columnTypes = new HashMap<>();

	public CsvDataFrameBuilder withDelimiter(String delimiter)
	{
		this.delimiter = delimiter;
		return this;
	}

	public CsvDataFrameBuilder withoutHeaders()
	{
		this.headers = false;
		return this;
	}

	public CsvDataFrameBuilder withHeaders()
	{
		this.headers = true;
		return this;
	}

	public ColumnTyper with(String column)
	{
		return new ColumnTyper(this, column);
	}

	public DataFrame read() throws IOException
	{
		List<String> allLines = ClasspathFileReader.instance().readLines(path);

		
		List<String> columns;
		List<String> lines = allLines;
		if (headers)
		{
			columns = Stream.of(allLines.get(0)
					.split(delimiter))
					.toList();

			lines.remove(0);
		} else
		{
			// default column names
			columns = IntStream.range(	0,
										allLines.get(0)
												.split(delimiter).length)
					.mapToObj(number -> "Column " + number)
					.toList();
		}

		List<Row> rows = lines.stream()
				.map(line -> {
					List<String> cells = Stream.of(line.split(delimiter))
							.toList();

					Map<String, FieldValue> rowData = new HashMap<>();
					for (int index = 0; index < cells.size(); index++)
					{
						String currentValue = cells.get(index);
						String currentColumn = columns.get(index);

						FieldValue field = getTypedField(currentColumn, currentValue);
						rowData.put(currentColumn, field);
					}

					return new Row(rowData);
				})
				.toList();

		return DataFrame.of(rows)
				.with(columns);
	}

	private FieldValue getTypedField(String currentColumn, String currentValue)
	{
		switch (columnTypes.getOrDefault(currentColumn, DataType.STRING))
		{
			case INTEGER:
				return FieldValue.of(Integer.parseInt(currentValue.strip()));
			case DOUBLE:
				return FieldValue.of(Double.parseDouble(currentValue.strip()));
			default:
				return FieldValue.of(currentValue);
		}
	}
}
