package jez.builders;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import jez.DataFrame;
import jez.FieldValue;
import jez.Row;
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
		String csv = Files.readString(Path.of(path));
		List<String> lines = new ArrayList<>(csv.lines()
				.toList());
		List<String> columns = Stream.of(lines.get(0)
				.split(delimiter))
				.toList();


		BiFunction<List<String>, List<String>, List<Row>> linesToRows =
				(myLines, myColumns) -> {
					if (headers)
					{
						myLines.remove(0);
					}
					List<Row> rows = new ArrayList<>();
					for (String line : myLines)
					{
						List<FieldValue> fields = Stream.of(line.split(delimiter))
								.map(cell -> FieldValue.of(cell))
								.toList();
						Map<String, FieldValue> data = new HashMap<>();
						for (int index = 0; index < fields.size(); index++)
						{
							String currentColumn = columns.get(index);
							FieldValue currentField;
							if (columnTypes.containsKey(currentColumn))
							{
								String value = fields.get(index)
										.<String>get();

								switch (columnTypes.get(currentColumn))
								{
									case INTEGER:
										currentField =
												FieldValue.of(Integer.parseInt(value.strip()));
										break;
									case DOUBLE:
										currentField =
												FieldValue.of(Double.parseDouble(value.strip()));
										break;
									// case STRING:
									// currentField = FieldValue.of(value);
									// break;
									default:
										currentField = FieldValue.of(value);
										break;
								}
							} else
							{
								currentField = fields.get(index);
							}

							data.put(currentColumn, currentField);
						}
						Row newRow = new Row(data);
						rows.add(newRow);
					}
					return rows;
				};

		List<Row> rows = linesToRows
				.apply(lines, columns);


		return DataFrame.of(rows)
				.with(columns);
	}
}
