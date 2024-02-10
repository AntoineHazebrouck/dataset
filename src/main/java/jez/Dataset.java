package jez;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public class Dataset
{
	private final List<Row> rows;

	private Dataset(List<Row> rows)
	{
		this.rows = rows;
	}

	public static Dataset of(List<Row> rows)
	{
		return new Dataset(rows);
	}

	public static Dataset fromCsv(String path, String delimiter) throws IOException
	{
		String csv = Files.readString(Path.of(path));
		List<String> lines = csv.lines()
				.toList();
		List<String> columns = Stream.of(lines.get(0)
				.split(delimiter))
				.toList();


		BiFunction<List<String>, List<String>, List<Row>> linesToRows =
				(myLines, myColumns) -> {
					List<Row> rows = new ArrayList<>();
					for (String line : myLines)
					{
						List<String> fields = Stream.of(line.split(delimiter))
								.toList();
						Row newRow = Row.of(fields)
								.withColumns(columns);
						rows.add(newRow);
					}
					return rows;
				};

		Function<List<Row>, List<Row>> removeHeaders = (rows) -> {
			rows.remove(0);
			return rows;
		};

		List<Row> rows = linesToRows.andThen(removeHeaders).apply(lines, columns);

		return Dataset.of(rows);
	}

	public int size()
	{
		return rows.size();
	}

	public Row row(int index)
	{
		return rows.get(index);
	}

}
