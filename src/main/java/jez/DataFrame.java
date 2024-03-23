package jez;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class DataFrame
{
	private final List<Row> rows;
	private final List<String> columns;

	private DataFrame(List<Row> rows, List<String> columns)
	{
		this.rows = rows;
		this.columns = columns;
	}

	public static DataFrame of(List<Row> rows, List<String> columns)
	{
		return new DataFrame(rows, columns);
	}

	public static DataFrame fromCsv(String path, String delimiter) throws IOException
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
								.withColumns(myColumns);
						rows.add(newRow);
					}
					return rows;
				};

		Function<List<Row>, List<Row>> removeHeaders = (rows) -> {
			rows.remove(0);
			return rows;
		};

		List<Row> rows = linesToRows.andThen(removeHeaders).apply(lines, columns);

		return DataFrame.of(rows, columns);
	}

	public int size()
	{
		return rows.size();
	}

	public Row row(int index)
	{
		return rows.get(index);
	}

	public List<String> columns()
	{
		return this.columns;
	}

	public DataFrame select(String... chosenColumns)
	{
		return select(Stream.of(chosenColumns).toList());
	}

	public DataFrame select(List<String> chosenColumns)
	{
		if (!columns().containsAll(chosenColumns))
			throw new IllegalArgumentException("A column does not exist");
		List<Row> projectedRows = rows.stream()
			.map(row -> Row.of(row.get(chosenColumns))
					.withColumns(chosenColumns)
			)
			.toList();
		return DataFrame.of(projectedRows, chosenColumns);
	}

	public List<Row> rows()
	{
		return this.rows;
	}

	@Override
	public String toString()
	{
		
		StringBuilder builder = new StringBuilder();
		Runnable newLine = () -> builder.append("\n");
		
		String horizontal = this.columns()
				.stream()
				.map(col -> "-".repeat(col.length()))
				.reduce("", (acc, value) -> acc += value + "-");

		builder.append(horizontal);
		newLine.run();
		for (String column : this.columns())
		{
			builder.append("|");
			builder.append(column);
		}
		builder.append("|");
		newLine.run();

		builder.append(horizontal);
		newLine.run();

		for (Row row : this.rows()) {
			for (String column : this.columns())
			{
				builder.append("|");
				builder.append(row.get(column));
			}
			builder.append("|");
			newLine.run();
		}
		return builder.toString();
	}

	public DataFrame where(Predicate<Row> predicate)
	{
		List<Row> filtered = rows().stream()
			.filter(predicate)
			.toList();
		return DataFrame.of(filtered, columns());
	}

	public DataFrame unique()
	{
		return DataFrame.of(rows.stream().distinct().toList(), columns);
	}
}
