package jez;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import jez.builders.CsvDataFrameBuilder;
import jez.builders.DataFrameBuilder;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataFrame
{
	private final List<Row> rows;
	private final List<String> columns;

	public static DataFrameBuilder of(List<Row> rows)
	{
		return new DataFrameBuilder(rows);
	}

	public static CsvDataFrameBuilder fromCsv(String path) throws IOException
	{
		return new CsvDataFrameBuilder(path);
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
		return select(Stream.of(chosenColumns)
				.toList());
	}

	public DataFrame select(List<String> chosenColumns)
	{
		if (!columns().containsAll(chosenColumns))
			throw new IllegalArgumentException("A column does not exist");
		List<Row> projectedRows = rows.stream()
				.map(row -> Row.of(row.get(chosenColumns))
						.withColumns(chosenColumns))
				.toList();
		return DataFrame.of(projectedRows)
				.with(chosenColumns);
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

		for (Row row : this.rows())
		{
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
		return DataFrame.of(filtered)
				.with(columns);
	}

	public DataFrame unique()
	{
		return DataFrame.of(rows.stream()
				.distinct()
				.toList())
				.with(columns);
	}

	public DataFrame unique(String on)
	{
		return this.select(on)
				.unique();
	}

	public DataFrame map(Function<Row, Row> mapping)
	{

		return DataFrame.of(rows.stream()
				.map(mapping)
				.toList())
				.with(columns);
	}

	public Optional<Row> reduce(BinaryOperator<Row> accumulator)
	{
		return this.rows()
				.stream()
				.reduce(accumulator);
	}
}
