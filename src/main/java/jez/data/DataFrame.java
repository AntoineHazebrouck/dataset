package jez.data;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import jez.builders.CsvDataFrameBuilder;
import jez.builders.DataFrameBuilder;

public class DataFrame extends DataSet<Row>
{
	private final List<String> columns;

	private final DataOperator dataOperator = DataOperator.instance();

	public DataFrame(List<Row> rows, List<String> columns)
	{
		super(rows);
		this.columns = columns;
	}

	public static DataFrameBuilder of(List<Row> rows)
	{
		return new DataFrameBuilder(rows);
	}

	public static CsvDataFrameBuilder fromCsv(String path) throws IOException
	{
		return new CsvDataFrameBuilder(path);
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

	@Override
	public String toString()
	{
		return dataOperator.toString(this);
	}

	@Override
	public DataFrame where(Predicate<Row> predicate)
	{
		return DataFrame.of(super.where(predicate).rows())
				.with(columns);
	}

	@Override
	public DataFrame unique()
	{
		return DataFrame.of(super.unique().rows())
				.with(columns);
	}

	public DataFrame unique(String on)
	{
		return (DataFrame) this.select(on)
				.unique();
	}

	@Override
	public DataFrame map(Function<Row, Row> mapping)
	{
		return DataFrame.of(super.map(mapping).rows())
				.with(columns);
	}
}
