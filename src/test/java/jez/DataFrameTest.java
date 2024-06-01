package jez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import jez.builders.DataType;
import jez.data.DataFrame;
import jez.data.FieldValue;
import jez.data.Row;
import jez.utils.ClasspathFileReader;

public class DataFrameTest
{
	private static final String CITY = "City";
	private static final String LAT_D = "LatD";

	private static DataFrame readCsv() throws IOException
	{
		return DataFrame.fromCsv("cities.csv")
				.withDelimiter(",")
				.withHeaders()
				.read();
	}

	private static DataFrame readCsvWithTypes() throws IOException
	{
		return DataFrame.fromCsv("cities.csv")
				.withDelimiter(",")
				.withHeaders()
				.with(LAT_D)
				.as(DataType.INTEGER)
				.with("LatM")
				.as(DataType.DOUBLE)
				.read();
	}

	BiFunction<List<String>, List<String>, List<Row>> linesToRows = (myLines, myColumns) -> {
		List<Row> rows = new ArrayList<>();
		for (String line : myLines)
		{
			List<FieldValue> fields = Stream.of(line.split(","))
					.map(cell -> FieldValue.of(cell))
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

	@Test
	void loading_a_dataset_of_rows_from_csv() throws IOException
	{
		List<String> lines = ClasspathFileReader.instance()
				.readLines("cities.csv");

		List<String> columns = Stream.of(lines.get(0)
				.split(","))
				.toList();
		List<FieldValue> firstCsvRow = Stream.of(lines.get(1)
				.split(","))
				.map(cell -> FieldValue.of(cell))
				.toList();
		List<Row> rows = linesToRows.andThen(removeHeaders)
				.apply(lines, columns);

		DataFrame dataset = readCsv();

		assertThat(dataset.size()).isEqualTo(rows.size());

		assertThat(dataset.row(0)
				.fields()).hasSameElementsAs(firstCsvRow);
		assertThat(dataset.row(0)
				.columns()).hasSameElementsAs(columns);
		assertThat(dataset.row(0)
				.get(columns.get(0))).isEqualTo(firstCsvRow.get(0));
	}

	@Test
	void csv_without_headers() throws IOException
	{
		DataFrame dataFrame = DataFrame.fromCsv("cities_no_headers.csv")
				.withDelimiter(",")
				.withoutHeaders()
				.read();

		assertThat(dataFrame.size()).isEqualTo(129);

		assertThat(dataFrame.columns()).hasSize(10);
		assertThat(dataFrame.columns()
				.get(0)).isEqualTo("Column 0");
		assertThat(dataFrame.columns()
				.get(9)).isEqualTo("Column 9");
	}

	@Test
	void csv_with_typed_columns() throws IOException
	{
		DataFrame dataFrame = DataFrame.fromCsv("cities.csv")
				.withDelimiter(",")
				.withHeaders()
				.with("LatD")
				.as(DataType.INTEGER)
				.with("LatM")
				.as(DataType.DOUBLE)
				.read();

		assertThat(dataFrame.row(0)
				.get("LatD")
				.<Integer>get()).isEqualTo(41);
		assertThat(dataFrame.row(0)
				.get("LatM")
				.<Double>get()).isEqualTo(5.0);
	}

	@Test
	void select_columns() throws IOException
	{
		List<String> lines = ClasspathFileReader.instance()
				.readLines("cities.csv");

		List<String> firstCsvRow = Stream.of(lines.get(1)
				.split(","))
				.toList();
		List<String> columns = Stream.of(lines.get(0)
				.split(","))
				.toList();

		DataFrame dataset = readCsv();

		assertThat(dataset.columns()).hasSameElementsAs(columns);

		List<String> chosenColumns = columns.subList(0, 2);
		DataFrame projection = dataset.select(chosenColumns);

		assertThat(projection.columns()
				.size()).isEqualTo(chosenColumns.size());
		assertThat(projection.columns()).hasSameElementsAs(chosenColumns);

		assertThat(projection.row(0)
				.get(chosenColumns.get(0))).isEqualTo(firstCsvRow.get(0));
	}

	@Test
	void select_non_existing_columns() throws IOException
	{
		DataFrame dataset = readCsv();

		assertThatThrownBy(() -> {
			dataset.select("a non existing column");
		}).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void filtering_lines() throws IOException
	{
		DataFrame dataset = readCsv();

		String aCity = dataset.row(42)
				.get(CITY)
				.<String>get();

		DataFrame filtered = dataset.where(row -> row.get(CITY)
				.equals(aCity));

		assertThat(filtered.size() == 1);
		assertThat(filtered.row(0)
				.get(CITY)).isEqualTo(aCity);
	}

	@Test
	void unique_on_all_columns() throws IOException
	{
		DataFrame dataset = readCsv();

		assertThat(dataset.size()).isEqualTo(129);

		dataset = dataset.unique();

		assertThat(dataset.size()).isEqualTo(128);
		assertThat(dataset.columns()
				.size()).isEqualTo(10);
	}

	@Test
	void unique_on_given_column() throws IOException
	{
		DataFrame dataset = readCsv();

		assertThat(dataset.size()).isEqualTo(129);

		dataset = dataset.unique(CITY);

		assertThat(dataset.size()).isEqualTo(120);
		assertThat(dataset.columns()
				.size()).isEqualTo(1);
		assertThat(dataset.columns()
				.get(0)).isEqualTo(CITY);
	}

	@Test
	void map() throws IOException
	{
		DataFrame dataset = readCsv();

		dataset = dataset.map(row -> row
				.transform(	LAT_D,
							value -> FieldValue.of("" + Integer.parseInt(value.<String>get()
									.strip()) * 2)));

		assertThat(dataset.row(0)
				.get(LAT_D)).isEqualTo("82");
	}

	@Test
	void reduce() throws IOException
	{
		DataFrame dataset = readCsvWithTypes();

		Integer columnSum = dataset.reduce(
											(accumulator, row) -> accumulator.transform(
																						LAT_D,
																						value -> FieldValue
																								.of(value
																										.<Integer>get()
																										+ row.get(LAT_D)
																												.<Integer>get())))
				.get()
				.get(LAT_D)
				.<Integer>get();

		assertThat(columnSum).isEqualTo(5010);
	}
}
