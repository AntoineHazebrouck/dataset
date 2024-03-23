package jez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class DataFrameTest
{
	private static final String CITY = " \"City\"";

	BiFunction<List<String>, List<String>, List<Row>> linesToRows =
			(myLines, myColumns) -> {
				List<Row> rows = new ArrayList<>();
				for (String line : myLines)
				{
					List<String> fields = Stream.of(line.split(","))
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
		String csv = Files.readString(Path.of("src/test/resources/cities.csv"));

		List<String> lines = csv.lines()
				.toList();
		List<String> columns = Stream.of(lines.get(0)
				.split(","))
				.toList();
		List<String> firstCsvRow = Stream.of(lines.get(1)
				.split(","))
				.toList();
		List<Row> rows = linesToRows.andThen(removeHeaders)
				.apply(lines, columns);


		DataFrame dataset = DataFrame.fromCsv("src/test/resources/cities.csv", ",");

		assertThat(dataset.size()).isEqualTo(rows.size());

		assertThat(dataset.row(0)
				.fields()).hasSameElementsAs(firstCsvRow);
		assertThat(dataset.row(0)
				.columns()).hasSameElementsAs(columns);
		assertThat(dataset.row(0)
				.get(columns.get(0))).isEqualTo(firstCsvRow.get(0));
	}

	@Test
	void select_columns() throws IOException
	{
		String csv = Files.readString(Path.of("src/test/resources/cities.csv"));
		List<String> lines = csv.lines()
				.toList();
		List<String> firstCsvRow = Stream.of(lines.get(1)
				.split(","))
				.toList();
		List<String> columns = Stream.of(lines.get(0)
				.split(","))
				.toList();

		DataFrame dataset = DataFrame.fromCsv("src/test/resources/cities.csv", ",");

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
		DataFrame dataset = DataFrame.fromCsv("src/test/resources/cities.csv", ",");

		assertThatThrownBy(() -> {
			dataset.select("a non existing column");
		}).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void filtering_lines() throws IOException
	{
		DataFrame dataset = DataFrame.fromCsv("src/test/resources/cities.csv", ",");

		String aCity = dataset.row(42)
				.get(CITY);

		DataFrame filtered = dataset.where(row -> row.get(CITY)
				.equals(aCity));


		assertThat(filtered.size() == 1);
		assertThat(filtered.row(0)
				.get(CITY)).isEqualTo(aCity);
	}

	@Test
	void unique_on_all_columns() throws IOException {
		DataFrame dataset = DataFrame.fromCsv("src/test/resources/cities.csv", ",");
		
		assertThat(dataset.size()).isEqualTo(129);

		dataset = dataset.unique();

		assertThat(dataset.size()).isEqualTo(128);
		assertThat(dataset.columns().size()).isEqualTo(10);
	}

	@Test
	void unique_on_given_column() throws IOException {
		DataFrame dataset = DataFrame.fromCsv("src/test/resources/cities.csv", ",");
		
		assertThat(dataset.size()).isEqualTo(129);

		dataset = dataset.unique(CITY);

		assertThat(dataset.size()).isEqualTo(120);
		assertThat(dataset.columns().size()).isEqualTo(1);
		assertThat(dataset.columns().get(0)).isEqualTo(CITY);
	}
}
