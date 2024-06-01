package jez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import jez.data.DataFrame;
import jez.data.FieldValue;
import jez.data.Row;
import jez.utils.ClasspathFileReader;

public class RowTest
{
	@Test
	void row_from_a_line() throws IOException
	{
		List<String> lines = ClasspathFileReader.instance()
				.readLines("cities.csv");

		List<String> columns = Stream.of(lines.get(0)
				.split(","))
				.toList();
		List<FieldValue> firsCsvRow = Stream.of(lines.get(1)
				.split(","))
				.map(cell -> FieldValue.of(cell))
				.toList();

		Row firstRow = Row.of(firsCsvRow)
				.withColumns(columns);

		for (int index = 0; index < columns.size(); index++)
		{
			String currentColumn = columns.get(index);
			FieldValue currentValue = firsCsvRow.get(index);

			assertThat(firstRow.get(currentColumn)).isEqualTo(currentValue.<String>get());
		}
	}

	@Test
	void should_not_build_row_if_columnssize_and_fieldssize_differ() throws IOException
	{
		List<String> lines = ClasspathFileReader.instance()
				.readLines("cities.csv");

		List<String> columns = Stream.of(lines.get(0)
				.split(","))
				.toList();
		List<FieldValue> firsCsvRow = Stream.of(lines.get(1)
				.split(","))
				.map(cell -> FieldValue.of(cell))
				.toList();
		List<String> tooManyColumns = new ArrayList<>(columns);
		tooManyColumns.add("something unexpected");

		assertThatThrownBy(() -> {
			Row.of(firsCsvRow)
					.withColumns(tooManyColumns);

		}).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void transform_row() throws IOException
	{
		Row row = Row.of(List.of(FieldValue.of("id1"), FieldValue.of("42")))
				.withColumns(List.of("id", "value"));

		row = row.transform("value",
							value -> FieldValue.of("" + Integer.parseInt(value.<String>get()) * 2));

		assertThat(row.get("value")).isEqualTo("84");
	}

	@Test
	void equals() throws IOException
	{
		Row row = Row.of(List.of(FieldValue.of("id1"), FieldValue.of("42")))
				.withColumns(List.of("id", "value"));

		Row row2 = Row.of(List.of(FieldValue.of("id1"), FieldValue.of("42")))
				.withColumns(List.of("id", "value"));

		Row row3 = Row.of(List.of(FieldValue.of("id1"), FieldValue.of("47")))
				.withColumns(List.of("id", "value"));

		assertThat(row).isEqualTo(row2);
		assertThat(row).isNotEqualTo(row3);

		DataFrame dataset = DataFrame.fromCsv("cities.csv")
				.withDelimiter(",")
				.withHeaders()
				.read();

		assertThat(dataset.row(0)).isEqualTo(dataset.row(1));
	}
}
