package jez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import lombok.val;

public class RowTest {
	@Test
	void row_from_a_line() throws IOException {
		val csv = Files.readString(Path.of("src/test/resources/cities.csv"));

		List<String> lines = csv.lines().toList();
		List<String> columns = Stream.of(lines.get(0).split(",")).toList();
		List<String> firsCsvRow = Stream.of(lines.get(1).split(",")).toList();

		Row firstRow = Row.of(firsCsvRow)
				.withColumns(columns);

		for (int index = 0; index < columns.size(); index++) {
			String currentColumn = columns.get(index);
			String currentValue = firsCsvRow.get(index);

			assertThat(firstRow.get(currentColumn)).isEqualTo(currentValue);
		}
	}

	@Test
	void should_not_build_row_if_columnssize_and_fieldssize_differ() throws IOException {
		val csv = Files.readString(Path.of("src/test/resources/cities.csv"));

		List<String> lines = csv.lines().toList();
		List<String> columns = Stream.of(lines.get(0).split(",")).toList();
		List<String> firsCsvRow = Stream.of(lines.get(1).split(",")).toList();
		List<String> tooManyColumns = new ArrayList<>(columns);
		tooManyColumns.add("something unexpected");

		assertThatThrownBy(() -> {
			Row.of(firsCsvRow)
					.withColumns(tooManyColumns);

		}).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void transform_row() throws IOException {
		Row row = Row.of(List.of("id1", "42")).withColumns(List.of("id", "value"));
		
		row = row.transform("value", value -> "" + Integer.parseInt(value) * 2);

		assertThat(row.get("value")).isEqualTo("84");
	}
}
