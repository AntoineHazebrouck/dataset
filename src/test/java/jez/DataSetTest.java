package jez;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import lombok.Data;

public class DataSetTest {
	@Data
	private static class Person {
		private final String firstname;
		private final String lastname;
	}

	@Test
	void init_dataset() {
		List<Person> persons = new ArrayList<>();
		persons.add(new Person("Antoine", "Hazebrouck"));
		persons.add(new Person("M", "Pokora"));

		DataSet<Person> dataset = DataSet.of(persons);

		assertThat(dataset.size()).isEqualTo(persons.size());
		assertThat(dataset.row(0)).isEqualTo(persons.get(0));
	}
}
