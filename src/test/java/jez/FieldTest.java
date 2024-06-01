package jez;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import jez.data.FieldValue;

public class FieldTest {
	@Test
	void retrieveTypeOfField() {
		FieldValue field = FieldValue.of("a string value");
		assertThat(field.<String>get()).isEqualTo("a string value");

		FieldValue field2 = FieldValue.of(2);
		assertThat(field2.<Integer>get()).isEqualTo(2);
	}

	@Test
	void equals() {
		FieldValue fieldValue = FieldValue.of("any");
		assertThat(fieldValue).isEqualTo(FieldValue.of("any"));
		assertThat(fieldValue).isEqualTo("any");
		assertThat(fieldValue).isNotEqualTo("anything");
		assertThat(fieldValue).isNotEqualTo(FieldValue.of("anything"));
	}
}
