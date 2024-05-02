package jez.builders;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ColumnTyper {
	private final CsvDataFrameBuilder fromBuilder;
	private final String column;

	public CsvDataFrameBuilder as(DataType type) {
		fromBuilder.columnTypes.put(column, type);
		return fromBuilder;
	}
}
