package jez.builders;

import java.util.List;
import jez.DataFrame;
import jez.Row;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataFrameBuilder {
	private final List<Row> rows;
	
	public DataFrame with(List<String> columns) {
		return new DataFrame(rows, columns);
	}
}
