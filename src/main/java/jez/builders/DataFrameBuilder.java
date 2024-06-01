package jez.builders;

import java.util.List;
import jez.data.DataFrame;
import jez.data.Row;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataFrameBuilder {
	private final List<Row> rows;
	
	public DataFrame with(List<String> columns) {
		return new DataFrame(rows, columns);
	}
}
