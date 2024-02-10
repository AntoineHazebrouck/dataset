package jez;

import java.util.List;

public class Dataset {
	private final List<Row> rows;

	private Dataset(List<Row> rows)
	{
		this.rows = rows;
	}

	public static Dataset of(List<Row> rows)
	{
		return new Dataset(rows);
	}

	public int size()
	{
		return rows.size();
	}

	public Row row(int index)
	{
		return rows.get(index);
	}

}
