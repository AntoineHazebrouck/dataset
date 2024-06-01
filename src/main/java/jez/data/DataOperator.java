package jez.data;

public class DataOperator
{
	private static DataOperator instance;

	private DataOperator()
	{}

	public static DataOperator instance()
	{
		if (instance == null)
		{
			return new DataOperator();
		} else
		{
			return instance;
		}
	}

	public String toString(DataFrame dataFrame)
	{
		StringBuilder builder = new StringBuilder();
		Runnable newLine = () -> builder.append("\n");

		String horizontal = dataFrame.columns()
				.stream()
				.map(col -> "-".repeat(col.length()))
				.reduce("", (acc, value) -> acc += value + "-");

		builder.append(horizontal);
		newLine.run();
		for (String column : dataFrame.columns())
		{
			builder.append("|");
			builder.append(column);
		}
		builder.append("|");
		newLine.run();

		builder.append(horizontal);
		newLine.run();

		for (Row row : dataFrame.rows())
		{
			for (String column : dataFrame.columns())
			{
				builder.append("|");
				builder.append(row.get(column));
			}
			builder.append("|");
			newLine.run();
		}
		return builder.toString();
	}
}
