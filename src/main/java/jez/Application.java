package jez;

import java.io.IOException;

public class Application
{

	public static void main(String[] args) throws IOException
	{
		Dataset dataset = Dataset.fromCsv("src/test/resources/cities.csv", ",");

		// String horizontal = dataset.columns()
		// 		.stream()
		// 		.map(col -> "-".repeat(col.length()))
		// 		.reduce("", (acc, value) -> acc += value + "-");

		// System.out.println(horizontal);
		// for (String column : dataset.columns())
		// {
		// 	System.out.print("|");
		// 	System.out.print(column);
		// }
		// System.out.println("|");
		// System.out.println(horizontal);

		// for (Row row : dataset.rows()) {
		// 	for (String column : dataset.columns())
		// 	{
		// 		int diffSize = column.length() - row.get(column).length();
		// 		System.out.print("|");
		// 		System.out.print(row.get(column).trim());
		// 	}
		// 	System.out.println("|");
		// }
		System.out.println(dataset);
	}

}
