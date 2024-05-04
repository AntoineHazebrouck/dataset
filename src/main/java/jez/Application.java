package jez;

import java.io.IOException;
import jez.builders.DataType;

public class Application
{

	public static void main(String[] args) throws IOException
	{
		DataFrame dataFrame = DataFrame.fromCsv("src/test/resources/cities.csv")
			.withHeaders()
			.withDelimiter(",")
			.with("LatD").as(DataType.INTEGER)
			.with("LatM").as(DataType.INTEGER)
			.with("LatS").as(DataType.INTEGER)
			.with("NS").as(DataType.STRING)
			.with("LonD").as(DataType.INTEGER)
			.with("LonM").as(DataType.INTEGER)
			.with("LonS").as(DataType.INTEGER)
			.with("EW").as(DataType.STRING)
			.with("City").as(DataType.STRING)
			.with("State").as(DataType.STRING)
			.read();

		System.out.println(dataFrame.columns());
		System.out.println(dataFrame.select("State", "City").row(0));
		System.out.println(dataFrame.size());
		dataFrame = dataFrame.where(row -> row.get("State").<String>get().contains("OH"));
		System.out.println(dataFrame.size());
	}

}
