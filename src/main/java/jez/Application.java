package jez;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class Application
{
	private static LocalDateTime time()
	{
		return LocalDateTime.now();
	}
	
	public static void main(String[] args) throws IOException
	{
		final DataFrame dataFrame = DataFrame
				.fromCsv("geographic-units-by-industry-and-statistical-area-2000-2023-descending-order-february-2023.csv")
				.withDelimiter(",")
				.withHeaders()
				.read();

		LocalDateTime start = time();

		DataFrame filtered = dataFrame.where(row -> row.get("anzsic06")
				.equals("S942") &&
				row.get("Area")
						.equals("A141700"));

		LocalDateTime end = time();

		System.out.println("New size : " + filtered.size());
		System.out.println("Computing time : " + ChronoUnit.MILLIS.between(start, end));
	}

}
