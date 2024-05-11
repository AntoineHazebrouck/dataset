package jez.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ClasspathFileReader
{
	private static ClasspathFileReader instance;

	private ClasspathFileReader()
	{}

	public static ClasspathFileReader instance()
	{
		if (instance == null)
		{
			return new ClasspathFileReader();
		} else
		{
			return instance;
		}
	}

	public List<String> readLines(String fileName) throws IOException
	{
		BufferedReader file = getBufferedReader(fileName);

		List<String> allLines = new ArrayList<>();
		String line;
		while ((line = file.readLine()) != null)
		{
			allLines.add(line);
		}
		return allLines;
	}

	public String read(String fileName)
	{
		BufferedReader file = getBufferedReader(fileName);

		return file.lines()
				.collect(Collectors.joining());
	}

	@SuppressWarnings("all")
	private BufferedReader getBufferedReader(String fileName)
	{
		Class thiz = this.getClass();
		InputStream in = thiz.getClassLoader()
				.getResourceAsStream(fileName);
		BufferedReader file = new BufferedReader(
				new InputStreamReader(in));
		return file;
	}
}
