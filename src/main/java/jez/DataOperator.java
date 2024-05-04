package jez;

public class DataOperator
{
	private static DataOperator instance;

	private DataOperator()
	{}

	public static DataOperator instance() {
		if (instance == null) {
			return new DataOperator();
		} else {
			return instance;
		}
	}
}
