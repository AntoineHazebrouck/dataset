package jez.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class DataSet<T>
{
	protected final List<T> rows;

	public static <T> DataSet<T> of(Collection<T> elements)
	{
		return new DataSet<T>(new ArrayList<>(elements));
	}

	public int size()
	{
		return rows.size();
	}

	public T row(int index)
	{
		return rows.get(index);
	}

	public List<T> rows()
	{
		return this.rows;
	}

	public DataSet<T> where(Predicate<T> predicate)
	{
		List<T> filtered = rows().stream()
				.filter(predicate)
				.toList();
		return DataSet.of(filtered);
	}

	public DataSet<T> unique()
	{
		return DataSet.of(rows.stream()
				.distinct()
				.toList());
	}

	public DataSet<T> map(Function<T, T> mapping)
	{

		return DataFrame.of(rows.stream()
				.map(mapping)
				.toList());
	}

	public Optional<T> reduce(BinaryOperator<T> accumulator)
	{
		return this.rows()
				.stream()
				.reduce(accumulator);
	}
}
