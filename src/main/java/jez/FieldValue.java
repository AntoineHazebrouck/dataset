package jez;

import java.util.Objects;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class FieldValue
{
	private final Object value;

	public static FieldValue of(Object value)
	{
		return new FieldValue(value);
	}

	@SuppressWarnings("unchecked")
	public <T> T get()
	{
		return (T) value;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			if (value.getClass() == obj.getClass())
				return Objects.equals(value, obj);
			else
				return false;
		FieldValue other = (FieldValue) obj;
		if (value == null)
		{
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return value.toString();
	}
}
