package exception;

@SuppressWarnings("serial")
public class DuplicateFile extends Exception
{
	public DuplicateFile()
	{
		super();
	}
	
	public DuplicateFile(String message)
	{
		super(message);
	}
	
	public DuplicateFile(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public DuplicateFile(Throwable throwable)
	{
		super(throwable);
	}
}