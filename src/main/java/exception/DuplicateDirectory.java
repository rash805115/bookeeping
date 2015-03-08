package exception;

@SuppressWarnings("serial")
public class DuplicateDirectory extends Exception
{
	public DuplicateDirectory()
	{
		super();
	}
	
	public DuplicateDirectory(String message)
	{
		super(message);
	}
	
	public DuplicateDirectory(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public DuplicateDirectory(Throwable throwable)
	{
		super(throwable);
	}
}