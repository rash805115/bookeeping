package exception;

@SuppressWarnings("serial")
public class DirectoryNotFound extends Exception
{
	public DirectoryNotFound()
	{
		super();
	}
	
	public DirectoryNotFound(String message)
	{
		super(message);
	}
	
	public DirectoryNotFound(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public DirectoryNotFound(Throwable throwable)
	{
		super(throwable);
	}
}