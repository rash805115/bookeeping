package bookeeping.backend.exception;

@SuppressWarnings("serial")
public class FileNotFound extends Exception
{
	public FileNotFound()
	{
		super();
	}
	
	public FileNotFound(String message)
	{
		super(message);
	}
	
	public FileNotFound(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public FileNotFound(Throwable throwable)
	{
		super(throwable);
	}
}