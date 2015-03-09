package exception;

@SuppressWarnings("serial")
public class VersionNotFound extends Exception
{
	public VersionNotFound()
	{
		super();
	}
	
	public VersionNotFound(String message)
	{
		super(message);
	}
	
	public VersionNotFound(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public VersionNotFound(Throwable throwable)
	{
		super(throwable);
	}
}