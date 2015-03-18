package bookeeping.backend.exception;

@SuppressWarnings("serial")
public class UserNotFound extends Exception
{
	public UserNotFound()
	{
		super();
	}
	
	public UserNotFound(String message)
	{
		super(message);
	}
	
	public UserNotFound(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public UserNotFound(Throwable throwable)
	{
		super(throwable);
	}
}