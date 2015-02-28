package exception;

@SuppressWarnings("serial")
public class DuplicateUser extends Exception
{
	public DuplicateUser()
	{
		super();
	}
	
	public DuplicateUser(String message)
	{
		super(message);
	}
	
	public DuplicateUser(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public DuplicateUser(Throwable throwable)
	{
		super(throwable);
	}
}