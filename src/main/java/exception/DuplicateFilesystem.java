package exception;

@SuppressWarnings("serial")
public class DuplicateFilesystem extends Exception
{
	public DuplicateFilesystem()
	{
		super();
	}
	
	public DuplicateFilesystem(String message)
	{
		super(message);
	}
	
	public DuplicateFilesystem(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public DuplicateFilesystem(Throwable throwable)
	{
		super(throwable);
	}
}