package exception;

@SuppressWarnings("serial")
public class FilesystemNotFound extends Exception
{
	public FilesystemNotFound()
	{
		super();
	}
	
	public FilesystemNotFound(String message)
	{
		super(message);
	}
	
	public FilesystemNotFound(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public FilesystemNotFound(Throwable throwable)
	{
		super(throwable);
	}
}