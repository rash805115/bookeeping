package bookeeping.backend.exception;

@SuppressWarnings("serial")
public class NodeUnavailable extends Exception
{
	public NodeUnavailable()
	{
		super();
	}
	
	public NodeUnavailable(String message)
	{
		super(message);
	}
	
	public NodeUnavailable(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public NodeUnavailable(Throwable throwable)
	{
		super(throwable);
	}
}