package bookeeping.backend.exception;

@SuppressWarnings("serial")
public class NodeNotFound extends Exception
{
	public NodeNotFound()
	{
		super();
	}
	
	public NodeNotFound(String message)
	{
		super(message);
	}
	
	public NodeNotFound(String message, Throwable throwable)
	{
		super(message, throwable);
	}
	
	public NodeNotFound(Throwable throwable)
	{
		super(throwable);
	}
}