package bookeeping.backend.database.service.neo4jembedded.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JEmbeddedConnection;
import bookeeping.backend.database.service.AutoIncrementService;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.utilities.AlphaNumericOperation;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public AutoIncrementServiceImpl()
	{
		this.graphDatabaseService = Neo4JEmbeddedConnection.getInstance().getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String getNextAutoIncrement()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node autoIncrement = null;
			try
			{
				autoIncrement = this.commonCode.getNode("0");
			}
			catch (NodeNotFound e) {}
			String nextAutoIncrement = (String) autoIncrement.getProperty(MandatoryProperties.next.name());
			autoIncrement.setProperty(MandatoryProperties.next.name(), AlphaNumericOperation.add(nextAutoIncrement, 1));
			
			transaction.success();
			return nextAutoIncrement;
		}
	}
}
