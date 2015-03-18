package bookeeping.backend.database.service.neo4jrest.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JRestConnection;
import bookeeping.backend.database.service.AutoIncrementService;
import bookeeping.backend.utilities.AlphaNumericOperation;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private GraphDatabaseService graphDatabaseService;
	
	public AutoIncrementServiceImpl()
	{
		this.graphDatabaseService = Neo4JRestConnection.getInstance().getGraphDatabaseServiceObject();
	}
	
	@Override
	public String getNextAutoIncrement()
	{
		Node autoIncrement = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			autoIncrement = readableIndex.get(MandatoryProperties.nodeId.name(), "0").getSingle();
			String nextAutoIncrement = (String) autoIncrement.getProperty(MandatoryProperties.next.name());
			autoIncrement.setProperty(MandatoryProperties.next.name(), AlphaNumericOperation.add(nextAutoIncrement, 1));
			
			transaction.success();
			return nextAutoIncrement;
		}
	}
}
