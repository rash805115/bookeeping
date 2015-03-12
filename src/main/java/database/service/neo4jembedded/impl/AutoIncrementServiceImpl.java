package database.service.neo4jembedded.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

import utilities.AlphaNumericOperation;
import database.MandatoryProperties;
import database.connection.singleton.Neo4JEmbeddedConnection;
import database.service.AutoIncrementService;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private GraphDatabaseService graphDatabaseService;
	
	public AutoIncrementServiceImpl()
	{
		this.graphDatabaseService = Neo4JEmbeddedConnection.getInstance().getGraphDatabaseServiceObject();
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
