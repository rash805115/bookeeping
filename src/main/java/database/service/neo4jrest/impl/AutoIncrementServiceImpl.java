package database.service.neo4jrest.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

import database.connection.singleton.Neo4JRestConnection;
import database.neo4j.NodeLabels;
import database.service.AutoIncrementService;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private GraphDatabaseService graphDatabaseService;
	
	public AutoIncrementServiceImpl()
	{
		this.graphDatabaseService = Neo4JRestConnection.getInstance().getGraphDatabaseServiceObject();
	}
	
	@Override
	public int getNextAutoIncrement()
	{
		Node autoIncrement = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			autoIncrement = readableIndex.get("nodeId", 0).getSingle();
			
			if(autoIncrement == null)
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.AutoIncrement);
				node.setProperty("nodeId", 0);
				node.setProperty("next", 2);
				
				transaction.success();
				return 1;
			}
			else
			{
				int nextAutoIncrement = (int) autoIncrement.getProperty("next");
				autoIncrement.setProperty("next", nextAutoIncrement + 1);
				
				transaction.success();
				return nextAutoIncrement;
			}
		}
	}
}
