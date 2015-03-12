package database.connection.singleton;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.ReadableIndex;

import database.neo4j.NodeLabels;
import utilities.configurationproperties.DatabaseConnectionProperty;

public class Neo4JEmbeddedConnection
{
	private static Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	private ExecutionEngine executionEngine;
	
	private Neo4JEmbeddedConnection()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseLocation = databaseConnectionProperty.getProperty("Neo4JEmbeddedDatabaseLocation");
		
		this.graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(databaseLocation);
		this.executionEngine = new ExecutionEngine(this.graphDatabaseService);
		this.setupGraph();
		this.setupPreRequisites();
	}
	
	private void setupGraph()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			AutoIndexer<Node> autoIndexer = this.graphDatabaseService.index().getNodeAutoIndexer();
			autoIndexer.startAutoIndexingProperty("nodeId");
			autoIndexer.startAutoIndexingProperty("userId");
			autoIndexer.setEnabled(true);
			
			transaction.success();
		}
	}
	
	private void setupPreRequisites()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			if(readableIndex.get("nodeId", "0").getSingle() == null)
			{
				Node autoIncrement = this.graphDatabaseService.createNode(NodeLabels.AutoIncrement);
				autoIncrement.setProperty("nodeId", "0");
				autoIncrement.setProperty("next", "2");
			}
			
			if(readableIndex.get("nodeId", "1").getSingle() == null)
			{
				Node user = this.graphDatabaseService.createNode(NodeLabels.User);
				user.setProperty("nodeId", "1");
				user.setProperty("userId", "public");
			}
			
			transaction.success();
		}
	}
	
	public static Neo4JEmbeddedConnection getInstance()
	{
		if(Neo4JEmbeddedConnection.neo4jEmbeddedConnection == null)
		{
			Neo4JEmbeddedConnection.neo4jEmbeddedConnection = new Neo4JEmbeddedConnection();
		}
		
		return Neo4JEmbeddedConnection.neo4jEmbeddedConnection;
	}
	
	public GraphDatabaseService getGraphDatabaseServiceObject()
	{
		return this.graphDatabaseService;
	}
	
	public Iterator<Map<String, Object>> runCypherQuery(String cypherQuery, Map<String,Object> queryParameters)
	{
		ExecutionResult executionResult = this.executionEngine.execute(cypherQuery, queryParameters);
		return executionResult.iterator();
	}
}
