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
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ExecutionResult executionResult = this.executionEngine.execute(cypherQuery, queryParameters);
			transaction.success();
			return executionResult.iterator();
		}
	}
}