package database.connection.singleton;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import utilities.configurationproperties.DatabaseConnectionProperty;

public class Neo4JRestConnection
{
	private static Neo4JRestConnection neo4jRestConnection;
	private GraphDatabaseService graphDatabaseService;
	private RestCypherQueryEngine restCypherQueryEngine;
	
	private Neo4JRestConnection()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String restEndpoint = databaseConnectionProperty.getProperty("Neo4JRestEndpoint");
		
		RestGraphDatabase restGraphDatabase = new RestGraphDatabase(restEndpoint);
		this.graphDatabaseService = restGraphDatabase;
		this.restCypherQueryEngine = new RestCypherQueryEngine(restGraphDatabase.getRestAPI());
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
	
	public static Neo4JRestConnection getInstance()
	{
		if(Neo4JRestConnection.neo4jRestConnection == null)
		{
			Neo4JRestConnection.neo4jRestConnection = new Neo4JRestConnection();
		}
		
		return Neo4JRestConnection.neo4jRestConnection;
	}
	
	public GraphDatabaseService getGraphDatabaseServiceObject()
	{
		return this.graphDatabaseService;
	}
	
	public Iterator<Map<String, Object>> runCypherQuery(String cypherQuery, Map<String,Object> queryParameters)
	{
		QueryResult<Map<String,Object>> queryResult = this.restCypherQueryEngine.query(cypherQuery, queryParameters);
		return queryResult.iterator();
	}
}
