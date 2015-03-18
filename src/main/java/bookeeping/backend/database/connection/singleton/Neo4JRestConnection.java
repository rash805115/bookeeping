package bookeeping.backend.database.connection.singleton;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.utilities.configurationproperties.DatabaseConnectionProperty;

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
		this.setupPreRequisites();
	}
	
	private void setupGraph()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			AutoIndexer<Node> autoIndexer = this.graphDatabaseService.index().getNodeAutoIndexer();
			autoIndexer.startAutoIndexingProperty(MandatoryProperties.nodeId.name());
			autoIndexer.startAutoIndexingProperty(MandatoryProperties.userId.name());
			autoIndexer.setEnabled(true);
			
			transaction.success();
		}
	}
	
	private void setupPreRequisites()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			if(readableIndex.get(MandatoryProperties.nodeId.name(), "0").getSingle() == null)
			{
				Node autoIncrement = this.graphDatabaseService.createNode(NodeLabels.AutoIncrement);
				autoIncrement.setProperty(MandatoryProperties.nodeId.name(), "0");
				autoIncrement.setProperty(MandatoryProperties.next.name(), "2");
			}
			
			if(readableIndex.get(MandatoryProperties.nodeId.name(), "1").getSingle() == null)
			{
				Node user = this.graphDatabaseService.createNode(NodeLabels.User);
				user.setProperty(MandatoryProperties.nodeId.name(), "1");
				user.setProperty(MandatoryProperties.userId.name(), "public");
			}
			
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
