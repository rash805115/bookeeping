package bookeeping.backend.database.connection.singleton;

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

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.utilities.configurationproperties.DatabaseConnectionProperty;

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
