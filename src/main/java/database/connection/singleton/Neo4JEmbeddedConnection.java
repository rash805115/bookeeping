package database.connection.singleton;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import utilities.configurationproperties.DatabaseConnectionProperty;

public class Neo4JEmbeddedConnection
{
	private static Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	private Neo4JEmbeddedConnection()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseLocation = databaseConnectionProperty.getProperty("Neo4JEmbeddedDatabaseLocation");
		
		this.graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(databaseLocation)
									.setConfig(GraphDatabaseSettings.node_keys_indexable, "nodeId")
									.setConfig(GraphDatabaseSettings.node_keys_indexable, "userId")
									.setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
									.newGraphDatabase();
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
}
