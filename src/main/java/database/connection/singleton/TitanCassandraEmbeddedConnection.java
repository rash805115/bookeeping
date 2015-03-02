package database.connection.singleton;

import org.apache.commons.configuration.BaseConfiguration;

import utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

public class TitanCassandraEmbeddedConnection
{
	private static TitanCassandraEmbeddedConnection titanCassandraEmbeddedConnection;
	private TitanGraph titanGraph;
	
	private TitanCassandraEmbeddedConnection()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseBackend = databaseConnectionProperty.getProperty("TitanCassandraEmbeddedServerBackend");
		String databaseHostname = databaseConnectionProperty.getProperty("TitanCassandraEmbeddedServerHostname");
		
		BaseConfiguration baseConfiguration = new BaseConfiguration();
		baseConfiguration.setProperty("storage.backend", databaseBackend);
		baseConfiguration.setProperty("storage.hostname", databaseHostname);
		
		this.titanGraph = TitanFactory.open(baseConfiguration);
		this.setupGraph();
	}
	
	private void setupGraph()
	{
		TitanManagement titanManagement = this.titanGraph.getManagementSystem();
		
		try
		{
			if(! this.titanGraph.containsVertexLabel("AutoIncrement"))
			{
				titanManagement.makeVertexLabel("AutoIncrement").make();
			}
			
			if(! this.titanGraph.containsVertexLabel("User"))
			{
				titanManagement.makeVertexLabel("User").make();
			}
			
			if(! this.titanGraph.containsPropertyKey("nodeId"))
			{
				PropertyKey nodeIdPropertyKey = titanManagement.makePropertyKey("nodeId").dataType(Integer.class).make();
				titanManagement.buildIndex("nodeIdIndex", Vertex.class).addKey(nodeIdPropertyKey).unique().buildCompositeIndex();
			}
			
			if(! this.titanGraph.containsPropertyKey("userId"))
			{
				PropertyKey userIdPropertyKey = titanManagement.makePropertyKey("userId").dataType(String.class).make();
				titanManagement.buildIndex("userIdIndex", Vertex.class).addKey(userIdPropertyKey).unique().buildCompositeIndex();
			}
			
			titanManagement.commit();
		}
		finally
		{
			if(titanManagement.isOpen())
			{
				titanManagement.rollback();
			}
		}
	}
	
	public static TitanCassandraEmbeddedConnection getInstance()
	{
		if(TitanCassandraEmbeddedConnection.titanCassandraEmbeddedConnection == null)
		{
			TitanCassandraEmbeddedConnection.titanCassandraEmbeddedConnection = new TitanCassandraEmbeddedConnection();
		}
		
		return TitanCassandraEmbeddedConnection.titanCassandraEmbeddedConnection;
	}
	
	public TitanGraph getTitanGraphObject()
	{
		return this.titanGraph;
	}
}
