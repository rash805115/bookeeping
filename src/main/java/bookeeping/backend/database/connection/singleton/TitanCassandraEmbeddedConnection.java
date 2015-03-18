package bookeeping.backend.database.connection.singleton;

import org.apache.commons.configuration.BaseConfiguration;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
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
		this.setupPreRequisites();
	}
	
	private void setupGraph()
	{
		TitanManagement titanManagement = this.titanGraph.getManagementSystem();
		
		try
		{
			for(NodeLabels nodeLabels : NodeLabels.values())
			{
				if(! this.titanGraph.containsVertexLabel(nodeLabels.name()))
				{
					titanManagement.makeVertexLabel(nodeLabels.name()).make();
				}
			}
			
			if(! this.titanGraph.containsPropertyKey(MandatoryProperties.nodeId.name()))
			{
				PropertyKey nodeIdPropertyKey = titanManagement.makePropertyKey(MandatoryProperties.nodeId.name()).dataType(String.class).make();
				titanManagement.buildIndex("nodeIdIndex", Vertex.class).addKey(nodeIdPropertyKey).unique().buildCompositeIndex();
			}
			
			if(! this.titanGraph.containsPropertyKey(MandatoryProperties.userId.name()))
			{
				PropertyKey userIdPropertyKey = titanManagement.makePropertyKey(MandatoryProperties.userId.name()).dataType(String.class).make();
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
	
	private void setupPreRequisites()
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			if(! this.titanGraph.getVertices(MandatoryProperties.nodeId.name(), "0").iterator().hasNext())
			{
				Vertex autoIncrement = this.titanGraph.addVertexWithLabel(NodeLabels.AutoIncrement.name());
				autoIncrement.setProperty(MandatoryProperties.nodeId.name(), "0");
				autoIncrement.setProperty(MandatoryProperties.next.name(), "2");
			}
			
			if(! this.titanGraph.getVertices(MandatoryProperties.nodeId.name(), "1").iterator().hasNext())
			{
				Vertex user = this.titanGraph.addVertexWithLabel(NodeLabels.User.name());
				user.setProperty(MandatoryProperties.nodeId.name(), "1");
				user.setProperty(MandatoryProperties.userId.name(), "public");
			}
			
			titanTransaction.commit();
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
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
