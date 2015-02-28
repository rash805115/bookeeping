package database.service.impl;

import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;

import utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

import database.service.DatabaseService;

public class TitanEmbeddedServiceImpl implements DatabaseService
{
	private TitanGraph titanGraph;
	
	public TitanEmbeddedServiceImpl()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseBackend = databaseConnectionProperty.getProperty("TitanEmbeddedServerBackend");
		String databaseHostname = databaseConnectionProperty.getProperty("TitanEmbeddedServerHostname");
		
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
			
			if(! this.titanGraph.containsPropertyKey("nodeIdPropertyKey"))
			{
				PropertyKey nodeIdPropertyKey = titanManagement.makePropertyKey("nodeIdPropertyKey").dataType(Long.class).make();
				titanManagement.buildIndex("nodeIdIndex", Vertex.class).addKey(nodeIdPropertyKey).unique().buildCompositeIndex();
			}
		}
		catch(TitanException titanException)
		{
			System.out.println("ERROR: Unable to setup titan graph.");
			titanException.printStackTrace();
			System.exit(1);
		}
		
		titanManagement.commit();
	}

	@Override
	public int getNextAutoIncrement()
	{
		Iterator<Vertex> iterator = this.titanGraph.getVertices("nodeId", 0).iterator();
		Vertex autoIncrement = null;
		while(iterator.hasNext())
		{
			autoIncrement = iterator.next();
		}
		
		if(autoIncrement == null)
		{
			try
			{
				this.titanGraph.newTransaction();
				
				Vertex vertex = this.titanGraph.addVertexWithLabel("AutoIncrement");
				vertex.setProperty("nodeId", 0);
				vertex.setProperty("next", 1);
				
				this.titanGraph.commit();
				return 0;
			}
			catch(TitanException titanException)
			{
				titanException.printStackTrace();
				return -1;
			}
		}
		else
		{
			try
			{
				this.titanGraph.newTransaction();
				
				int nextIncrement = autoIncrement.getProperty("next");
				autoIncrement.setProperty("next", nextIncrement + 1);
				
				this.titanGraph.commit();
				return nextIncrement;
			}
			catch(TitanException titanException)
			{
				titanException.printStackTrace();
				return -1;
			}
		}
	}
}
