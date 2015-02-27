package database.service.impl;

import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;

import utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

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
	}

	@Override
	public long getNextAutoIncrement()
	{
		//get the autoIncrement vertex. This one right here is just a dummy.
		//what should be the next type of gremlinPipeline
		//will transaction throw an error on failure?
		//will value returned from next increment be an int or long?
		Iterator<Vertex> iterator = new GremlinPipeline<Vertex, Iterator<Vertex>>().start(this.titanGraph.getVertex(0)).iterator();
		Vertex nextAutoIncrement = null;
		while(iterator.hasNext())
		{
			nextAutoIncrement = iterator.next();
		}
		
		if(nextAutoIncrement == null)
		{
			try
			{
				this.titanGraph.buildTransaction();
				
				Vertex vertex = this.titanGraph.addVertex(null);
				vertex.setProperty("next", 0);
				
				this.titanGraph.commit();
				
				return 0;
			}
			catch(Exception exception)
			{
				exception.printStackTrace();
				return -1;
			}
		}
		else
		{
			try
			{
				this.titanGraph.buildTransaction();
				
				long nextIncrement = nextAutoIncrement.getProperty("next");
				nextAutoIncrement.setProperty("next", nextIncrement + 1);
				
				this.titanGraph.commit();
				
				return nextIncrement;
			}
			catch(Exception exception)
			{
				exception.printStackTrace();
				return -1;
			}
		}
	}
}
