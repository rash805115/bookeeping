package database.service.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.BaseConfiguration;

import utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

import database.service.DatabaseService;
import exception.DuplicateUser;
import exception.UserNotFound;

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

	@Override
	public int getNextAutoIncrement()
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Iterator<Vertex> iterator = this.titanGraph.getVertices("nodeId", 0).iterator();
			Vertex autoIncrement = null;
			while(iterator.hasNext())
			{
				autoIncrement = iterator.next();
			}
			
			if(autoIncrement == null)
			{
				Vertex vertex = this.titanGraph.addVertexWithLabel("AutoIncrement");
				vertex.setProperty("nodeId", 0);
				vertex.setProperty("next", 2);
				
				titanTransaction.commit();
				return 1;
			}
			else
			{
				int nextIncrement = autoIncrement.getProperty("next");
				autoIncrement.setProperty("next", nextIncrement + 1);
				
				titanTransaction.commit();
				return nextIncrement;
			}
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}

	@Override
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Iterator<Vertex> iterator = this.titanGraph.getVertices("userId", userId).iterator();
			Vertex user = null;
			while(iterator.hasNext())
			{
				user = iterator.next();
			}
			
			if(user != null)
			{
				titanTransaction.commit();
				throw new DuplicateUser("ERROR: User already present! - \"" + userId + "\"");
			}
			else
			{
				Vertex vertex = this.titanGraph.addVertexWithLabel("User");
				vertex.setProperty("nodeId", this.getNextAutoIncrement());
				vertex.setProperty("userId", userId);
				
				for(Entry<String, Object> userPropertiesEntry : userProperties.entrySet())
				{
					vertex.setProperty(userPropertiesEntry.getKey(), userPropertiesEntry.getValue());
				}
				
				titanTransaction.commit();
			}
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}

	@Override
	public Map<String, Object> getUser(String userId) throws UserNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Iterator<Vertex> iterator = this.titanGraph.getVertices("userId", userId).iterator();
			Vertex user = null;
			while(iterator.hasNext())
			{
				user = iterator.next();
			}
			
			if(user == null)
			{
				titanTransaction.commit();
				throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
			}
			else
			{
				Map<String, Object> userProperties = new HashMap<String, Object>();
				
				Iterable<String> iterable = user.getPropertyKeys();
				for(String key : iterable)
				{
					userProperties.put(key, user.getProperty(key));
				}
				
				titanTransaction.commit();
				return userProperties;
			}
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
}
