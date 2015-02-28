package database.service.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.ReadableIndex;

import utilities.configurationproperties.DatabaseConnectionProperty;
import database.service.DatabaseService;

public class Neo4JEmbeddedServiceImpl implements DatabaseService
{
	private enum Labels implements Label
	{
		AutoIncrement
	}
	
	private GraphDatabaseService graphDatabaseService;
	
	public Neo4JEmbeddedServiceImpl()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseLocation = databaseConnectionProperty.getProperty("Neo4JEmbeddedDatabaseLocation");
		
		this.graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(databaseLocation)
									.setConfig(GraphDatabaseSettings.node_keys_indexable, "nodeId")
									.setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
									.newGraphDatabase();
	}

	@Override
	public long getNextAutoIncrement()
	{
		Node autoIncrement = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			autoIncrement = readableIndex.get("nodeId", 0).getSingle();
		}
		catch(Exception exception)
		{
			exception.printStackTrace();
			return -1;
		}
		
		if(autoIncrement == null)
		{
			try(Transaction transaction = this.graphDatabaseService.beginTx())
			{
				Node node = this.graphDatabaseService.createNode(Labels.AutoIncrement);
				node.setProperty("nodeId", 0);
				node.setProperty("next", 1);
				
				transaction.success();
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
			try(Transaction transaction = this.graphDatabaseService.beginTx())
			{
				Integer nextAutoIncrement = (Integer) autoIncrement.getProperty("next");
				autoIncrement.setProperty("next", nextAutoIncrement + 1);
				
				transaction.success();
				return nextAutoIncrement.longValue();
			}
			catch(Exception exception)
			{
				exception.printStackTrace();
				return -1;
			}
		}
	}
}