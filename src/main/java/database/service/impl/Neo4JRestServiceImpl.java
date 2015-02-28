package database.service.impl;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.rest.graphdb.RestGraphDatabase;

import utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.TitanException;

import database.service.DatabaseService;

public class Neo4JRestServiceImpl implements DatabaseService
{
	private enum Labels implements Label
	{
		AutoIncrement
	}
	
	private GraphDatabaseService graphDatabaseService;
	
	public Neo4JRestServiceImpl()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String restEndpoint = databaseConnectionProperty.getProperty("Neo4JRestEndpoint");
		
		this.graphDatabaseService = new RestGraphDatabase(restEndpoint);
		this.setupGraph();
	}
	
	private void setupGraph()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			AutoIndexer<Node> autoIndexer = this.graphDatabaseService.index().getNodeAutoIndexer();
			autoIndexer.startAutoIndexingProperty("nodeId");
			autoIndexer.setEnabled(true);
			
			transaction.success();
		}
		catch(TitanException titanException)
		{
			System.out.println("ERROR: Unable to setup neo4j-rest graph.");
			titanException.printStackTrace();
			System.exit(1);
		}
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
