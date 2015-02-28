package database.service.impl;

import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import utilities.configurationproperties.DatabaseConnectionProperty;
import database.service.DatabaseService;

public class Neo4JEmbeddedServiceImpl implements DatabaseService
{
	private GraphDatabaseService graphDatabaseService;
	private ExecutionEngine executionEngine;
	
	public Neo4JEmbeddedServiceImpl()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseLocation = databaseConnectionProperty.getProperty("Neo4JEmbeddedDatabaseLocation");
		
		this.graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(databaseLocation);
		this.executionEngine = new ExecutionEngine(this.graphDatabaseService);
	}

	@Override
	public long getNextAutoIncrement()
	{
		ExecutionResult executionResult = this.executionEngine.execute("match (autoIncrement:AutoIncrement {nodeId: 0}) return autoIncrement.next");
		Map<String, Object> nextAutoIncrement = null;
		ResourceIterator<Map<String, Object>> result = executionResult.iterator();
		while(result.hasNext())
		{
			nextAutoIncrement = result.next();
		}
		
		if(nextAutoIncrement == null)
		{
			try(Transaction transaction = this.graphDatabaseService.beginTx())
			{
				this.executionEngine.execute("create (autoIncrement:AutoIncrement {nodeId: 0, next: 1})");
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
				this.executionEngine.execute("match (autoIncrement:AutoIncrement {nodeId: 0}) set autoIncrement.next = autoIncrement.next + 1");
				transaction.success();
				return (long) nextAutoIncrement.get("autoIncrement.next");
			}
			catch(Exception exception)
			{
				exception.printStackTrace();
				return -1;
			}
		}
	}
}