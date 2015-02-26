package database.service.impl;

import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
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
		ExecutionResult executionResult = this.executionEngine.execute("match (autoIncrement:AutoIncrement) return autoIncrement.next");
		List<String> nextAutoIncrement = executionResult.columns();
		
		if(nextAutoIncrement.size() != 1)
		{
			try(Transaction transaction = this.graphDatabaseService.beginTx())
			{
				executionResult = this.executionEngine.execute("create (autoIncrement:AutoIncrement {next: 0})");
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
				executionResult = this.executionEngine.execute("match (autoIncrement:AutoIncrement) set autoIncrement.next = autoIncrement.next + 1");
				transaction.success();
				return Long.parseLong(nextAutoIncrement.get(0));
			}
			catch(Exception exception)
			{
				exception.printStackTrace();
				return -1;
			}
		}
	}
	
	public static void main(String args[])
	{
		new Neo4JEmbeddedServiceImpl();
	}
}