package database.service.impl;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import utilities.configurationproperties.DatabaseConnectionProperty;
import database.service.DatabaseService;

public class Neo4JRestServiceImpl implements DatabaseService
{
	private RestGraphDatabase restGraphDatabase;
	private RestCypherQueryEngine restCypherQueryEngine;
	
	public Neo4JRestServiceImpl()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String restEndpoint = databaseConnectionProperty.getProperty("Neo4JRestEndpoint");
		
		this.restGraphDatabase = new RestGraphDatabase(restEndpoint);
		this.restCypherQueryEngine = new RestCypherQueryEngine(this.restGraphDatabase.getRestAPI());
	}

	@Override
	public long getNextAutoIncrement()
	{
		QueryResult<Map<String,Object>> queryResult = this.restCypherQueryEngine.query("match (autoIncrement:AutoIncrement) return autoIncrement.next", null);
		Map<String, Object> nextAutoIncrement = null;
		Iterator<Map<String, Object>> result = queryResult.iterator();
		while(result.hasNext())
		{
			nextAutoIncrement = result.next();
		}
		
		if(nextAutoIncrement == null)
		{
			try(Transaction transaction = this.restGraphDatabase.beginTx())
			{
				this.restCypherQueryEngine.query("create (autoIncrement:AutoIncrement {next: 0})", null);
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
			try(Transaction transaction = this.restGraphDatabase.beginTx())
			{
				this.restCypherQueryEngine.query("match (autoIncrement:AutoIncrement) set autoIncrement.next = autoIncrement.next + 1", null);
				transaction.success();
				Integer nextIncrement = (Integer) nextAutoIncrement.get("autoIncrement.next");
				return nextIncrement.longValue();
			}
			catch(Exception exception)
			{
				exception.printStackTrace();
				return -1;
			}
		}
	}
}
