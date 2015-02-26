package database.service.impl;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import utilities.configurationproperties.DatabaseConnectionProperty;
import database.service.DatabaseService;

public class Neo4JRestServiceImpl implements DatabaseService
{
	private RestGraphDatabase restGraphDatabase;
	private RestCypherQueryEngine restCypherQueryEngine;
	
	public Neo4JRestServiceImpl() throws FileNotFoundException, IOException
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String restEndpoint = databaseConnectionProperty.getProperty("Neo4JRestEndpoint");
		
		this.restGraphDatabase = new RestGraphDatabase(restEndpoint);
		this.restCypherQueryEngine = new RestCypherQueryEngine(this.restGraphDatabase.getRestAPI());
	}
}
