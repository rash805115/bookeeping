package database.service.impl;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
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
}
