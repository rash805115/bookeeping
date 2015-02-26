package database.service.impl;

import database.service.DatabaseService;
import junit.framework.TestCase;

public class Neo4JEmbeddedServiceImplTest extends TestCase
{
	private DatabaseService databaseService;
	
	public Neo4JEmbeddedServiceImplTest()
	{
		this.databaseService = new Neo4JEmbeddedServiceImpl();
	}
	
	public void testAutoIncrement()
	{
		System.out.println("Next AutoIncrement: " + this.databaseService.getNextAutoIncrement());
		assertTrue(this.databaseService.getNextAutoIncrement() > 0);
	}
}
