package database.service.impl;

import database.service.DatabaseService;
import junit.framework.TestCase;

public class Neo4JRestServiceImplTest extends TestCase
{
	private DatabaseService databaseService;
	
	public Neo4JRestServiceImplTest()
	{
		this.databaseService = new Neo4JRestServiceImpl();
	}
	
	public void testGetNextAutoIncrement()
	{
		assertTrue(this.databaseService.getNextAutoIncrement() >= 0);
	}
}
