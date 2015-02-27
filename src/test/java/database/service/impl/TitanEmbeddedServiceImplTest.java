package database.service.impl;

import database.service.DatabaseService;
import junit.framework.TestCase;

public class TitanEmbeddedServiceImplTest extends TestCase
{
	private DatabaseService databaseService;
	
	public TitanEmbeddedServiceImplTest()
	{
		this.databaseService = new TitanEmbeddedServiceImpl();
	}
	
	public void testGetNextAutoIncrement()
	{
		assertTrue(this.databaseService.getNextAutoIncrement() >= 0);
	}
}
