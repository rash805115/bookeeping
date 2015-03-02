package database.service.neo4jrest.impl;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import database.service.AutoIncrementService;

public class AutoIncrementServiceTest
{
	private AutoIncrementService autoIncrementService;
	
	@Before
	public void setup()
	{
		this.autoIncrementService = new AutoIncrementServiceImpl();
	}
	
	@After
	public void tearDown()
	{
		this.autoIncrementService = null;
	}
	
	@Test
	public void testGetNextAutoIncrement()
	{
		long nextAutoIncrement1 = this.autoIncrementService.getNextAutoIncrement();
		long nextAutoIncrement2 = this.autoIncrementService.getNextAutoIncrement();
		
		assertEquals(nextAutoIncrement1 + 1, nextAutoIncrement2);
	}
}
