package database.service.neo4jrest.impl;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import utilities.AlphaNumericOperation;
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
		String nextAutoIncrement1 = this.autoIncrementService.getNextAutoIncrement();
		String nextAutoIncrement2 = this.autoIncrementService.getNextAutoIncrement();
		
		assertEquals(AlphaNumericOperation.add(nextAutoIncrement1, 1), nextAutoIncrement2);
	}
}
