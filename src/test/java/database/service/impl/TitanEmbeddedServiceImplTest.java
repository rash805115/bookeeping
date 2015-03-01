package database.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import database.service.DatabaseService;
import exception.DuplicateUser;
import exception.UserNotFound;

public class TitanEmbeddedServiceImplTest
{
	private DatabaseService databaseService;
	
	@Before
	public void setup()
	{
		this.databaseService = new TitanEmbeddedServiceImpl();
	}
	
	@After
	public void tearDown()
	{
		this.databaseService = null;
	}
	
	@Test
	public void testGetNextAutoIncrement()
	{
		long nextAutoIncrement1 = this.databaseService.getNextAutoIncrement();
		long nextAutoIncrement2 = this.databaseService.getNextAutoIncrement();
		assertEquals(nextAutoIncrement1 + 1, nextAutoIncrement2);
	}
	
	@Test
	public void testUserSetGet()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		try
		{
			this.databaseService.getUser(userId);
			assertFalse("Expected Exception UserNotFound. Got none!", true);
		}
		catch(UserNotFound userNotFound)
		{
			assertTrue(true);
		}
		
		try
		{
			this.databaseService.createNewUser(userId, userProperties);
			assertTrue(true);
		}
		catch(DuplicateUser duplicateUser)
		{
			assertFalse(duplicateUser.getMessage(), true);
		}
		
		try
		{
			this.databaseService.createNewUser(userId, userProperties);
			assertFalse("Expected Exception DuplicateUser. Got none!", true);
		}
		catch(DuplicateUser duplicateUser)
		{
			assertTrue(true);
		}
		
		try
		{
			Map<String, Object> retrievedUserProperties = this.databaseService.getUser(userId);
			for(Entry<String, Object> entry : userProperties.entrySet())
			{
				String key = entry.getKey();
				Object value = entry.getValue();
				
				assertTrue(value.equals(retrievedUserProperties.get(key)));
			}
		}
		catch(UserNotFound userNotFound)
		{
			assertFalse(userNotFound.getMessage(), true);
		}
	}
}
