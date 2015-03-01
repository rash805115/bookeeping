package database.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import database.service.DatabaseService;
import exception.DuplicateUser;
import exception.UserNotFound;

public class TitanEmbeddedServiceImplTest
{
	private static DatabaseService databaseService;
	
	@BeforeClass
	public static void globalSetup()
	{
		TitanEmbeddedServiceImplTest.databaseService = new TitanEmbeddedServiceImpl();
	}
	
	@AfterClass
	public static void globalTearDown()
	{
		TitanEmbeddedServiceImplTest.databaseService = null;
	}
	
	@Test
	public void testGetNextAutoIncrement()
	{
		long nextAutoIncrement1 = TitanEmbeddedServiceImplTest.databaseService.getNextAutoIncrement();
		long nextAutoIncrement2 = TitanEmbeddedServiceImplTest.databaseService.getNextAutoIncrement();
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
			TitanEmbeddedServiceImplTest.databaseService.getUser(userId);
			assertFalse("Expected Exception UserNotFound. Got none!", true);
		}
		catch(UserNotFound userNotFound)
		{
			assertTrue(true);
		}
		
		try
		{
			TitanEmbeddedServiceImplTest.databaseService.createNewUser(userId, userProperties);
			assertTrue(true);
		}
		catch(DuplicateUser duplicateUser)
		{
			assertFalse(duplicateUser.getMessage(), true);
		}
		
		try
		{
			TitanEmbeddedServiceImplTest.databaseService.createNewUser(userId, userProperties);
			assertFalse("Expected Exception DuplicateUser. Got none!", true);
		}
		catch(DuplicateUser duplicateUser)
		{
			assertTrue(true);
		}
		
		try
		{
			Map<String, Object> retrievedUserProperties = TitanEmbeddedServiceImplTest.databaseService.getUser(userId);
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
