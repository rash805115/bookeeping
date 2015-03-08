package database.service.neo4jembedded.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import database.service.UserService;
import exception.DuplicateUser;
import exception.UserNotFound;

public class UserServiceTest
{
	private UserService userService;
	
	@Before
	public void setup()
	{
		this.userService = new UserServiceImpl();
	}
	
	@After
	public void tearDown()
	{
		this.userService = null;
	}
	
	@Test
	public void testUserServiceImpl() throws UserNotFound, DuplicateUser
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		long oldUserCount = this.userService.countUsers();
		this.userService.createNewUser(userId, userProperties);
		long newUserCount = this.userService.countUsers();
		assertEquals(oldUserCount + 1, newUserCount);
		
		Map<String, Object> retrievedUserProperties = this.userService.getUser(userId);
		assertEquals(userProperties.size() + 2, retrievedUserProperties.size());
	}
	
	/*@Test
	public void testCreateNewUser() throws UserNotFound
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		try
		{
			long oldUserCount = this.userService.countUsers();
			this.userService.createNewUser(userId, userProperties);
			long newUserCount = this.userService.countUsers();
			this.userService.removeUser(userId);
			
			assertEquals(oldUserCount + 1, newUserCount);
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}
	
	@Test
	public void testCountUsers()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		try
		{
			long oldUserCount = this.userService.countUsers();
			this.userService.createNewUser(userId, userProperties);
			long newUserCount = this.userService.countUsers();
			this.userService.removeUser(userId);
			
			assertEquals(oldUserCount + 1, newUserCount);
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}
	
	@Test
	public void testRemoveUser()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		try
		{
			this.userService.createNewUser(userId, userProperties);
			long oldUserCount = this.userService.countUsers();
			this.userService.removeUser(userId);
			long newUserCount = this.userService.countUsers();
			
			assertEquals(oldUserCount - 1, newUserCount);
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}
	
	@Test
	public void testGetUser()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		try
		{
			this.userService.createNewUser(userId, userProperties);
			Map<String, Object> map = this.userService.getUser(userId);
			this.userService.removeUser(userId);
			assertEquals(userProperties.size() + 2, map.size());
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}
	
	@Test
	public void testGetUsersByMatchingAllProperty()
	{
		String userId1 = "testUser1";
		Map<String, Object> userProperties1 = new HashMap<String, Object>();
		userProperties1.put("firstName", "Test1");
		userProperties1.put("lastName", "User");
		userProperties1.put("email", "testuser@test.com");
		
		String userId2 = "testUser2";
		Map<String, Object> userProperties2 = new HashMap<String, Object>();
		userProperties2.put("firstName", "Test1");
		userProperties2.put("lastName", "User");
		userProperties2.put("email", "testuser@test.com");
		
		String userId3 = "testUser3";
		Map<String, Object> userProperties3 = new HashMap<String, Object>();
		userProperties3.put("firstName", "Test2");
		userProperties3.put("lastName", "User");
		userProperties3.put("email", "testuser@test.com");
		
		Map<String, Object> matchingProperties = new HashMap<String, Object>();
		matchingProperties.put("firstName", "Test1");
		matchingProperties.put("lastName", "User");
		
		try
		{
			this.userService.createNewUser(userId1, userProperties1);
			this.userService.createNewUser(userId2, userProperties2);
			this.userService.createNewUser(userId3, userProperties3);
			
			List<Map<String, Object>> matchResults1 = this.userService.getUsersByMatchingAllProperty(matchingProperties);
			List<Map<String, Object>> matchResults2 = this.userService.getUsersByMatchingAllProperty(new HashMap<String, Object>());
			
			this.userService.removeUser(userId1);
			this.userService.removeUser(userId2);
			this.userService.removeUser(userId3);
			
			assertEquals(matchResults1.size() + 1, matchResults2.size());
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}
	
	@Test
	public void testGetUsersByMatchingAnyProperty()
	{
		String userId1 = "testUser1";
		Map<String, Object> userProperties1 = new HashMap<String, Object>();
		userProperties1.put("firstName", "Test1");
		userProperties1.put("lastName", "User");
		userProperties1.put("email", "testuser@test.com");
		
		String userId2 = "testUser2";
		Map<String, Object> userProperties2 = new HashMap<String, Object>();
		userProperties2.put("firstName", "Test1");
		userProperties2.put("lastName", "User");
		userProperties2.put("email", "testuser@test.com");
		
		String userId3 = "testUser3";
		Map<String, Object> userProperties3 = new HashMap<String, Object>();
		userProperties3.put("firstName", "Test2");
		userProperties3.put("lastName", "User");
		userProperties3.put("email", "testuser@test.com");
		
		Map<String, Object> matchingProperties = new HashMap<String, Object>();
		matchingProperties.put("firstName", "Test1");
		matchingProperties.put("lastName", "User");
		
		try
		{
			this.userService.createNewUser(userId1, userProperties1);
			this.userService.createNewUser(userId2, userProperties2);
			this.userService.createNewUser(userId3, userProperties3);
			
			List<Map<String, Object>> matchResults1 = this.userService.getUsersByMatchingAnyProperty(matchingProperties);
			List<Map<String, Object>> matchResults2 = this.userService.getUsersByMatchingAnyProperty(new HashMap<String, Object>());
			
			this.userService.removeUser(userId1);
			this.userService.removeUser(userId2);
			this.userService.removeUser(userId3);
			
			assertEquals(matchResults1.size(), matchResults2.size());
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}*/
}
