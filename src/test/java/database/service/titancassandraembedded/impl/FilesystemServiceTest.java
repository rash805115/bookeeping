package database.service.titancassandraembedded.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import database.service.FilesystemService;
import database.service.UserService;
import exception.DuplicateFilesystem;
import exception.DuplicateUser;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class FilesystemServiceTest
{
	private FilesystemService filesystemService;
	private UserService userService;
	
	@Before
	public void setup()
	{
		this.filesystemService = new FilesystemServiceImpl();
		this.userService = new UserServiceImpl();
	}
	
	@After
	public void tearDown()
	{
		this.filesystemService = null;
		this.userService = null;
	}
	
	@Test
	public void testFilesystemServiceImpl() throws DuplicateUser, UserNotFound, DuplicateFilesystem, FilesystemNotFound, VersionNotFound
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		String filesystemId = "testFilesystem";
		Map<String, Object> filesystemProperties = new HashMap<String, Object>();
		filesystemProperties.put("sourceLocation", "testUser:/home/testUser/bookeeping");
		filesystemProperties.put("backupLocations", "[http://googledrive.com/testUser, http://dropbox.com/testUser]");
		filesystemProperties.put("dateCreated", new Date().toString());
		
		this.userService.createNewUser(userId, userProperties);
		this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
		Map<String, Object> retrievedFilesystemProperties = this.filesystemService.getFilesystem(userId, filesystemId, -1);
		assertEquals(filesystemProperties.size() + 3, retrievedFilesystemProperties.size());
	}
	
	/*@Test
	public void testCreateNewFilesystem()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		String filesystemId = "testFilesystem";
		Map<String, Object> filesystemProperties = new HashMap<String, Object>();
		filesystemProperties.put("sourceLocation", "testUser:/home/testUser/bookeeping");
		filesystemProperties.put("backupLocations", "[http://googledrive.com/testUser, http://dropbox.com/testUser]");
		filesystemProperties.put("dateCreated", new Date().toString());
		
		try
		{
			this.userService.createNewUser(userId, userProperties);
			this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
			assertTrue(true);
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
		
		try
		{
			this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
			assertFalse("ERROR: Expecting error DuplicateFilesystem. Got none!", true);
		}
		catch(Exception exception)
		{
			assertTrue(true);
		}
		
		try
		{
			this.filesystemService.removeFilesystem(userId, filesystemId);
			this.userService.removeUser(userId);
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}
	
	@Test
	public void testRemoveFilesystem()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		String filesystemId = "testFilesystem";
		Map<String, Object> filesystemProperties = new HashMap<String, Object>();
		filesystemProperties.put("sourceLocation", "testUser:/home/testUser/bookeeping");
		filesystemProperties.put("backupLocations", "[http://googledrive.com/testUser, http://dropbox.com/testUser]");
		filesystemProperties.put("dateCreated", new Date().toString());
		
		try
		{
			this.userService.createNewUser(userId, userProperties);
			this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
			
			if(this.filesystemService.getFilesystem(userId, filesystemId).size() > 0)
			{
				assertTrue(true);
			}
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
		
		try
		{
			this.filesystemService.removeFilesystem(userId, filesystemId);
		}
		catch(Exception exception)
		{
			
		}
		
		try
		{
			if(this.filesystemService.getFilesystem(userId, filesystemId).size() > 0)
			{
				assertFalse("ERROR: Not expecting any entity. Got one!", true);
			}
		}
		catch(Exception exception)
		{
			assertTrue(true);
		}
		
		try
		{
			this.userService.removeUser(userId);
		}
		catch(Exception exception)
		{
			assertTrue(true);
		}
	}
	
	@Test
	public void testGetFilesystem()
	{
		String userId = "testUser";
		Map<String, Object> userProperties = new HashMap<String, Object>();
		userProperties.put("firstName", "Test");
		userProperties.put("lastName", "User");
		userProperties.put("email", "testuser@test.com");
		
		String filesystemId = "testFilesystem";
		Map<String, Object> filesystemProperties = new HashMap<String, Object>();
		filesystemProperties.put("sourceLocation", "testUser:/home/testUser/bookeeping");
		filesystemProperties.put("backupLocations", "[http://googledrive.com/testUser, http://dropbox.com/testUser]");
		filesystemProperties.put("dateCreated", new Date().toString());
		
		try
		{
			this.userService.createNewUser(userId, userProperties);
			this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
			
			Map<String, Object> retrievedFilesystem = this.filesystemService.getFilesystem(userId, filesystemId);
			if(retrievedFilesystem.size() > 0)
			{
				assertEquals(filesystemProperties.size() + 3, retrievedFilesystem.size());
			}
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
		
		try
		{
			this.filesystemService.removeFilesystem(userId, filesystemId);
			this.userService.removeUser(userId);
		}
		catch(Exception exception)
		{
			assertFalse(exception.getMessage(), true);
		}
	}*/
}
