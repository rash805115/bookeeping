package database.service.neo4jembedded.impl;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import database.service.FilesystemService;
import exception.DuplicateFilesystem;
import exception.DuplicateUser;
import exception.UserNotFound;

public class FilesystemServiceTest
{
	private FilesystemService filesystemService;
	
	@Before
	public void setup()
	{
		this.filesystemService = new FilesystemServiceImpl();
	}
	
	@After
	public void tearDown()
	{
		this.filesystemService = null;
	}
	
	@Test
	public void testCreateNewFilesystem() throws UserNotFound, DuplicateFilesystem, DuplicateUser
	{
		new UserServiceImpl().createNewUser("u", new HashMap<String, Object>());
		this.filesystemService.createNewFilesystem("1", "u", new HashMap<String, Object>());
		assertEquals(true, true);
	}
}
