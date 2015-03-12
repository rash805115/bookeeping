package database.service.titancassandraembedded.impl;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import database.service.DirectoryService;
import database.service.FileService;
import database.service.FilesystemService;
import database.service.UserService;
import database.titan.VersionChangeType;
import exception.DirectoryNotFound;
import exception.DuplicateDirectory;
import exception.DuplicateFile;
import exception.DuplicateFilesystem;
import exception.DuplicateUser;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class FileServiceTest
{
	private FileService fileService;
	private DirectoryService directoryService;
	private FilesystemService filesystemService;
	private UserService userService;
	
	@Before
	public void setup()
	{
		this.fileService = new FileServiceImpl();
		this.directoryService = new DirectoryServiceImpl();
		this.filesystemService = new FilesystemServiceImpl();
		this.userService = new UserServiceImpl();
	}
	
	@After
	public void tearDown()
	{
		this.fileService = null;
		this.directoryService = null;
		this.filesystemService = null;
		this.userService = null;
	}
	
	@Test
	public void testFileServiceImpl() throws DuplicateUser, UserNotFound, DuplicateFilesystem, FilesystemNotFound, DirectoryNotFound, DuplicateFile, FileNotFound, DuplicateDirectory, VersionNotFound
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
		
		String directoryPath = "/";
		String directoryName = "testFolder";
		Map<String, Object> directoryProperties = new HashMap<String, Object>();
		directoryProperties.put("sourceLocation", "testUser:/home/testUser/bookeeping/testFolder");
		directoryProperties.put("backupLocations", "[http://googledrive.com/testUser/testFolder, http://dropbox.com/testUser/testFolder]");
		directoryProperties.put("dateCreated", new Date().toString());
		
		String directoryPath2 = "/testFolder2";
		String directoryName2 = "testFolder";
		Map<String, Object> directoryProperties2 = new HashMap<String, Object>();
		directoryProperties2.put("sourceLocation", "testUser:/home/testUser/bookeeping/testFolder2/testFolder");
		directoryProperties2.put("backupLocations", "[http://googledrive.com/testUser/testFolder2/testFolder, http://dropbox.com/testUser/testFolder2/testFolder]");
		directoryProperties2.put("dateCreated", new Date().toString());
		
		String filePath = "/testFolder";
		String fileName = "test.txt";
		Map<String, Object> fileProperties = new HashMap<String, Object>();
		fileProperties.put("sourceLocation", "testUser:/home/testUser/bookeeping/test.txt");
		fileProperties.put("backupLocations", "[http://googledrive.com/testUser/file1.txt, http://dropbox.com/testUser/file1.txt]");
		fileProperties.put("dateCreated", new Date().toString());
		
		this.userService.createNewUser(userId, userProperties);
		this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
		this.directoryService.createNewDirectory(directoryPath, directoryName, filesystemId, userId, directoryProperties);
		this.directoryService.createNewDirectory(directoryPath2, directoryName2, filesystemId, userId, directoryProperties2);
		this.fileService.createNewFile(filePath, fileName, filesystemId, userId, fileProperties);
		
		Map<String, Object> changeMetadata = new HashMap<String, Object>();
		changeMetadata.put("versionChangeType", VersionChangeType.MODIFY.name());
		changeMetadata.put("dateChanged", new Date().toString());
		
		Map<String, Object> changedProperties = new HashMap<String, Object>();
		changedProperties.put("backupLocations", "[http://googledrive.com/testUser/file1_mod.txt, http://dropbox.com/testUser/file1_mod.txt]");
		changedProperties.put("testProperty", "Test Property");
		
		this.fileService.createNewVersion(userId, filesystemId, filePath, fileName, changeMetadata, changedProperties);
		
		Map<String, Object> retrievedFileV0Properties = this.fileService.getFile(userId, filesystemId, filePath, fileName, 0);
		Map<String, Object> retrievedFileV1Properties = this.fileService.getFile(userId, filesystemId, filePath, fileName, -1);
		
		assertEquals(fileProperties.size() + 4, retrievedFileV0Properties.size());
		assertEquals(fileProperties.size() + 5, retrievedFileV1Properties.size());
		
		this.fileService.deleteFileTemporarily(userId, filesystemId, filePath, fileName);
		
		this.filesystemService.createNewVersion(userId, filesystemId, changeMetadata, new HashMap<String, Object>());
		this.filesystemService.deleteFilesystemTemporarily(userId, filesystemId);
		this.filesystemService.restoreTemporaryDeletedFilesystem(userId, filesystemId);
	}
}
