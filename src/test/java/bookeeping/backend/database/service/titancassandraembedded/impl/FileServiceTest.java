package bookeeping.backend.database.service.titancassandraembedded.impl;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bookeeping.backend.database.VersionChangeType;
import bookeeping.backend.database.service.DirectoryService;
import bookeeping.backend.database.service.FileService;
import bookeeping.backend.database.service.FilesystemService;
import bookeeping.backend.database.service.UserService;
import bookeeping.backend.database.service.titancassandraembedded.impl.DirectoryServiceImpl;
import bookeeping.backend.database.service.titancassandraembedded.impl.FileServiceImpl;
import bookeeping.backend.database.service.titancassandraembedded.impl.FilesystemServiceImpl;
import bookeeping.backend.database.service.titancassandraembedded.impl.UserServiceImpl;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateDirectory;
import bookeeping.backend.exception.DuplicateFile;
import bookeeping.backend.exception.DuplicateFilesystem;
import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;
import bookeeping.backend.file.FilePermission;

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
		
		String commitId = "commit1";
		
		this.userService.createNewUser(userId, userProperties);
		this.filesystemService.createNewFilesystem(filesystemId, userId, filesystemProperties);
		this.directoryService.createNewDirectory(commitId, directoryPath, directoryName, filesystemId, userId, directoryProperties);
		this.directoryService.createNewDirectory(commitId, directoryPath2, directoryName2, filesystemId, userId, directoryProperties2);
		this.fileService.createNewFile(commitId, filePath, fileName, filesystemId, userId, fileProperties);
		
		Map<String, Object> changeMetadata = new HashMap<String, Object>();
		changeMetadata.put("versionChangeType", VersionChangeType.MODIFY.name());
		changeMetadata.put("dateChanged", new Date().toString());
		
		Map<String, Object> changedProperties = new HashMap<String, Object>();
		changedProperties.put("backupLocations", "[http://googledrive.com/testUser/file1_mod.txt, http://dropbox.com/testUser/file1_mod.txt]");
		changedProperties.put("testProperty", "Test Property");
		
		this.fileService.createNewVersion(commitId, userId, filesystemId, filePath, fileName, changeMetadata, changedProperties);
		
		Map<String, Object> retrievedFileV0Properties = this.fileService.getFile(userId, filesystemId, filePath, fileName, 0);
		Map<String, Object> retrievedFileV1Properties = this.fileService.getFile(userId, filesystemId, filePath, fileName, -1);
		
		assertEquals(fileProperties.size() + 4, retrievedFileV0Properties.size());
		assertEquals(fileProperties.size() + 3, retrievedFileV1Properties.size());
		
		this.fileService.moveFile(commitId, userId, filesystemId, filePath, fileName, "/testFolder2/testFolder", "test_renamed.txt");
		this.directoryService.moveDirectory(commitId, userId, filesystemId, directoryPath2, directoryName2, "/testFolder2", "testFolder1.1");
		//this.fileService.deleteFileTemporarily(userId, filesystemId, filePath, fileName);
		
		this.filesystemService.createNewVersion(userId, filesystemId, changeMetadata, new HashMap<String, Object>());
		this.filesystemService.deleteFilesystemTemporarily(userId, filesystemId);
		this.filesystemService.restoreTemporaryDeletedFilesystem(userId, filesystemId);
		
		String userId2 = "testUser2";
		this.userService.createNewUser(userId2, userProperties);
		this.fileService.shareFile(commitId, userId, filesystemId, "/testFolder2/testFolder1.1", "test_renamed.txt", userId2, FilePermission.READ);
		this.fileService.unshareFile(commitId, userId, filesystemId, "/testFolder2/testFolder1.1", "test_renamed.txt", userId2);
	}
}