package bookeeping.backend.database.service.neo4jrest.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.service.DirectoryService;
import bookeeping.backend.database.service.FileService;
import bookeeping.backend.database.service.FilesystemService;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.database.service.UserService;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateDirectory;
import bookeeping.backend.exception.DuplicateFile;
import bookeeping.backend.exception.DuplicateFilesystem;
import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public class BasicFunctionalityTest
{
	private UserService userService;
	private FilesystemService filesystemService;
	private DirectoryService directoryService;
	private FileService fileService;
	private GenericService genericService;
	
	private String user1;
	private Map<String, Object> user1Properties;
	private String user2;
	private Map<String, Object> user2Properties;
	
	private String filesystem1;
	private Map<String, Object> filesystem1Properties;
	private String filesystem2;
	private Map<String, Object> filesystem2Properties;
	
	private String directory1Path;
	private String directory1Name;
	private String directory2Path;
	private String directory2Name;
	private String directory3Path;
	private String directory3Name;
	private String directory4Path;
	private String directory4Name;
	
	private String file1Path;
	private String file1Name;
	private String file2Path;
	private String file2Name;
	private String file3Path;
	private String file3Name;
	private String file4Path;
	private String file4Name;
	
	@Before
	public void setup()
	{
		this.userService = new UserServiceImpl();
		this.filesystemService = new FilesystemServiceImpl();
		this.directoryService = new DirectoryServiceImpl();
		this.fileService = new FileServiceImpl();
		this.genericService = new GenericServiceImpl();
		
		this.user1 = "rash";
		this.user1Properties = new HashMap<String, Object>();
		user1Properties.put("firstName", "Rahul");
		user1Properties.put("lastName", "Chaudhary");
		
		this.user2 = "honey";
		this.user2Properties = new HashMap<String, Object>();
		this.user2Properties.put("firstName", "Honey");
		this.user2Properties.put("lastName", "Anant");
		
		this.filesystem1 = "rash-filesystem-1";
		this.filesystem1Properties = new HashMap<String, Object>();
		this.filesystem1Properties.put("sourceLocation", "/users/Rash/Documents/bookeeping");
		this.filesystem1Properties.put("dateCreated", new Date().toString());
		
		this.filesystem2 = "honey-filesystem-1";
		this.filesystem2Properties = new HashMap<String, Object>();
		this.filesystem2Properties.put("sourceLocation", "C:\\Users\\Honey\\My Documents\\bookeeping");
		this.filesystem2Properties.put("dateCreated", new Date().toString());
		
		this.directory1Path = "/Music";
		this.directory1Name = "Inspirational";
		this.directory2Path = "/Books/Programming";
		this.directory2Name = "Java";
		this.directory3Path = "/Books/Programming/Web";
		this.directory3Name = "HTML5";
		this.directory4Path = "/Books/Language";
		this.directory4Name = "English";
		
		this.file1Path = "/Music/Inspirational";
		this.file1Name = "bourne-theme.mp3";
		this.file2Path = "/";
		this.file2Name = "to-do.txt";
		this.file3Path = "/Books/Language/English";
		this.file3Name = "Barron's GRE.pdf";
		this.file4Path = "/Books/Programming/Web/HTML5";
		this.file4Name = "Learn Web Development In 2 Minutes.pdf";
	}
	
	@After
	public void tearDown()
	{
		this.userService = null;
		this.filesystemService = null;
		this.directoryService = null;
		this.fileService = null;
	}
	
	@Test
	public void testBasicFunctionalities() throws DuplicateUser, UserNotFound, DuplicateFilesystem, FilesystemNotFound, NodeNotFound, VersionNotFound, NodeUnavailable, DuplicateDirectory, DirectoryNotFound, DuplicateFile, FileNotFound
	{
		Map<String, Object> changeMetadata = new HashMap<String, Object>();
		changeMetadata.put("changeType", "modify");
		Map<String, Object> changedProperties = new HashMap<String, Object>();
		changedProperties.put("dateCreated", new Date().toString());
		changedProperties.put("testProperty", "Test Property");
		
		/*
		 * User Check
		 */
		this.userService.createNewUser(this.user1, this.user1Properties);
		this.userService.createNewUser(this.user2, this.user2Properties);
		
		Map<String, Object> retrievedUser1Properties = this.userService.getUser(this.user1);
		String retrievedUser1Name = (String) retrievedUser1Properties.get("firstName") + " " + (String) retrievedUser1Properties.get("lastName");
		String expectedUser1Name = (String) this.user1Properties.get("firstName") + " " + (String) this.user1Properties.get("lastName");
		
		Map<String, Object> retrievedUser2Properties = this.userService.getUser(this.user2);
		String retrievedUser2Name = (String) retrievedUser2Properties.get("firstName") + " " + (String) retrievedUser2Properties.get("lastName");
		String expectedUser2Name = (String) this.user2Properties.get("firstName") + " " + (String) this.user2Properties.get("lastName");
		
		assertEquals(retrievedUser1Name, expectedUser1Name);
		assertEquals(retrievedUser2Name, expectedUser2Name);
		
		/*
		 * Filesystem Check
		 */
		String filesystem1NodeId = this.filesystemService.createNewFilesystem("First Commit", this.user1, this.filesystem1, this.filesystem1Properties);
		String filesystem2NodeId = this.filesystemService.createNewFilesystem("firstCommit", this.user2, this.filesystem2, this.filesystem2Properties);
		
		Map<String, Object> retrievedFilesystem1Properties = this.filesystemService.getFilesystem(this.user1, this.filesystem1);
		String retrievedFilesystem1SourceLocation = (String) retrievedFilesystem1Properties.get("sourceLocation");
		String expectedFilesystem1SourceLocation = (String) this.filesystem1Properties.get("sourceLocation");
		
		Map<String, Object> retrievedFilesystem2Properties = this.filesystemService.getFilesystem(this.user2, this.filesystem2);
		String retrievedFilesystem2SourceLocation = (String) retrievedFilesystem2Properties.get("sourceLocation");
		String expectedFilesystem2SourceLocation = (String) this.filesystem2Properties.get("sourceLocation");
		
		assertEquals(retrievedFilesystem1SourceLocation, expectedFilesystem1SourceLocation);
		assertEquals(retrievedFilesystem2SourceLocation, expectedFilesystem2SourceLocation);
		
		/*
		 * Filesystem Version Check
		 */
		this.genericService.createNewVersion("Second Commit", filesystem1NodeId, changeMetadata, changedProperties);
		this.genericService.createNewVersion("secondCommit", filesystem2NodeId, changeMetadata, changedProperties);
		
		Map<String, Object> retrievedFilesystem1Version1Properties = this.genericService.getNodeVersion(filesystem1NodeId, 1);
		String retrievedFilesystem1Version1TestProperty = (String) retrievedFilesystem1Version1Properties.get("testProperty");
		String expectedFilesystem1Version1TestProperty = (String) changedProperties.get("testProperty");
		
		Map<String, Object> retrievedFilesystem2Version1Properties = this.genericService.getNodeVersion(filesystem2NodeId, 1);
		String retrievedFilesystem2Version1TestProperty = (String) retrievedFilesystem2Version1Properties.get("testProperty");
		String expectedFilesystem2Version1TestProperty = (String) changedProperties.get("testProperty");
		
		assertEquals(retrievedFilesystem1Version1TestProperty, expectedFilesystem1Version1TestProperty);
		assertEquals(retrievedFilesystem2Version1TestProperty, expectedFilesystem2Version1TestProperty);
		
		/*
		 * Filesystem Delete/Restore Check
		 */
		this.genericService.deleteNodeTemporarily("Third Commit", filesystem1NodeId);
		try
		{
			this.filesystemService.getFilesystem(user1, filesystem1);
			assertTrue("Was expecting FilesystemNotFound error because Filesystem is deleted.", false);
		}
		catch(FilesystemNotFound filesystemNotFound)
		{
			assertTrue(true);
		}
		
		this.filesystemService.restoreFilesystem("Fourth Commit", user1, filesystem1, filesystem1NodeId);
		try
		{
			this.filesystemService.getFilesystem(user1, filesystem1);
			assertTrue(true);
		}
		catch(FilesystemNotFound filesystemNotFound)
		{
			assertTrue("Was not expecting FilesystemNotFound error because Filesystem is restored.", false);
		}
		
		/*
		 * Directory Check
		 */
		String directory1NodeId = this.directoryService.createNewDirectory("Fifth Commit", this.user1, this.filesystem1, 0, this.directory1Path, this.directory1Name, new HashMap<String, Object>());
		String directory2NodeId = this.directoryService.createNewDirectory("Fifth Commit", this.user1, this.filesystem1, 0, this.directory2Path, this.directory2Name, new HashMap<String, Object>());
		this.directoryService.createNewDirectory("Fifth Commit", this.user1, this.filesystem1, 0, this.directory3Path, this.directory3Name, new HashMap<String, Object>());
		String directory4NodeId = this.directoryService.createNewDirectory("Fifth Commit", this.user1, this.filesystem1, 0, this.directory4Path, this.directory4Name, new HashMap<String, Object>());
		
		Map<String, Object> retrievedDirectory3Properties = this.directoryService.getDirectory(this.user1, this.filesystem1, 0, this.directory3Path, this.directory3Name);
		String retrievedDirectory3Name = (String) retrievedDirectory3Properties.get(MandatoryProperties.directoryName.name());
		Map<String, Object> retrievedDirectory4Properties = this.genericService.getNode(directory4NodeId);
		String retrievedDirectory4Name = (String) retrievedDirectory4Properties.get(MandatoryProperties.directoryName.name());
		
		assertEquals(retrievedDirectory3Name, this.directory3Name);
		assertEquals(retrievedDirectory4Name, this.directory4Name);
		
		/*
		 * Directory Version Check
		 */
		String directory1Version1NodeId = this.genericService.createNewVersion("Sixth Commit", directory1NodeId, changeMetadata, changedProperties);
		Map<String, Object> retrievedDirectory1Version1Properties = this.genericService.getNodeVersion(directory1Version1NodeId, 1);
		String retrievedDirectory1Version1NodeId = (String) retrievedDirectory1Version1Properties.get(MandatoryProperties.nodeId.name());
		
		assertEquals(directory1Version1NodeId, retrievedDirectory1Version1NodeId);
		
		/*
		 * Directory Delete/Restore Check
		 */
		this.genericService.deleteNodeTemporarily("Seventh Commit", directory2NodeId);
		try
		{
			this.directoryService.getDirectory(this.user1, this.filesystem1, 0, this.directory2Path, this.directory2Name);
			assertTrue("Was expecting DirectoryNotFound error because Directory is deleted.", false);
		}
		catch(DirectoryNotFound directoryNotFound)
		{
			assertTrue(true);
		}
		
		this.directoryService.restoreDirectory("Eighth Commit", this.user1, this.filesystem1, 0, this.directory2Path, this.directory2Name, directory2NodeId);
		try
		{
			this.directoryService.getDirectory(this.user1, this.filesystem1, 0, this.directory2Path, this.directory2Name);
			assertTrue(true);
		}
		catch(DirectoryNotFound directoryNotFound)
		{
			assertTrue("Was not expecting DirectoryNotFound error because Directory is restored.", false);
		}
		
		/*
		 * File Check
		 */
		this.fileService.createNewFile("Ninth Commit", this.user1, this.filesystem1, 0, this.file1Path, this.file1Name, new HashMap<String, Object>());
		String file2NodeId = this.fileService.createNewFile("Ninth Commit", this.user1, this.filesystem1, 0, this.file2Path, this.file2Name, new HashMap<String, Object>());
		String file3NodeId = this.fileService.createNewFile("Ninth Commit", this.user1, this.filesystem1, 0, this.file3Path, this.file3Name, new HashMap<String, Object>());
		this.fileService.createNewFile("Ninth Commit", this.user1, this.filesystem1, 0, this.file4Path, this.file4Name, new HashMap<String, Object>());
		
		Map<String, Object> retrievedFile1Properties = this.fileService.getFile(this.user1, this.filesystem1, 0, this.file1Path, this.file1Name);
		String retrievedFile1Name = (String) retrievedFile1Properties.get(MandatoryProperties.fileName.name());
		
		assertEquals(this.file1Name, retrievedFile1Name);
		
		/*
		 * File Version Check
		 */
		String file2Version1NodeId = this.genericService.createNewVersion("Tenth Commit", file2NodeId, changeMetadata, changedProperties);
		Map<String, Object> retrievedFile2Version1Properties = this.genericService.getNodeVersion(file2Version1NodeId, 1);
		String retrievedFile2Version1NodeId = (String) retrievedFile2Version1Properties.get(MandatoryProperties.nodeId.name());
		
		assertEquals(file2Version1NodeId, retrievedFile2Version1NodeId);
		
		/*
		 * File Delete/Restore Check
		 */
		this.genericService.deleteNodeTemporarily("Eleventh Commit", file3NodeId);
		try
		{
			this.fileService.getFile(this.user1, this.filesystem1, 0, this.file3Path, this.file3Name);
			assertTrue("Was expecting FileNotFound error because File is deleted.", false);
		}
		catch(FileNotFound fileNotFound)
		{
			assertTrue(true);
		}
		
		this.fileService.restoreFile("Twelth Commit", this.user1, this.filesystem1, 0, this.file3Path, this.file3Name, file3NodeId);
		try
		{
			this.fileService.getFile(this.user1, this.filesystem1, 0, this.file3Path, this.file3Name);
			assertTrue(true);
		}
		catch(FileNotFound fileNotFound)
		{
			assertTrue("Was not expecting FileNotFound error because File is restored.", false);
		}
		
		/*
		 * File Share/Unshare
		 */
		this.fileService.shareFile("Thirteenth Commit", this.user1, this.filesystem1, 0, this.file4Path, this.file4Name, this.user2, "read");
		this.fileService.unshareFile("Fourteenth Commit", user1, filesystem1, 0, file4Path, file4Name, user2);
		assertTrue(true);
	}
}
