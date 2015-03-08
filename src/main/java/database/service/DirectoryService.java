package database.service;

import java.util.Map;

import exception.DirectoryNotFound;
import exception.DuplicateDirectory;
import exception.FilesystemNotFound;
import exception.UserNotFound;

public interface DirectoryService
{
	public void createNewDirectory(String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory;
	
	public Map<String, Object> getDirectory(String userId, String filesystemId, String directoryId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound;
}
