package database.service;

import java.util.Map;

import exception.DirectoryNotFound;
import exception.DuplicateDirectory;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public interface DirectoryService
{
	public void createNewDirectory(String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory;
	public void createNewVersion(String userId, String filesystemId, String directoryPath, String directoryName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound;
	public void deleteDirectoryTemporarily(String userId, String filesystemId, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound;
	public Map<String, Object> getDirectory(String userId, String filesystemId, String directoryPath, String directoryName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound;
}
