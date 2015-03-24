package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateDirectory;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public interface DirectoryService
{
	public String createNewDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, VersionNotFound, DuplicateDirectory;
	public void restoreDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound, DuplicateDirectory, NodeNotFound, NodeUnavailable;
	public String moveDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String oldDirectoryPath, String oldDirectoryName, String newDirectoryPath, String newDirectoryName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound, DuplicateDirectory;
	public Map<String, Object> getDirectory(String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound;
}
