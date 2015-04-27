package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.DuplicateFilesystem;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public interface FilesystemService
{
	public String createNewFilesystem(String commitId, String userId, String filesystemId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem;
	public void restoreFilesystem(String commitId, String userId, String filesystemId, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DuplicateFilesystem, NodeNotFound, NodeUnavailable;
	public Map<String, Object> getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound;
	public String getRootDirectory(String userId, String filesystemId, int filesystemVersion) throws UserNotFound, FilesystemNotFound, VersionNotFound;
}
