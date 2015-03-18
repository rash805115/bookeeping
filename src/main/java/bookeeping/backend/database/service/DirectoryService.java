package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateDirectory;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public interface DirectoryService
{
	public void createNewDirectory(String commitId, String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory;
	public void createNewVersion(String commitId, String userId, String filesystemId, String directoryPath, String directoryName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound;
	public void deleteDirectoryTemporarily(String commitId, String userId, String filesystemId, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound;
	public void restoreTemporaryDeletedDirectory(String commitId, String userId, String filesystemId, String directoryPath, String directoryName, String previousCommitId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateDirectory;
	public void moveDirectory(String commitId, String userId, String filesystemId, String oldDirectoryPath, String oldDirectoryName, String newDirectoryPath, String newDirectoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateDirectory;
	public Map<String, Object> getDirectory(String userId, String filesystemId, String directoryPath, String directoryName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound;
}
