package database.service;

import java.util.Map;

import exception.DuplicateFilesystem;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public interface FilesystemService
{
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem;
	public void createNewVersion(String userId, String filesystemId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound;
	public void deleteFilesystemTemporarily(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound;
	public void restoreTemporaryDeletedFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound;
	public Map<String, Object> getFilesystem(String userId, String filesystemId, int version) throws UserNotFound, FilesystemNotFound, VersionNotFound;
}
