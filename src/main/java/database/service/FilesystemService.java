package database.service;

import java.util.Map;

import exception.DuplicateFilesystem;
import exception.FilesystemNotFound;
import exception.UserNotFound;

public interface FilesystemService
{
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem;
	public void removeFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound;
	
	public Map<String, Object> getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound;
}
