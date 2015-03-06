package database.service;

import java.util.Map;

import exception.DuplicateFilesystem;
import exception.UserNotFound;

public interface FilesystemService
{
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem;
}
