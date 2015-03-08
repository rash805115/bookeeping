package database.service;

import java.util.Map;

import exception.DirectoryNotFound;
import exception.DuplicateFile;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;

public interface FileService
{
	public void createNewFile(String fileId, String directoryId, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile;
	
	public Map<String, Object> getFile(String userId, String filesystemId, String directoryId, String fileId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
}
