package database.service;

import java.util.Map;

import exception.DirectoryNotFound;
import exception.DuplicateFile;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public interface FileService
{
	public void createNewFile(String filePath, String fileName, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile;
	public void createNewVersion(String userId, String filesystemId, String filePath, String fileName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
	public void deleteFileTemporarily(String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
	public Map<String, Object> getFile(String userId, String filesystemId, String filePath, String fileName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound;
}
