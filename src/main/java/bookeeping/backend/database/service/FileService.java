package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateFile;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public interface FileService
{
	public String createNewFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound, DuplicateFile;
	public void shareFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String shareWithUserId, String filePermission) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound;
	public void unshareFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String unshareWithUserId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound;
	public void restoreFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound, DuplicateFile, NodeNotFound, NodeUnavailable;
	public String moveFile(String commitId, String userId, String filesystemId, int filesystemVersion, String oldFilePath, String oldFileName, String newFilePath, String newFileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound, DuplicateFile;
	public Map<String, Object> getFile(String userId, String filesystemId, int filesystemVersion, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound;
}
