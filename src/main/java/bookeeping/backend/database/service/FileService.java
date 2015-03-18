package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateFile;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;
import bookeeping.backend.file.FilePermission;

public interface FileService
{
	public void createNewFile(String commitId, String filePath, String fileName, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile;
	public void createNewVersion(String commitId, String userId, String filesystemId, String filePath, String fileName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
	public void shareFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String shareWithUserId, FilePermission filePermission) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
	public void unshareFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String unshareWithUserId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
	public void deleteFileTemporarily(String commitId, String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound;
	public void restoreTemporaryDeletedFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String previousCommitId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile;
	public void moveFile(String commitId, String userId, String filesystemId, String oldFilePath, String oldFileName, String newFilePath, String newFileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile;
	public Map<String, Object> getFile(String userId, String filesystemId, String filePath, String fileName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound;
}
