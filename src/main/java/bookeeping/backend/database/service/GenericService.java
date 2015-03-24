package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.VersionNotFound;

public interface GenericService
{
	public String createNewVersion(String commitId, String nodeId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws NodeNotFound;
	public Map<String, Object> getNode(String nodeId) throws NodeNotFound;
	public Map<String, Object> getNodeVersion(String nodeId, int version) throws NodeNotFound, VersionNotFound;
	public void deleteNodeTemporarily(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable;
}
