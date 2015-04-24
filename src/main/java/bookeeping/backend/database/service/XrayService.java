package bookeeping.backend.database.service;

import java.util.List;
import java.util.Map;

import bookeeping.backend.exception.NodeNotFound;

public interface XrayService
{
	public List<Map<String, Object>> xrayNode(String nodeId) throws NodeNotFound;
	public List<Map<String, Object>> xrayVersion(String nodeId) throws NodeNotFound;
	public List<Map<String, Object>> xrayDeleted(String nodeId) throws NodeNotFound;
}
