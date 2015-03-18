package bookeeping.backend.database.service;

import java.util.List;
import java.util.Map;

import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.UserNotFound;

public interface UserService
{
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser;
	public int countUsers();
	
	public Map<String, Object> getUser(String userId) throws UserNotFound;
	public List<Map<String, Object>> getUsersByMatchingAllProperty(Map<String, Object> userProperties);
	public List<Map<String, Object>> getUsersByMatchingAnyProperty(Map<String, Object> userProperties);
}
