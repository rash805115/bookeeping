package database.service;

import java.util.List;
import java.util.Map;

import exception.DuplicateUser;
import exception.UserNotFound;

public interface UserService
{
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser;
	public long countUsers();
	public void removeUser(String userId) throws UserNotFound;
	
	public Map<String, Object> getUser(String userId) throws UserNotFound;
	public List<Map<String, Object>> getUsersByMatchingAllProperty(Map<String, Object> userProperties) throws UserNotFound;
	public List<Map<String, Object>> getUsersByMatchingAnyProperty(Map<String, Object> userProperties) throws UserNotFound;
}
