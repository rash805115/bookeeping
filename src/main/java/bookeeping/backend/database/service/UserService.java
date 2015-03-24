package bookeeping.backend.database.service;

import java.util.Map;

import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.UserNotFound;

public interface UserService
{
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser;
	public Map<String, Object> getUser(String userId) throws UserNotFound;
}
