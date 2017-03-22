package com.cg.syscab.integration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cg.syscab.business.org.Employee;
import com.cg.syscab.business.system.User;
import com.cg.syscab.dao.org.EmployeeDAOImp;
import com.cg.syscab.integration.exceptions.IntegrationException;
import com.cg.syscab.utils.ConfigUtil;
import com.cg.syscab.web.system.EncryptPassword;

public class SSOService {
	private static Logger log = LoggerFactory.getLogger(SSOService.class);
	public static User getSSOUser(String encryptEmailSource , String encryptToken) throws IntegrationException{
		SSOUser ssoUser = null;
		try {
			String emailSource = EncryptPassword.decrypt(encryptEmailSource);
			String token = EncryptPassword.decrypt(encryptToken);
			ssoUser = new SSOUser(token, emailSource);
		} catch (Exception e) {
			log.error(String.format("[ERROR] decrypt fail emailSource %s || token %s", encryptEmailSource , encryptToken));
			throw new IntegrationException("error.sso.decryptFail");
		}
		SSOService.tokenValidate(ssoUser);
		return SSOService.getUserBySSOUser(ssoUser);
	}
	private static void tokenValidate(SSOUser user) throws IntegrationException{
		// check token expired.
		Date tokenDate = null;
		try {
			tokenDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(user.getToken());
		} catch (ParseException e) {
			log.debug(String.format("[ERROR] token invalidation", e.getMessage()) , e);
			throw new IntegrationException("error.sso.tokenInvalidate");
		}
		Calendar expiredDate = Calendar.getInstance();
		expiredDate.add(Calendar.MINUTE,(0 - ConfigUtil.getSystemConfig().ssoTimeoutMinutes()));
		if (tokenDate.before(expiredDate.getTime())){
			log.debug(String.format("[ERROR] token expired %s %s" , tokenDate.getTime() , expiredDate.getTime().getTime()));
			throw new IntegrationException("error.sso.tokenExpired");
		}
	}
	private static User getUserBySSOUser(final SSOUser user) throws IntegrationException{
		Employee employee = new EmployeeDAOImp().getEmployeeByEmployeeId(user.getUserId());
		if (employee == null){
			log.info(String.format("[INFO] SSO User has no employee %s %s", user.getUserId(), user.getEmail()));
			throw new IntegrationException("error.sso.emptyEmployee");
		}
		if (ObjectUtils.equals(Employee.Status.DISABLE.getCode(), employee.getStatus())){
			log.info(String.format("[INFO] SSO User has no validated employee %s %s", user.getUserId(), user.getEmail()));
			throw new IntegrationException("error.sso.employeeIsDisable");
		}
		if (employee.getUser() == null){
			log.info(String.format("[INFO] SSO User has no employee %s %s", user.getUserId() , user.getEmail()));
			throw new IntegrationException("error.sso.userNotActivated");
		}
		if (ObjectUtils.equals(User.Status.DISABLE.getCode(), employee.getUser().getStatus())){
			log.info(String.format("[INFO] User is not enable %s %s", user.getUserId() , user.getEmail()));
			throw new IntegrationException("error.sso.userDisable");
		}
		return employee.getUser();
	}

	public static class SSOUser{
		private String token;
		private String email;
		private String userId;
		public SSOUser(String token, String sourceEmail) {
			super();
			this.token = token;
			String[] sources = sourceEmail.split("@");
			if (sources.length < 2){
				throw new IllegalArgumentException("e.sourceEmail.format.invalidate");
			}
			this.email = sources[1];
			this.userId = sources[0];
		}
		public String getToken() {
			return token;
		}
		public void setToken(String token) {
			this.token = token;
		}
		public String getEmail() {
			return email;
		}
		public void setEmail(String email) {
			this.email = email;
		}
		public String getUserId() {
			return userId;
		}
		public void setUserId(String userId) {
			this.userId = userId;
		}
	}

}
