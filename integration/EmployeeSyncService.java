package com.cg.syscab.integration;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.easybatch.core.api.Engine;
import org.easybatch.core.api.Record;
import org.easybatch.core.api.RecordFilter;
import org.easybatch.core.api.RecordMapper;
import org.easybatch.core.api.RecordMappingException;
import org.easybatch.core.api.RecordProcessingException;
import org.easybatch.core.api.RecordProcessor;
import org.easybatch.core.api.RecordValidator;
import org.easybatch.core.api.ValidationError;
import org.easybatch.core.impl.EngineBuilder;
import org.easybatch.core.reader.IterableRecordReader;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cg.syscab.business.QueryService;
import com.cg.syscab.business.QueryService.QueryCondition;
import com.cg.syscab.business.integration.BesEmp;
import com.cg.syscab.business.org.Company;
import com.cg.syscab.business.org.Department;
import com.cg.syscab.business.org.Employee;
import com.cg.syscab.business.parameter.ParameterService;
import com.cg.syscab.business.parameter.Title;
import com.cg.syscab.business.system.User;
import com.cg.syscab.dao.org.CompanyDAOImp;
import com.cg.syscab.dao.org.EmployeeDAOImp;
import com.cg.syscab.dao.system.UserDAO;
import com.cg.syscab.utils.ConfigUtil;
import com.cg.syscab.utils.HibernateUtil;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class EmployeeSyncService implements SyncService , Job{
	/**
	 * index of job title id in votype.
	 */
	private Logger log = LoggerFactory.getLogger(EmployeeSyncService.class);

	@SuppressWarnings("unchecked")
	@Override
	public void doSync() {
		log.debug("[DEBUG] start to employee synchronized.");
		final Session dbSession = HibernateUtil.getSession();
		HibernateUtil.beginTransaction();
		final EmployeeDAOImp empDao = new EmployeeDAOImp();
		final UserDAO userDao = new UserDAO();
		final ParameterService paramService = new ParameterService();
		final SyncServiceResult result = new SyncServiceResult();
		final String companyId = ConfigUtil.getSystemConfig().syncDepartmentForCompanyId();
		final Company company = new CompanyDAOImp().getCompanyByCompanyId(companyId);
		if (company == null){
			return;
		}

		List<Department> departments = QueryService.createQuery(Department.class).query(new QueryCondition() {

			@Override
			public Criteria queryCondition(Criteria critiria) {
				return critiria.createAlias("company", "c").add(Restrictions.eq("c.companyId", companyId));
			}
		}).list();

		final Map<String, Department> deptsMap = FluentIterable.from(departments)
				.uniqueIndex(
				new Function<Department, String>() {
					@Override
					public String apply(Department dep) {
						return dep.getDepartmentId().trim();
					}
				});

		final Map<String , Title> titlesMap = Maps.newHashMap(FluentIterable
				.from(paramService.getActiveTitles())
				.uniqueIndex(new Function<Title, String>() {
					@Override
					public String apply(Title title) {
						return title.getName().trim();
					}
				}));
		if (log.isTraceEnabled()){
			log.trace(String.format("[DEBUG] job title map %s", titlesMap));
		}
		List<BesEmp> empsFromBES = dbSession.createCriteria(BesEmp.class).list();
		result.setTotal(empsFromBES.size());
		List<Employee> emps = Lists.newArrayList();
		for(Department department : departments){
			emps.addAll(department.getEmployees());
		}
	    final Map<String , BesEmp> besEmpsMap = FluentIterable.from(empsFromBES)
	    		.uniqueIndex(new Function<BesEmp, String>() {
			@Override
			public String apply(BesEmp employee) {
				return employee.getId().trim();
			}
		});
	    final Map<String , Employee> empsMap = FluentIterable.from(emps).uniqueIndex(new Function<Employee, String>() {
			@Override
			public String apply(Employee employee) {
				return employee.getEmployeeId().trim();
			}
		});

	    Engine cuEngine = EngineBuilder.aNewEngine().reader(new IterableRecordReader<BesEmp>(empsFromBES))
	    .filter(new RecordFilter() {
			@Override
			public boolean filterRecord(Record record) {
				if (!(record.getPayload() instanceof BesEmp)){
					return true;
				}
				return shouldFiltered((BesEmp)record.getPayload());
			}
			private boolean shouldFiltered(BesEmp employeeFromBES){
	    		Employee employee = empsMap.get(employeeFromBES.getId().trim());
	    		if (employee == null){
	    			if (log.isDebugEnabled()){
	    				log.debug(String.format("[DEBUG] employee %s should filter false", employeeFromBES.getId()));
	    			}
	    			return false;
	    		}
	    		String departmentId = employee.getDepartment() != null ?
	    				employee.getDepartment().getDepartmentId() : null;
	    		String title = employee.getTitle() != null ? employee.getTitle().getName() :
	    				null;
	    		boolean shouldModified = (!StringUtils.equals(employee.getName(), employeeFromBES.getName()))
	    	    		|| (!StringUtils.equals(employee.getEmail(), employeeFromBES.getEmail()))
	    	    		|| (!StringUtils.equals(title, employeeFromBES.getTitle()))
	    	    		|| (!StringUtils.equals(departmentId, employeeFromBES.getDept())
	    	    		);
	    		if (log.isTraceEnabled()){
	    			log.trace(String.format("[FILTER] employee %s should be filtered %s", employeeFromBES.getId() , !shouldModified));
	    		}
	    		return !shouldModified;
	    	}
		}).mapper(new RecordMapper<EmployeeSyncProcessBean>() {
			@Override
			public EmployeeSyncProcessBean mapRecord(Record record)
					throws RecordMappingException {
				BesEmp empFromBES = (BesEmp)record.getPayload();
				if (log.isDebugEnabled()){
					log.debug(String.format("[MAPPER] prepare map record [departmentId(%s)] [email(%s)] [name(%s)] [title(%s)] [id(%s)]"
							, empFromBES.getDept() , empFromBES.getEmail() , empFromBES.getName()
							, empFromBES.getTitle() , empFromBES.getId()));
				}
				Employee employee = Optional.fromNullable(empsMap.get(empFromBES.getId())).or(new Employee());
				EmployeeSyncProcessBean employeeBean = new EmployeeSyncProcessBean(employee);
				employeeBean.putEvent(EmployeeSyncProcessBean.Event.DepartmentChanged
						, isDepartmentChanged(empFromBES, employee));
				employee.setDepartment(deptsMap.get(empFromBES.getDept().trim()));
				employee.setEmail(empFromBES.getEmail());
				employee.setName(empFromBES.getName());
				employee.setTitle(getTitleAndAddTitleIfNotExist(empFromBES));
				employee.setEmployeeId(empFromBES.getId());
				employee.setStatus(Employee.Status.ENABLE.getCode());
				return employeeBean;
			}

			/**
			 * // BES department    || POERP  department   || isDepartmentChanged
			   //       null           null                   false
			   //       null           any                    true
			   //       97B            null                   true
			   //       97B            96B                    true
			   //       97B            97B                    false
			 * @param empFromBES
			 * @param employee
			 * @return
			 */
			private boolean isDepartmentChanged(BesEmp empFromBES,
					Employee employee) {
				String besDeptID = empFromBES.getDept().trim();
				String poerpDeptId = employee.getDepartment() != null ?
						employee.getDepartment().getDepartmentId() : null;
			    return !StringUtils.equalsIgnoreCase(besDeptID, poerpDeptId);
			}

			private Title getTitleAndAddTitleIfNotExist(BesEmp empFromBES) {
				Title title = null;
				if (null != empFromBES.getTitle()){
					title = titlesMap.get(empFromBES.getTitle().trim());
				}
				// save job title parameter.
				if ((null == title) && ( null != empFromBES.getTitle())){
					try{
						log.trace(String.format("[MAPPER] prepare to add job title %s", empFromBES.getTitle()));
						Title newTitle = new Title();
						newTitle.setName(empFromBES.getTitle().trim());						
				        paramService.saveParameter(newTitle);
				        titlesMap.put(newTitle.getName(), newTitle);
				        title = newTitle;
					} catch (Exception e){
						log.error(String.format("[MAPPER] create job title fail %s [error: %s]"
								, empFromBES.getTitle() , e.getMessage()) , e );
					}
				}
				return title;
			}
		}).validator(new RecordValidator<EmployeeSyncProcessBean>() {
			@Override
			public Set<ValidationError> validateRecord(EmployeeSyncProcessBean employee) {
				Set<ValidationError> errors = Sets.newHashSet();
				if (employee.getEmployee().getDepartment() == null){
					errors.add(new ValidationError(String.format("sync.employee.department.notFound empId(%s)", employee.getEmployee().getEmployeeId())));
				}
				return errors;
			}
		}).processor(new RecordProcessor<EmployeeSyncProcessBean , Employee>() {
			@Override
			public Employee processRecord(EmployeeSyncProcessBean employee)
					throws RecordProcessingException {
				if (employee.getEmployee().getId() == null){
					log.debug(String.format("[DEBUG] add employee(%s)", employee.getEmployee().getEmployeeId()));
					employee.getEmployee().setCreateDate(new Date());
					empDao.insert(employee.getEmployee());
					result.setCreate((result.getCreate() + 1));
				} else {
					User user = employee.getEmployee().getUser();
					if (user != null){
						boolean isDepartmentChanged = employee.shouldProcessEvent
					    		(EmployeeSyncProcessBean.Event.DepartmentChanged);
						log.debug(String.format("[DEBUG] employee(%s) is department changed %s", employee.getEmployee().getEmployeeId() , isDepartmentChanged));
						if (isDepartmentChanged){
							user.setStatus(User.Status.DISABLE.getCode());
							user.setModified(new Date());
							userDao.update(user);
						}
					}
					employee.getEmployee().setModifyDate(new Date());
					empDao.update(employee.getEmployee());
					log.debug(String.format("[DEBUG] update employee(%s)", employee.getEmployee().getEmployeeId()));
					result.setUpdate((result.getUpdate() + 1));
				}
				return employee.getEmployee();
			}
		}).build();

	    Engine delEngine = EngineBuilder.aNewEngine().reader(new IterableRecordReader<Employee>(emps))
	    .filter(new RecordFilter() {
			@Override
			public boolean filterRecord(Record record) {
				if (!(record.getPayload() instanceof Employee)){
					return true;
				}
				Employee employee = (Employee)record.getPayload();
				if(StringUtils.equals(Employee.Status.DISABLE.getCode(), employee.getStatus())){
					return true;
				}
				BesEmp empFromBES = besEmpsMap.get(employee.getEmployeeId().trim());
				return !(empFromBES == null);
			}
		})
		.mapper(new RecordMapper<Employee>() {
			@Override
			public Employee mapRecord(Record record)
					throws RecordMappingException {
				return (Employee)record.getPayload();
			}
		})
		.processor(new RecordProcessor<Employee, Employee>() {
			@Override
			public Employee processRecord(Employee employee)
					throws RecordProcessingException {
				Employee employeeFromMap = empsMap.get(employee.getEmployeeId());
				employeeFromMap.setStatus(Employee.Status.DISABLE.getCode());
				User account = employeeFromMap.getUser();
				if (account != null){
					account.setStatus(User.Status.DISABLE.getCode());
					account.setModified(new Date());
					userDao.update(account);
				}
				employeeFromMap.setModifyDate(new Date());
				employeeFromMap.setVoidDate(new Date());
				empDao.update(employeeFromMap);
				log.debug(String.format("[DEBUG] delete employee(%s)", employeeFromMap.getEmployeeId()));
				result.setDelete((result.getDelete() + 1));
				return employee;
			}
		})
	    .build();

	    try {
			cuEngine.call();
			delEngine.call();
			HibernateUtil.commitTransaction();
		} catch (Exception e) {
			log.error("[ERROR] job fail ", e);
		} finally{
			HibernateUtil.closeSession();
		}
	    log.info(String.format("Employee sync result %s", result.toLogString()));
		log.debug("[DEBUG] end to employee synchronized.");
	}

	public static class EmployeeSyncProcessBean{
		public enum Event{
			DepartmentChanged;
		}

		private Employee employee;

		private Map<String , Boolean> events = Maps.newHashMap();

		public EmployeeSyncProcessBean(Employee employee) {
			super();
			this.employee = employee;
		}

		public Employee getEmployee() {
			return employee;
		}

		public void setEmployee(Employee employee) {
			this.employee = employee;
		}

		public void putEvent(Event event , boolean willFire) {
			this.events.put(event.toString(), willFire);
		}

		public boolean shouldProcessEvent(Event event){
			Boolean shouldProcess = this.events.get(event.toString());
			return shouldProcess == null ? false : shouldProcess.booleanValue();
		}
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String jobName = context.getJobDetail().getDescription();
		log.info(String.format("[JOB] %s prepare to start", jobName));
		this.doSync();
		log.info(String.format("[JOB] %s is ended.", jobName));
	}
}
