package com.cg.syscab.integration;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.easybatch.core.api.Engine;
import org.easybatch.core.api.Record;
import org.easybatch.core.api.RecordFilter;
import org.easybatch.core.api.RecordMapper;
import org.easybatch.core.api.RecordMappingException;
import org.easybatch.core.api.RecordProcessingException;
import org.easybatch.core.api.RecordProcessor;
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
import com.cg.syscab.business.integration.BesDeptData;
import com.cg.syscab.business.org.Company;
import com.cg.syscab.business.org.Department;
import com.cg.syscab.dao.org.CompanyDAOImp;
import com.cg.syscab.dao.org.DepartmentDAOImp;
import com.cg.syscab.utils.ConfigUtil;
import com.cg.syscab.utils.HibernateUtil;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

/**
 *
 * @author Meng-Ting Lin
 */
public class DepartmentSyncService implements SyncService , Job{

    private Logger log = LoggerFactory.getLogger(DepartmentSyncService.class);

	@Override
	public void doSync() {
		log.debug("[DEBUG] start to department synchronized.");
		final Session dbSession = HibernateUtil.getSession();
		HibernateUtil.beginTransaction();
		final DepartmentDAOImp deptDao = new DepartmentDAOImp();
		final String companyId = ConfigUtil.getSystemConfig().syncDepartmentForCompanyId();
		final Company company = new CompanyDAOImp().getCompanyByCompanyId(companyId);
		final SyncServiceResult result = new SyncServiceResult();
		List<BesDeptData> besDepts = dbSession.createCriteria(BesDeptData.class).list();
		result.setTotal(besDepts.size());
		List<Department> depts = QueryService.createQuery(Department.class).query(new QueryCondition() {

			@Override
			public Criteria queryCondition(Criteria critiria) {
				return critiria.createAlias("company", "c").add(Restrictions.eq("c.companyId", companyId));
			}
		}).list();

		final Map<String, BesDeptData> besDeptsMap = FluentIterable.from(
				besDepts).uniqueIndex(
				new Function<BesDeptData, String>() {
					@Override
					public String apply(BesDeptData dep) {
						return dep.getId().trim();
					}
				});
		final Map<String, Department> deptsMap = FluentIterable.from(
				depts).uniqueIndex(
				new Function<Department, String>() {
					@Override
					public String apply(Department dep) {
						return dep.getDepartmentId().trim();
					}
				});

		Engine cuEngine = EngineBuilder.aNewEngine().reader(new IterableRecordReader<BesDeptData>(besDepts))
				.filter(new RecordFilter() {
					@Override
					public boolean filterRecord(Record record) {
						if (!(record.getPayload() instanceof BesDeptData)){
							return true;
						}
						BesDeptData besDept = (BesDeptData)record.getPayload();
						Department deptInPOERP = deptsMap.get(besDept.getId().trim());
						if (log.isTraceEnabled()){
						    log.trace(String.format("[FILTER] check POERP's department exist: BES's departmentid(id=%s) in POERP's department(%s)" , besDept.getId() , deptInPOERP));
						}
						boolean shouldFiltered =
								(deptInPOERP != null)
								&& (StringUtils.equals(besDept.getName(), deptInPOERP.getName()))  //change name
								&& (StringUtils.equalsIgnoreCase(Department.Status.ENABLE.getCode(), deptInPOERP.getStatus()));  //change status
						return shouldFiltered;
					}
				})
				.mapper(new RecordMapper<Department>() {
					@Override
					public Department mapRecord(Record record)
							throws RecordMappingException {
						BesDeptData besDept = (BesDeptData)record.getPayload();
						Department deptForPOERP = Optional.fromNullable(deptsMap.get(besDept.getId().trim())).or(new Department());
						if (log.isDebugEnabled()){
						    log.debug(String.format("[MAPPER] get POERP's department by BES's departmentid(id=%s) in POERP's department(%s)" , besDept.getId() , deptForPOERP));
						}
						deptForPOERP.setCompany(company);
						deptForPOERP.setDepartmentId(besDept.getId().trim());
						deptForPOERP.setName(besDept.getName());
						deptForPOERP.setStatus(Department.Status.ENABLE.getCode());
						deptForPOERP.setVoidDate(null);
						deptForPOERP.setVoidId(null);
						return deptForPOERP;
					}
				})
				.processor(new RecordProcessor<Department, Department>() {
					@Override
					public Department processRecord(Department dept)
							throws RecordProcessingException {
						if (dept.getId() == null){
							log.debug(String.format("[PROCESSOR] add POERP's department %s", dept));
							dept.setCreateDate(new Date());
							deptDao.insert(dept);
							result.setCreate((result.getCreate() + 1));
						} else {
							log.debug(String.format("[PROCESSOR] update POERP's department %s", dept));
							dept.setModifyDate(new Date());
							deptDao.update(dept);
							result.setUpdate((result.getUpdate() + 1));
						}
						return dept;
					}
				})
				.build();
		Engine delEngine = EngineBuilder.aNewEngine().reader(new IterableRecordReader<Department>(depts))
				.filter(new RecordFilter() {
					@Override
					public boolean filterRecord(Record record) {
						if (!(record.getPayload() instanceof Department)){
							return true;
						}
						Department dept = (Department)record.getPayload();
						if(StringUtils.equals(Department.Status.DISABLE.getCode(), dept.getStatus())){
							return true;
						}
						BesDeptData deptFromMap = besDeptsMap.get(dept.getDepartmentId().trim());
						return (deptFromMap != null);
					}
				})
				.mapper(new RecordMapper<Department>() {
					@Override
					public Department mapRecord(Record record)
							throws RecordMappingException {
						return (Department)record.getPayload();
					}
				})
				.processor(new RecordProcessor<Department, Department>() {
					@Override
					public Department processRecord(Department dept)
							throws RecordProcessingException {
						dept.setStatus(Department.Status.DISABLE.getCode());
						dept.setModifyDate(new Date());
						dept.setVoidDate(new Date());
						log.debug(String.format("[PROCESSOR] delete POERP's department %s", dept));
						deptDao.update(dept);
						result.setDelete((result.getDelete() + 1));
						return dept;
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
		log.info(String.format("Department sync result %s", result.toLogString()));
		log.debug("[DEBUG] end to department synchronized.");
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String jobName = context.getJobDetail().getDescription();
		log.info(String.format("[JOB] %s prepare to start", jobName));
		this.doSync();
		log.info(String.format("[JOB] %s is ended.", jobName));
	}
}
