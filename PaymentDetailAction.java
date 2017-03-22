package com.cg.syscab.web.payment;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.struts.Globals;
import org.apache.struts.action.ActionForm;
import org.apache.struts.action.ActionForward;
import org.apache.struts.action.ActionMapping;
import org.apache.struts.actions.LookupDispatchAction;
import org.apache.struts.util.MessageResources;
import org.hibernate.Criteria;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.jxls.area.Area;
import org.jxls.builder.AreaBuilder;
import org.jxls.builder.xls.XlsCommentAreaBuilder;
import org.jxls.common.CellRef;
import org.jxls.common.Context;
import org.jxls.transform.Transformer;
import org.jxls.transform.poi.PoiTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cg.syscab.business.AmountComparator;
import com.cg.syscab.business.QueryService;
import com.cg.syscab.business.QueryService.QueryCondition;
import com.cg.syscab.business.payment.Payment;
import com.cg.syscab.business.payment.PaymentService;
import com.cg.syscab.business.project.Project;
import com.cg.syscab.business.purchase.PurchaseOrder;
import com.cg.syscab.exceptions.InfrastructureException;
import com.cg.syscab.web.common.Constants;
import com.cg.syscab.web.common.RedirectController;
import com.cg.syscab.web.payment.ui.PaymentBean;
import com.cg.syscab.web.system.SystemConstants;
import com.cg.syscab.web.utils.HttpResponseSupport;
import com.cg.syscab.web.utils.HttpResponseSupport.DoAfterHttpConfig;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class PaymentDetailAction extends LookupDispatchAction {

	private Logger logger = LoggerFactory.getLogger(PaymentDetailAction.class);

	private PaymentService paService = new PaymentService();

	private static Map methodMap = new HashMap();
	static {
		methodMap.put("payment.link.paymentDetailList", "list");
		methodMap.put("button.search", "list");

		methodMap.put("payment.button.export", "export");
		methodMap.put("payment.button.exportAccounting", "exportAccounting");
	}

	protected Map getKeyMethodMap() {
		return methodMap;
	}

	protected ActionForward unspecified(ActionMapping mapping, ActionForm form,
			HttpServletRequest request, HttpServletResponse response)
			throws Exception {
		logger.info("unspecified()...");
		return list(mapping, form, request, response);
	}

	public ActionForward list(ActionMapping mapping, ActionForm form,
			HttpServletRequest request, HttpServletResponse response)
			throws Exception {

		logger.info("list()...");

		RedirectController.getInstance().saveCurrentPath(request, Constants.RETURN_LOCATION_KEY);

		Project project = (Project)request.getSession().getAttribute(SystemConstants.PROJECT);
		final Integer projectId = project.getId();
		//dropdown list contents
		List purchaseOrders = paService.findPurchaseOrders(projectId);
		PurchaseOrder.bubbleSortByPoId(purchaseOrders);

		List suppliers = paService.findSuppliers(projectId);
		request.setAttribute("purchaseOrders", purchaseOrders);
		request.setAttribute("suppliers", suppliers);

		//search conditions
		final PaymentForm paymentForm = (PaymentForm) form;

		List<PaymentBean> payments = QueryService.createQuery(Payment.class).query(new QueryService.QueryCondition() {
			@Override
			public Criteria queryCondition(Criteria critiria) {
				critiria.createAlias("project", "p").createAlias("purchaseOrder", "po").createAlias("supplier", "s");

				critiria.add(
					Restrictions.eq("p.id", projectId)
				);

				if (StringUtils.isNotBlank(paymentForm.getPoId()) && !StringUtils.equals("0", paymentForm.getPoId())) {
					critiria.add(
						Restrictions.eq("po.id", new Integer(paymentForm.getPoId()))
					);
				}

				if (StringUtils.isNotBlank(paymentForm.getPoNote())) {
					critiria.add(
						Restrictions.like("po.note", String.format("%%%s%%", paymentForm.getPoNote()))
					);
				}

				if (StringUtils.isNotBlank(paymentForm.getStatus()) && !StringUtils.equals("0", paymentForm.getStatus())) {
					critiria.add(
						Restrictions.eq("status", paymentForm.getStatus())
					);
				}

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

				if ( StringUtils.isNotBlank(paymentForm.getBeginBatchDate())){
					try {
						Date d = sdf.parse(paymentForm.getBeginBatchDate());
						critiria.add(
							Restrictions.ge("batchDate", DateUtils.truncate(d, Calendar.DATE))
						);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

				if (StringUtils.isNotBlank(paymentForm.getEndBatchDate())){
					try {
						Date d = sdf.parse(paymentForm.getEndBatchDate());
						critiria.add(
							Restrictions.le("batchDate", DateUtils.addMilliseconds(d, 86399000))
						);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

				if (StringUtils.isNotBlank(paymentForm.getSupplierId()) && !StringUtils.equals("0", paymentForm.getSupplierId())) {
					critiria.add(
						Restrictions.eq("s.id", new Integer(paymentForm.getSupplierId()))
					);
				}
				critiria.addOrder(Order.asc("po.id")).addOrder(Order.asc("paymentNo"));
				return critiria;
			}
		}).filter(new Predicate<Payment>() {
			@Override
			public boolean apply(Payment payment) {

				BigDecimal totalPOAmount = payment.getPurchaseOrder().getTotalPriceVerified();

				if (StringUtils.isNotBlank(paymentForm.getTotalPOAmountOperater())
						&& StringUtils.isNotBlank(paymentForm.getTotalPOAmount())){

					BigDecimal totalPOAmountCriteria = new BigDecimal(paymentForm.getTotalPOAmount());
					return AmountComparator
							.from(totalPOAmount, totalPOAmountCriteria)
							.compareBy(paymentForm.getTotalPOAmountOperater());
				}
				return true;
			}
		}).list(new Function<Payment, PaymentBean>() {
			private BigDecimal tempAccumulatedAmount = BigDecimal.ZERO;
			private String tempPOId;
			@Override
			public PaymentBean apply(Payment payment) {
				String poIdString = payment.getPurchaseOrder().getPurchaseOrderId();
				if (!StringUtils.equals(poIdString, tempPOId)){
					tempAccumulatedAmount = BigDecimal.ZERO;
				}
				tempAccumulatedAmount = tempAccumulatedAmount.add(payment.getRequestAmount());
				tempPOId = poIdString;
				PaymentBean bean = new PaymentBean();
				bean.setPayment(payment);
				bean.setAccumulatedAmount(tempAccumulatedAmount);
				bean.setPoAmount(payment.getPurchaseOrder().getTotalPriceVerified());
				if (bean.getPoAmount().compareTo(BigDecimal.ZERO) == 0){
					bean.setProcessRate(BigDecimal.ZERO);
				} else {
					bean.setProcessRate(
							bean.getAccumulatedAmount().divide(bean.getPoAmount(), 10, RoundingMode.CEILING)
							.multiply(new BigDecimal(100)
					));
				}
				return bean;
			}
		});
		request.setAttribute("payments", payments);

		return mapping.findForward("paymentDetailList");
	}


	public ActionForward export(ActionMapping mapping, ActionForm form, HttpServletRequest request, HttpServletResponse response) throws Exception {
		logger.info ("export() start...");

		// --- query payments data. ---
		PaymentForm dynaForm = (PaymentForm) form;
		String[] ids = dynaForm.getIds();
		final Iterable<Integer> paymentIds = FluentIterable.from(Lists.newArrayList(ids)).filter(new Predicate<String>() {
			@Override
			public boolean apply(String id) {
				return NumberUtils.isNumber(id);
			}
		}).transform(new Function<String, Integer>() {
			@Override
			public Integer apply(String id) {
				return new Integer(id);
			}
		});

		List<PaymentBean> payments = QueryService.createQuery(Payment.class).query(new QueryCondition() {
			@Override
			public Criteria queryCondition(Criteria critiria) {
				critiria.add(Restrictions.in("id", Lists.newArrayList(paymentIds)));
				return critiria;
			}
		}).list(new Function<Payment, PaymentBean>() {
			@Override
			public PaymentBean apply(final Payment payment) {
				PaymentBean bean = new PaymentBean();
				bean.setPayment(payment);
				bean.setPoAmount(payment.getPurchaseOrder().getTotalPriceVerified());
				for(Payment item : Sets.<Payment>filter(payment.getPurchaseOrder().getPayments(), new Predicate<Payment>() {
					@Override
					public boolean apply(Payment thePayment) {
						return (thePayment.getPeriod() <= payment.getPeriod());
					}
				})){
					bean.setAccumulatedAmount(bean.getAccumulatedAmount().add(item.getRequestAmount()));
				}
				if (bean.getPoAmount().compareTo(BigDecimal.ZERO) == 0){
					bean.setProcessRate(BigDecimal.ZERO);
				} else {
					bean.setProcessRate(
							bean.getAccumulatedAmount().divide(bean.getPoAmount(), 10, RoundingMode.CEILING)
							.multiply(new BigDecimal(100)
					));
				}
				return bean;
			}
		});
		Map properties = new HashMap();
		properties.put("payments", payments);

		//--- merge xlsx template ---
		String templatePath = request.getSession().getServletContext().getRealPath("/templates/" + "PaymentListTemplate.xlsx");
		InputStream in = new BufferedInputStream(new FileInputStream(templatePath));
		OutputStream out = response.getOutputStream();
		final Transformer transformer = PoiTransformer.createTransformer(in, out);
		AreaBuilder areaBuilder = new XlsCommentAreaBuilder(transformer);
		List<Area> xlsAreaList = areaBuilder.build();
		Area xlsArea = xlsAreaList.get(0);
		Context context = new Context(properties);
		xlsArea.applyAt(new CellRef("Sheet1!A1"), context);

		String contentType = Constants.getMimeType();
		response.setContentType(contentType);

		String xlsxFilename = "估驗計價單列表" ;
		HttpResponseSupport.responseXlsFile(xlsxFilename, request, response, new DoAfterHttpConfig() {
			@Override
			public void doAfter() throws InfrastructureException{
				try {
					transformer.write();
				} catch (IOException e) {
					throw new InfrastructureException(e);
				}
			}
		});
		return null;
	}
	
	public ActionForward exportAccounting(ActionMapping mapping, ActionForm form, HttpServletRequest request, HttpServletResponse response) throws Exception {
		logger.debug("exportAccounting() start...");

		PaymentForm dynaForm = (PaymentForm) form;
		String[] ids = dynaForm.getIds();

		Locale locale = (Locale)request.getSession().getAttribute(Globals.LOCALE_KEY);
		MessageResources mr = this.getResources(request, "paymentBundle");
		final PaymentExcelExport paymentExcelExport = new PaymentExcelExport(mr, locale);

		for(int i = 0,size = ids.length; i < size;i++){
			Payment thisPayment = paService.getPaymentById(new Integer(ids[i]));
			if(thisPayment != null) paymentExcelExport.add(thisPayment);
		}

        String contentType = Constants.getMimeType();
		response.setContentType(contentType);
		final OutputStream out = response.getOutputStream();
		
		String filename = mr.getMessage(locale , "payment.excel.exportFileName");
		HttpResponseSupport.responseXlsFile(filename, request, response, new DoAfterHttpConfig() {
			@Override
			public void doAfter() throws InfrastructureException{
				try {
					ExcelCreator.doExport(out, paymentExcelExport);
				} catch (Exception e) {
					throw new InfrastructureException(e);
				}
			}
		});

		return null;
	}

}
