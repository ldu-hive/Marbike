package com.bjsxt.spark.skynet;

import org.apache.spark.AccumulatorParam;

import com.bjsxt.spark.constant.Constants;
import com.bjsxt.spark.util.StringUtils;

public class MonitorAndCameraStateAccumulator implements AccumulatorParam<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}

	/**
	 * 初始化的值
	 */
	@Override
	public String zero(String v1) {
		return Constants.FIELD_NORMAL_MONITOR_COUNT+"=0|"
				+ Constants.FIELD_NORMAL_CAMERA_COUNT+"=0|"
						+ Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=0|"
								+ Constants.FIELD_ABNORMAL_CAMERA_COUNT+"=0|"
										+ Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"= "; 
	}

	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}

	  
	/**
	 * @param v1 连接串,上次累加后的结果
	 * @param v2 本次累加传入的值
	 * @return 更新以后的连接串
	 */
	private String add(String v1, String v2) {
		 if(StringUtils.isEmpty(v1)){
			return v2;
		} 
		String[] valArr = v2.split("\\|");
		for (String string : valArr) {
			String[] fieldAndValArr = string.split("=",2);
			String field = fieldAndValArr[0];
			String value = fieldAndValArr[1];
			String oldVal = StringUtils.getFieldFromConcatString(v1, "\\|", field);
			if(oldVal != null){
				if(Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS.equals(field)){
					v1 = StringUtils.setFieldInConcatString(v1, "\\|", field, oldVal + "~" + value); 
				}else{
					int newVal = Integer.parseInt(oldVal)+Integer.parseInt(value);
					v1 = StringUtils.setFieldInConcatString(v1, "\\|", field, String.valueOf(newVal));  
				}
			}
		}
		return v1;
	}
}
