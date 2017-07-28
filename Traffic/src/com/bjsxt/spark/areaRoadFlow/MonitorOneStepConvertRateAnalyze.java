package com.bjsxt.spark.areaRoadFlow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.bjsxt.spark.constant.Constants;
import com.bjsxt.spark.dao.ITaskDAO;
import com.bjsxt.spark.dao.factory.DAOFactory;
import com.bjsxt.spark.domain.Task;
import com.bjsxt.spark.util.DateUtils;
import com.bjsxt.spark.util.NumberUtils;
import com.bjsxt.spark.util.ParamUtils;
import com.bjsxt.spark.util.SparkUtils;

import scala.Tuple2;


/**=
 * 卡扣流
 * monitor_id   1 2 3 4      1_2 2_3 3_4
 * 
 * 
 * 指定一个道路流  1 2 3 4
 * 1 carCount1 2carCount2  转化率 carCount2/carCount1
 * 1 2 3  转化率                           1 2 3的车流量/1 2的车流量  
 * 1 2 3 4 转化率       1 2 3 4的车流量 / 1 2 3 的车流量
 * 京A1234	1,2,3,6,2,3
 * 1、查询出来的数据封装到cameraRDD
 * 2、计算每一车的轨迹  
 * 3、匹配指定的道路流       1：carCount   1，2：carCount   1,2,3carCount
 * @author root
 */
public class MonitorOneStepConvertRateAnalyze {
	public static void main(String[] args) {
			// 1、构造Spark上下文
			SparkConf conf = new SparkConf()
					.setAppName(Constants.SPARK_APP_NAME_SESSION);
			SparkUtils.setMaster(conf);  
			
			JavaSparkContext sc = new JavaSparkContext(conf);
			SQLContext sqlContext = SparkUtils.getSQLContext(sc);
			
			// 2、生成模拟数据
			SparkUtils.mockData(sc, sqlContext);  
			
			// 3、查询任务，获取任务的参数
			long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT);
			
			ITaskDAO taskDAO = DAOFactory.getTaskDAO();
			Task task = taskDAO.findTaskById(taskid);
			if(task == null) {
				return;
			}
			JSONObject taskParam = JSONObject.parseObject(task.getTaskParams());
			
			/**
			 * 从数据库中查找出来我们指定的卡扣流
			 * 0001,0002,0003,0004,0005
			 */
			String roadFlow = ParamUtils.getParam(taskParam, Constants.PARAM_MONITOR_FLOW);
			final Broadcast<String> roadFlowBroadcast = sc.broadcast(roadFlow);
			
			/**
			 * 通过时间的范围拿到合法的车辆
			 */
			JavaRDD<Row> rowRDDByDateRange = SparkUtils.getCameraRDDByDateRange(sqlContext, taskParam);
			/**
			 * 将rowRDDByDateRange 变成key-value对的形式，key car value 详细信息
			 * 
			 * 为什么要变成k v对的形式？
			 * 因为下面要对car 按照时间排序，绘制出这辆车的轨迹。
			 */
			JavaPairRDD<String, Row> car2RowRDD = getCar2RowRDD(rowRDDByDateRange);
			
			/**
			 * 计算这一辆车，有多少次匹配到咱指定的卡扣流
			 * 
			 * 1,2,3,4,5
			 * 
			 * 1
			 * 1,2
			 * 1,2,3
			 * 1,2,3,4
			 * 1,2,3,4,5
			 * 
			 * 这辆车的轨迹是   1 2 3 6 7 8 1 2
			 * 
			 * 1:2
			 * 1,2 2
			 * 1,2,3 1
			 * 1,2,3,4 0
			 * 1,2,3,4,5 0
			 */
			JavaPairRDD<String, Long> roadSplitRDD = generateAndMatchRowSplit(taskParam,roadFlowBroadcast,car2RowRDD);
			
			/**
			 * roadSplitRDD
			 * [1,100]
			 * [1_2,100]
			 * [1,200] 
			 * 变成了
			 * [1,300]
			 * [1_2,100]
			 */
			Map<String, Long> roadFlow2Count = getRoadFlowCount(roadSplitRDD);
			
			Map<String, Double> convertRateMap = computeRoadSplitConvertRate(roadFlowBroadcast,roadFlow2Count);
			
			Set<Entry<String, Double>> entrySet = convertRateMap.entrySet();
			for (Entry<String, Double> entry : entrySet) {
				System.out.println(entry.getKey()+"="+entry.getValue());
			}
	}

	private static Map<String, Long> getRoadFlowCount(JavaPairRDD<String, Long> roadSplitRDD) {
		
		JavaPairRDD<String, Long> sumByKey = roadSplitRDD.reduceByKey(new Function2<Long, Long, Long>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		 
		/**
		 * 1 200
		 * 1_2 150
		 * 1_2_3 100
		 */
		Map<String, Long> map = new HashMap<>();
		
		List<Tuple2<String, Long>> results = sumByKey.collect();
		
		for (Tuple2<String, Long> tuple : results) {
			map.put(tuple._1, tuple._2);
		}
		return map;
	}

	private static Map<String, Double> computeRoadSplitConvertRate(Broadcast<String> roadFlowBroadcast, Map<String, Long> roadFlow2Count) {
		String roadFlow = roadFlowBroadcast.value();
		String[] split = roadFlow.split(",");
//		List<String> roadFlowList = Arrays.asList(split);
		/**
		 * 存放卡扣切面的转换率
		 * 1_2 0.9
		 */
		Map<String, Double> rateMap = new HashMap<>();
		long lastMonitorCarCount = 0L;
		String tmpRoadFlow = "";
		for (int i = 0; i < split.length; i++) {
			tmpRoadFlow += "," + split[i];   //1(monitot_id)
			Long count = roadFlow2Count.get(tmpRoadFlow.substring(1));
			if(count != null){
				/**
				 * 1_2
				 * lastMonitorCarCount      1 count
				 */
				if(i != 0 && lastMonitorCarCount != 0L){
					double rate = NumberUtils.formatDouble((double)count/(double)lastMonitorCarCount,2);
					rateMap.put(tmpRoadFlow.substring(1), rate);
				}
				lastMonitorCarCount = count;
			}
		}
		return rateMap;
	}

	/**
	 * car2RowRDD car   row详细信息  
	 * 按照通过时间进行排序，拿到他的轨迹  
	 * 1 2 3 4  1_2
	 * 1 2 6 1 2 4   1_2 2
	 * @param taskParam
	 * @param roadFlowBroadcast
	 * @param car2RowRDD
	 * @return
	 */
	private static JavaPairRDD<String, Long> generateAndMatchRowSplit(JSONObject taskParam,final Broadcast<String> roadFlowBroadcast,JavaPairRDD<String, Row> car2RowRDD) {
		return car2RowRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				Iterator<Row> iterator = tuple._2.iterator();
				List<Tuple2<String, Long>> list = new ArrayList<>();
				
				List<Row> rows = new ArrayList<>();
				/**
				 * 遍历的这一辆车的所有的详细信息，然后将详细信息放入到rows集合中
				 */
				while(iterator.hasNext()){
					Row row = iterator.next();
					rows.add(row);
				}
				
				/**
				 * 对这个rows集合 按照车辆通过卡扣的时间排序
				 */
				Collections.sort(rows, new Comparator<Row>() {

					@Override
					public int compare(Row row1, Row row2) {
						 String actionTime1 = row1.getString(4);
						 String actionTime2 = row2.getString(4);
						 if(DateUtils.after(actionTime1, actionTime2)){
							 return 1;
						 }else {
							return -1;
						 }
					}
				});
				
				/**
				 * roadFlowBuilder保存到是？ 卡扣id   组合起来就是这辆车的运行轨迹
				 */
				StringBuilder roadFlowBuilder = new StringBuilder();
				
				/**
				 * roadFlowBuilder怎么拼起来的？  rows是由顺序了，直接便利然后追加到roadFlowBuilder就可以了吧。
				 */
				for (Row row : rows) {
					roadFlowBuilder.append(","+ row.getString(1));
				}
				/**
				 * roadFlowBuilder这里面的开头有一个逗号， 去掉逗号。
				 */
				String roadFlow = roadFlowBuilder.toString().substring(1);
				/**
				 *  从广播变量中获取指定的卡扣流参数
				 */
				String standardRoadFlow = roadFlowBroadcast.value();
				
				/**
				 * 对指定的卡扣流参数分割
				 */
				String[] split = standardRoadFlow.split(",");

				/**
				 * 1,2,3，4,5
				 * 1 2 3 4 5 
				 * 遍历分割完成的数组
				 */
				for (int i = 1; i <= split.length; i++) {
					//临时组成的卡扣切片  1,2 1,2,3 
					String tmpRoadFlow = "";
					for (int j = 0; j < i; j++) {
						tmpRoadFlow += ","+split[j];
					}
					tmpRoadFlow = tmpRoadFlow.substring(1);
					
					//indexOf 从哪个位置开始查找
					int index = 0;
					//这辆车有多少次匹配到这个卡扣切片的次数
					Long count = 0L;
					
					while (roadFlow.indexOf(tmpRoadFlow,index) != -1) {
						index = roadFlow.indexOf(tmpRoadFlow,index) + 1;
						count ++;
					}
					list.add(new Tuple2<String, Long>(tmpRoadFlow, count));
				}
				return list;
			}
		});
	}

	private static JavaPairRDD<String, Row> getCar2RowRDD(JavaRDD<Row> car2RowRDD) {
		return car2RowRDD.mapToPair(new PairFunction<Row, String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(3), row);
			}
		});
	}
}
