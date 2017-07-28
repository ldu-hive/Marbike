package com.bjsxt.spark.skynet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.bjsxt.spark.constant.Constants;
import com.bjsxt.spark.dao.ICarTrackDAO;
import com.bjsxt.spark.dao.IRandomExtractDAO;
import com.bjsxt.spark.dao.ITaskDAO;
import com.bjsxt.spark.dao.factory.DAOFactory;
import com.bjsxt.spark.domain.CarTrack;
import com.bjsxt.spark.domain.RandomExtractCar;
import com.bjsxt.spark.domain.RandomExtractMonitorDetail;
import com.bjsxt.spark.domain.Task;
import com.bjsxt.spark.util.DateUtils;
import com.bjsxt.spark.util.ParamUtils;
import com.bjsxt.spark.util.SparkUtils;
import com.bjsxt.spark.util.StringUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

public class RandomExtractCars {
	public static void main(String[] args) {
		// 构建Spark上下文
				SparkConf conf = new SparkConf()
						.setAppName(Constants.SPARK_APP_NAME_SESSION)
						.set("spark.sql.shuffle.partitions", "10")
//						.set("spark.default.parallelism", "100")
//						.set("spark.storage.memoryFraction", "0.5")  
//						.set("spark.shuffle.consolidateFiles", "true")
//						.set("spark.shuffle.file.buffer", "64")  
//						.set("spark.shuffle.memoryFraction", "0.3")    
//						.set("spark.reducer.maxSizeInFlight", "24")  
//						.set("spark.shuffle.io.maxRetries", "60")  
//						.set("spark.shuffle.io.retryWait", "60")   
//						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
						;
				SparkUtils.setMaster(conf); 
				
				JavaSparkContext sc = new JavaSparkContext(conf);
				
				SQLContext sqlContext = SparkUtils.getSQLContext(sc);
				
				/**
				 * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的临时表就可以
				 * 本地模拟数据注册成一张临时表
				 * monitor_flow_action
				 */
				SparkUtils.mockData(sc, sqlContext);
				
				
				/**
				 * 给定一个时间段，统计出卡口数量的正常数量，异常数量，还有通道数
				 * 异常数：每一个卡口都会有n个摄像头对应每一个车道，
				 * 		如果这一段时间内卡口的信息没有第N车道的信息的话就说明这个卡口存在异常。
				 * 这需要拿到一份数据（每一个卡口对应的摄像头的编号）  
				 * 模拟数据在monitor_camera_info临时表中
				 */
				
				//从配置文件中查询出来指定的任务ID
				long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_EXTRACT_CAR);
				
				/**
				 * 通过taskId从数据库中查询相应的参数
				 * 	1、通过DAOFactory工厂类创建出TaskDAO组件
				 * 	2、查询task
				 */
				ITaskDAO taskDAO = DAOFactory.getTaskDAO();
				Task task = taskDAO.findTaskById(taskId);
				
				if(task == null){
					return;
				}
				
				/**
				 * task对象已经获取到，因为params是一个json，所以需要创建一个解析json的对象 
				 */
				JSONObject taskParamsJsonObject = JSONObject.parseObject(task.getTaskParams());

				
				/**
				 * 统计出这一段时间内，所有卡口的状态，所以需要根据param参数，去camera_flow_action临时表中获取结果
				 */
				JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(sqlContext, taskParamsJsonObject);

				/**
  				 * 随机抽取N个车辆信息，比如一天有24个小时，其中08:00~09:00的车辆数量占当天总车流量的50%，在这天中我们需要随机抽取100个，
  				 * 那么08:00~09:00的，就得抽取100*50%=50，而且这50个需要随机抽取。
  				 * 我 们需要使用Spark自己实现一个算法，按照时间段分段抽取车辆信息，然后这些车辆信息可以很权威的代表整个城市的车辆信息，
  				 * 我们可以基于这些抽样的数据进行数据分析，可以绘制出这些车辆每天的运行轨迹，对于道路的规划起到了很重要的作用，
  				 * 比如，我们抽样出来的数据80%的车辆在早高峰和晚高峰都是基本同样的行车轨迹，然而他们每天途径的路段都会堵车，这时候我们可以根据这些数据对道路进行规划
  				 * 可以根据用户的画像进行多维度的数据分析
  				 */
  				JavaPairRDD<String, Row> randomExtractCar2DetailRDD = randomExtractCarInfo(sc,taskId,taskParamsJsonObject,cameraRDD);
  				
  				/**
  				 * carTrackRDD<String,String>
  				 * k:car
  				 * v:date|carTracker
  				 */
  				JavaPairRDD<String, String> carTrackRDD = getCarTrack(taskId,randomExtractCar2DetailRDD);
  				/**
  				 * 将每一辆车的轨迹信息写入到数据库中
  				 */
  				saveCarTrack2DB(taskId,carTrackRDD);
  				
 				sc.close();				
	}

	
	private static void saveCarTrack2DB(final long taskId,JavaPairRDD<String, String> carTrackRDD) {
		carTrackRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
				 
				ICarTrackDAO carTrackDAO = DAOFactory.getCarTrackDAO();
				List<CarTrack> carTracks = new ArrayList<>();
				while (iterator.hasNext()) {
					Tuple2<String, String> tuple = iterator.next();
					String car = tuple._1;
					String dateAndCarTrack = tuple._2;
					String date = StringUtils.getFieldFromConcatString(dateAndCarTrack, "\\|", Constants.FIELD_DATE);
					String track = StringUtils.getFieldFromConcatString(dateAndCarTrack, "\\|",Constants.FIELD_CAR_TRACK);
					CarTrack carTrack = new CarTrack(taskId, date,car, track);
					carTracks.add(carTrack);
				}
				carTrackDAO.insertBatchCarTrack(carTracks);
			}
		});
		
	}


	private static JavaPairRDD<String, String> getCarTrack(long taskId, JavaPairRDD<String, Row> randomExtractCar2DetailRDD) {
		JavaPairRDD<String, Iterable<Row>> groupByCar = randomExtractCar2DetailRDD.groupByKey();
		JavaPairRDD<String, String> carTrackRDD = groupByCar.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				String car = tuple._1;
				Iterator<Row> carMetailsIterator = tuple._2.iterator();
				List<Row> rows = new ArrayList<>();
				while(carMetailsIterator.hasNext()){
					Row row = carMetailsIterator.next();
					rows.add(row);
				}
				
				Collections.sort(rows, new Comparator<Row>() {

					@Override
					public int compare(Row r1, Row r2) {
						if(DateUtils.after(r1.getString(4), r2.getString(4))){
							return 1;
						}
						return -1;
					}
				});
				
				StringBuilder carTrack = new StringBuilder();
				String date = "";
				for (Row row : rows) {
					carTrack.append(","+row.getString(1));
					date = row.getString(0);
				}
				return new Tuple2<String, String>(car, Constants.FIELD_DATE+"="+date+"|"+Constants.FIELD_CAR_TRACK+"="+carTrack.substring(1));
			}
		});

		return carTrackRDD;
		
	}


	/**
	 * cameraRDD  
	 * 1、key：8-9	value：carCount    mapTopair   countByKey
	 * 2、计算出来8-9的占全天总车流量的百分比
	 * 3、Map<date,Map<hour,List<Interger>>>
	 * 4、进行抽取        
	 * @param sc
	 * @param taskId
	 * @param params
	 * @param cameraRDD
	 * @return
	 * 
	 * 1、date_hour key   car
	 */
	private static JavaPairRDD<String, Row> randomExtractCarInfo(JavaSparkContext sc, final long taskId,JSONObject params, JavaRDD<Row> cameraRDD) {
		//key:时间段   value：car
		JavaPairRDD<String, String> dateHourCar2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String,String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String actionTime = row.getString(4);
				String dateHour = DateUtils.getDateHour(actionTime);
				String car = row.getString(3);
				/**
				 * 为什么要使用组合Key？
				 *   	因为在某一个时间段内，这一两车很有可能经过多个卡扣
				 */
				String key = Constants.FIELD_DATE_HOUR + "=" + dateHour;
				String value = Constants.FIELD_CAR + "=" + car;
				return new Tuple2<String, String>(key, value);
			}
		});
		
	 	//key-value <car,row>
		JavaPairRDD<String, Row> car2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String , Row>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String car = row.getString(3);
				return new Tuple2<String, Row>(car, row);
			}
		}); 
		
		
		 JavaPairRDD<String, String> dateHour2DetailRDD = dateHourCar2DetailRDD.distinct();
		 
		 /**
		  * String：dateHour
		  * Object:这个小时段的车流量
		  */
		Map<String, Object> countByKey = dateHour2DetailRDD.countByKey();
		
		//将<dateHour,car_count>这种格式改成格式如下： <date,<Hour,count>>
		Map<String, Map<String,Long>> dateHourCountMap = new HashMap<>();
		
		for (Entry<String, Object> entry : countByKey.entrySet()) {
			String dateHour = entry.getKey();
			String[] dateHourSplit = dateHour.split("_");
			String date = dateHourSplit[0];
			String hour = dateHourSplit[1];
			
			Long count = Long.parseLong(String.valueOf(entry.getValue()));
			
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if(hourCountMap == null){
				hourCountMap = new HashMap<String,Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			
			hourCountMap.put(hour, count);
		}
		
		/**
		 * 要抽取的车辆数
		 */
		int extractNums = Integer.parseInt(ParamUtils.getParam(params, Constants.FIELD_EXTRACT_NUM));
		
		//extractNumPerDay = 100
		int extractNumPerDay = extractNums / dateHourCountMap.size();
		
		/**
		 * 记录每天每小时抽取索引的集合
		 */
		Map<String, Map<String,List<Integer>>> dateHourExtractMap = new HashMap<>();
		
		Random random = new Random();
		//dateHourCountMap<String,Map<String,Long>>
		for (Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
			String date = entry.getKey();
			/**
			 * hourCountMap  key:hour  value:carCount
			 */
			Map<String, Long> hourCountMap = entry.getValue();
			
			//计算出这一天总的车流量
			long dateCarCount = 0L;

			for (long tmpHourCount : hourCountMap.values()) {
				dateCarCount += tmpHourCount;
			}
			
			
			
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if(hourExtractMap == null){
				hourExtractMap = new HashMap<String,List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			
			for (Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long hourCarCount = hourCountEntry.getValue();
				
				//计算出这个小时的车流量占总车流量的百分比,然后计算出在这个时间段内应该抽取出来的车辆信息的数量
				int hourExtractNum = (int)(((double)hourCarCount / (double)dateCarCount) * extractNumPerDay);
				
				/**
				 * 如果在这个时间段内抽取的车辆信息数量比这个时间段内的车流量还要多的话，只需要将count的值赋值给hourExtractNum就可以
				 * 
				 * 7-8  num   %  
				 * 全天都比较少
				 * 10000
				 */
				if(hourExtractNum >= hourCarCount){
					hourExtractNum = (int)hourCarCount;
				}
				
				//获取当前小时 存储随机数的List集合
				List<Integer> extractIndexs = hourExtractMap.get(hour);
				if(extractIndexs == null){
					extractIndexs = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexs);
				}
				
				//生成抽取的car的index，  实际上就是生成一系列的随机数   随机数的范围就是0-count(这个时间段内的车流量)
				for(int i = 0 ; i < hourExtractNum ; i++){
					/**
					 *  50 
					 *  
					 */
					int index = random.nextInt((int)hourCarCount);
					while(extractIndexs.contains(index)){
						index = random.nextInt((int)hourCarCount);
					}
					extractIndexs.add(index);
				}
			}
		}
		
//		Map<String, Map<String, IntList>> fastutilDateHourExtractMap = 	new HashMap<String, Map<String, IntList>>();
//		
//
//		for(Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
//			String date = dateHourExtractEntry.getKey();
//			Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
//			
//			Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
//			
//			for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
//				String hour = hourExtractEntry.getKey();
//				List<Integer> extractList = hourExtractEntry.getValue();
//				
//				IntList fastutilExtractList = new IntArrayList();
//				
//				for(int i = 0; i < extractList.size(); i++) {
//					fastutilExtractList.add(extractList.get(i));  
//				}
//				
//				fastutilHourExtractMap.put(hour, fastutilExtractList);
//			}
//			
//			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
//		}
		
		final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractBroadcast = sc.broadcast(dateHourExtractMap);
		/**
		 * 在dateHour2DetailRDD中进行随机抽取车辆信息，首先第一步：按照date_hour进行分组，然后对组内的信息按照 dateHourExtractBroadcast参数抽取相应的车辆信息
		 * 抽取出来的结果直接放入到MySQL数据库中。
		 */
		JavaPairRDD<String, String> extractCarRDD = dateHour2DetailRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				List<Tuple2<String,String>> list = new ArrayList<>(); 
				List<RandomExtractCar> carRandomExtracts = new ArrayList<>();
				
				String dateHour = t._1;
				//
				 Iterator<String> iterator = t._2.iterator();

				 String date = dateHour.split("_")[0];
				 String hour = dateHour.split("_")[1];
				
				
				 
				 Map<String, Map<String, List<Integer>>> dateHourExtractMap = dateHourExtractBroadcast.value();
				 
				 
				 List<Integer> indexList = dateHourExtractMap.get(date).get(hour);
				 
				 int index = 0;
				 while(iterator.hasNext()){
					 String car = StringUtils.getFieldFromConcatString(iterator.next(), "\\|", Constants.FIELD_CAR);
					 if(indexList.contains(index)){
						 RandomExtractCar carRandomExtract = new RandomExtractCar(taskId, car, date, dateHour);
						 carRandomExtracts.add(carRandomExtract);
						 list.add(new Tuple2<String, String>(car, car));
					 }
					index++;
				}
				 IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
				 randomExtractDAO.insertBatchRandomExtractCar(carRandomExtracts);
				return list;
			}
		});
		
		/**
		 * extractCarRDD  K:car V:car
		 * 
		 */
		JavaPairRDD<String, Row> randomExtractCar2DetailRDD = extractCarRDD.join(car2DetailRDD).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>, String,Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
				List<RandomExtractMonitorDetail> randomExtractMonitorDetails = new ArrayList<>();
				IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
				List<Tuple2<String, Row>> list = new ArrayList<>();
				
				while(iterator.hasNext()){
					Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
					Row row = tuple._2._2;
					String car = tuple._1;
					RandomExtractMonitorDetail m = new RandomExtractMonitorDetail(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
					randomExtractMonitorDetails.add(m);
					list.add(new Tuple2<String, Row>(car, row));
				}
				randomExtractDAO.insertBatchRandomExtractDetails(randomExtractMonitorDetails);
				return list;
			}
		});
		return randomExtractCar2DetailRDD;
	}
}
