package com.bjsxt.spark.areaRoadFlow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.bjsxt.spark.conf.ConfigurationManager;
import com.bjsxt.spark.constant.Constants;
import com.bjsxt.spark.dao.ITaskDAO;
import com.bjsxt.spark.dao.factory.DAOFactory;
import com.bjsxt.spark.domain.Task;
import com.bjsxt.spark.util.ParamUtils;
import com.bjsxt.spark.util.SparkUtils;

import scala.Tuple2;

/**
 * 计算出每一个区域top3的道路流量
 * 	每一个区域车流量最多的3条道路   每条道路有多个卡扣
 * @author root
 * ./spark-submit  --master spark://hadoop1:7077 --executor-memory 512m  --driver-class-path /software/Hive/hive-1.2.1/lib/mysql-connector-java-5.1.26-bin.jar:/root/resource/fastjson-1.2.11.jar  --jars /software/Hive/hive-1.2.1/lib/mysql-connector-java-5.1.26-bin.jar,/root/resource/fastjson-1.2.11.jar  ~/resource/AreaTop3RoadFlowAnalyze.jar 
 * 
 * 这是一个分组取topN  SparkSQL分组取topN    开窗函数吧。  按照谁开窗？区域，道路流量排序，rank 小鱼等于3
 * 0
 * 区域，道路流量排序         按照区域和道路进行分组  
 * SELECT 
 * 	area，
 * 	road_id,
 * 	groupConcatDistinst(monitor_id,car) monitor_infos,
 * 	count(0 car_count
 * group by area，road_id
 *
 */
public class AreaTop3RoadFlowAnalyze {
	public static void main(String[] args) {
			// 创建SparkConf
				SparkConf conf = new SparkConf()
						.setAppName("AreaTop3ProductSpark");
				SparkUtils.setMaster(conf); 
				
				// 构建Spark上下文
				JavaSparkContext sc = new JavaSparkContext(conf);
				SQLContext sqlContext = SparkUtils.getSQLContext(sc);
//				sqlContext.setConf("spark.sql.shuffle.partitions", "2000"); 
				//"SELECT * FROM table1 join table2 ON (连接条件)"  如果某一个表小时20G 他会自动广播出去
				
//				sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");  KB
				
				
				// 注册自定义函数
				sqlContext.udf().register("concat_String_string", 
						new ConcatStringStringUDF(), DataTypes.StringType);
				sqlContext.udf().register("random_prefix", 
						new RandomPrefixUDF(), DataTypes.StringType);
				sqlContext.udf().register("remove_random_prefix", 
						new RemoveRandomPrefixUDF(), DataTypes.StringType); 
				sqlContext.udf().register("group_concat_distinct", 
						new GroupConcatDistinctUDAF());
				
				
				// 准备模拟数据
				SparkUtils.mockData(sc, sqlContext);  
				
				// 获取命令行传入的taskid，查询对应的任务参数
				ITaskDAO taskDAO = DAOFactory.getTaskDAO();
				
				long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW);
				Task task = taskDAO.findTaskById(taskid);
				 if(task == null){
					 return ;
				 }
				 System.out.println(task.getTaskParams());
				 JSONObject taskParam = JSONObject.parseObject(task.getTaskParams());
				 
				 /**
				  * 获取指定日期内的参数
				  * monitor_id  car   road_id	area_id
				  */
				 JavaPairRDD<String, Row> areaId2DetailInfos = getInfosByDateRDD(sqlContext,taskParam);
								 
				 /**
				  * 从异构数据源MySQL中获取区域信息
				  * area_id	area_name
				  * areaId2AreaInfoRDD<area_id,row:详细信息>
				  */
				 JavaPairRDD<String, Row> areaId2AreaInfoRDD = getAreaId2AreaInfoRDD(sqlContext);
				
				 /**
				  * 补全区域信息    添加区域名称
				  * 	monitor_id car road_id	area_id	area_name 
				  * 生成基础临时信息表
				  * 	tmp_car_flow_basic
				  */
				 generateTempRoadFlowBasicTable(sqlContext,areaId2DetailInfos,areaId2AreaInfoRDD);
				 
				 /**
				  * 统计各个区域各个路段车流量的临时表
				  */
 				 generateTempAreaRoadFlowTable(sqlContext);
				 
				 /**
				  *  area_name
					 road_id
					 count(*) car_count
					 monitor_infos   
				  * 使用开窗函数  获取每一个区域的topN路段
				  */
   				 getAreaTop3RoadFolwRDD(sqlContext);
 				
				sc.close();
	}

	@SuppressWarnings("unused")
	private static JavaRDD<Row> getAreaTop3RoadFolwRDD(SQLContext sqlContext) {
		/**
		 * tmp_area_road_flow_count表：
		 * 		area_name
		 * 		road_id
		 * 		car_count
		 * 		monitor_infos
		 */
		String sql = ""
				+ "SELECT "
					+ "area_id,"
					+ "road_id,"
					+ "car_count,"
					+ "monitor_infos, "
					+ "CASE "
						+ "WHEN car_count > 170 THEN 'A LEVEL' "
						+ "WHEN car_count > 160 AND car_count <= 170 THEN 'B LEVEL' "
						+ "WHEN car_count > 150 AND car_count <= 160 THEN 'C LEVEL' "
						+ "ELSE 'D LEVEL' "
					+"END flow_level "
				+ "FROM ("
					+ "SELECT "
						+ "area_id,"
						+ "road_id,"
						+ "car_count,"
						+ "monitor_infos,"
						+ "row_number() OVER (PARTITION BY area_id ORDER BY car_count DESC) rn "
					+ "FROM tmp_area_road_flow_count "
					+ ") tmp "
				+ "WHERE rn <=3"; 
		DataFrame df = sqlContext.sql(sql);
		df.show();
		return df.javaRDD();
	}

	private static void generateTempAreaRoadFlowTable(SQLContext sqlContext) {
		/**
		 * 	structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));  
			structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
			structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
			structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));  
			structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));  
		 */
		String sql = 
				"SELECT "
				+ "area_id,"
				+ "road_id,"
				+ "count(*) car_count,"
				+ "group_concat_distinct(monitor_id) monitor_infos "
				+ "FROM tmp_car_flow_basic "
				+ "GROUP BY area_id,road_id";
		
		 	String sqlText = ""
					+ "SELECT "
						+ "area_name_road_id,"
						+ "sum(car_count),"
						+ "group_concat_distinct(monitor_infos) monitor_infoss "
					+ "FROM ("
						+ "SELECT "
							+ "remove_random_prefix(area_name_road_id) area_name_road_id,"
							+ "car_count,"
							+ "monitor_infos "
						+ "FROM ("
							+ "SELECT "
								+ "area_name_road_id,"
								+ "count(*) car_count,"
								+ "group_concat_distinct(monitor_id) monitor_infos "
							+ "FROM ("
								+ "SELECT "
								+ "monitor_id,"
								+ "car,"
								+ "random_prefix(concat_String_string(area_name,road_id,':'),10) area_name_road_id "
								+ "FROM tmp_car_flow_basic "
							+ ") t1 "
							+ "GROUP BY area_name_road_id "
						+ ") t2 "
					+ ") t3 "
					+ "GROUP BY area_name_road_id"; 
		
		
		DataFrame df = sqlContext.sql(sql);		
	
//		df.show();
		df.registerTempTable("tmp_area_road_flow_count"); 
	}

	private static void generateTempRoadFlowBasicTable(SQLContext sqlContext, JavaPairRDD<String, Row> areaId2DetailInfos, JavaPairRDD<String, Row> areaId2AreaInfoRDD) {
		
		JavaRDD<Row> tmpRowRDD = areaId2DetailInfos.join(areaId2AreaInfoRDD).map(new Function<Tuple2<String,Tuple2<Row,Row>>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Row, Row>> tuple) throws Exception {
				String areaId = tuple._1;
				Row carFlowDetailRow = tuple._2._1;
				Row areaDetailRow = tuple._2._2;
				
				String monitorId = carFlowDetailRow.getString(0);
				String car = carFlowDetailRow.getString(1);
				String roadId = carFlowDetailRow.getString(2);
				
				String areaName = areaDetailRow.getString(1);
				
				return RowFactory.create(areaId, areaName, roadId, monitorId,car); 
			}
		});
		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));  
		structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));  
		structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));  
	 
		
		StructType schema = DataTypes.createStructType(structFields);
		
		DataFrame df = sqlContext.createDataFrame(tmpRowRDD, schema);
		
		df.registerTempTable("tmp_car_flow_basic");
		
	}

	private static JavaPairRDD<String, Row> getAreaId2AreaInfoRDD(SQLContext sqlContext) {
				String url = null;
				String user = null;
				String password = null;
				boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
				
				if(local) {
					url = ConfigurationManager.getProperty(Constants.JDBC_URL);
					user = ConfigurationManager.getProperty(Constants.JDBC_USER);
					password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
				} else {
					url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
					user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
					password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
				}
				
				Map<String, String> options = new HashMap<String, String>();
				options.put("url", url);
				options.put("dbtable", "area_info");  
				options.put("user", user);
				options.put("password", password);
				
				// 通过SQLContext去从MySQL中查询数据
				DataFrame areaInfoDF = sqlContext.read().format("jdbc")
						.options(options).load();
				
				// 返回RDD
				JavaRDD<Row> areaInfoRDD = areaInfoDF.javaRDD();
			
				JavaPairRDD<String, Row> areaid2areaInfoRDD = areaInfoRDD.mapToPair(
					
						new PairFunction<Row, String, Row>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<String, Row> call(Row row) throws Exception {
								String areaid = String.valueOf(row.get(0));  
								return new Tuple2<String, Row>(areaid, row);
							}
						});
				
				return areaid2areaInfoRDD;
	}

	private static JavaPairRDD<String, Row> getInfosByDateRDD(SQLContext sqlContext, JSONObject taskParam) {
		 String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		 String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		 
		 String sql = "SELECT "
		 		+ "monitor_id,"
		 		+ "car,"
		 		+ "road_id,"
		 		+ "area_id "
		 		+ "FROM	monitor_flow_action "
		 		+ "WHERE date >= '"+startDate+"'"
		 				+ "AND date <= '"+endDate+"'";
		 DataFrame df = sqlContext.sql(sql);
		 return df.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String areaId = row.getString(3);
				return new Tuple2<String, Row>(areaId,row);
			}
		});
	}
}
