package com.gx.session;

import com.alibaba.fastjson.JSONObject;
import com.gx.session.utils.Constants;
import com.gx.session.utils.DateUtils;
import com.gx.session.utils.ParamUtils;
import com.gx.session.utils.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;


/**
 * 用户访问session分析Spark作业
 * @author Administrator
 *
 */
public class UserVisitSessionAnalyzeSpark {

	public static void main(String[] args) {
		// 构建Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName("session")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());

		// 生成模拟测试数据
		mockData(sc, sqlContext);

		// 创建需要使用的DAO组件
	/*	ITaskDAO taskDAO = DAOFactory.getTaskDAO();

		// 首先得查询出来指定的任务，并获取任务的查询参数
		long taskid = ParamUtils.getTaskIdFromArgs(args);
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());*/
		JSONObject taskParam = JSONObject.parseObject("{'startAge':['10'],'endAge':['50'],'startDate':['2018-03-01'],'endDate':['2018-03-25']}");
		// 如果要进行session粒度的数据聚合
		// 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SessionAggrStatAccumulator());
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);


		// 首先，可以将行为数据，按照session_id进行groupByKey分组
		// 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
		// 与用户信息数据，进行join
		// 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
		JavaPairRDD<String, String> sessionid2AggrInfoRDD =
				aggregateBySession(sqlContext, actionRDD,sessionAggrStatAccumulator);

		// 关闭Spark上下文
        System.out.println(sessionid2AggrInfoRDD.count());
		System.out.println(sessionAggrStatAccumulator.value());
		sc.close();
	}
	
	/**
	 * 获取SQLContext
	 * 如果是在本地测试环境的话，那么就生成SQLContext对象
	 * 如果是在生产环境运行的话，那么就生成HiveContext对象
	 * @param sc SparkContext
	 * @return SQLContext
	 */
	private static SQLContext getSQLContext(SparkContext sc) {
		/*boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}*/
		return new SQLContext(sc);
	}
	
	/**
	 * 生成模拟数据（只有本地模式，才会去生成模拟数据）
	 * @param sc 
	 * @param sqlContext
	 */
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		/*boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);  
		}*/
		MockData.mock(sc, sqlContext);
	}

	/**
	 * 获取指定日期范围内的用户访问行为数据
	 * @param sqlContext SQLContext
	 * @param taskParam 任务参数
	 * @return 行为数据RDD
	 */
	private static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext, JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		System.out.println(startDate+endDate);
		String sql =
				"select * "
						+ "from user_visit_action "
						/*+ "where date>='" + startDate + "' "
						+ "and date<='" + endDate + "'";*/
                        +" where date Between '"+ startDate +"' And  '"+ endDate +"'";
		DataFrame actionDF = sqlContext.sql(sql);

		return actionDF.javaRDD();
	}

	/**
	 * 对行为数据按session粒度进行聚合
	 * @param actionRDD 行为数据RDD
	 * @return session粒度聚合数据
	 */
	private static JavaPairRDD<String, String> aggregateBySession(
			SQLContext sqlContext, JavaRDD<Row> actionRDD,final Accumulator<String> sessionAggrStatAccumulator ) {
		// 现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
		// 我们现在需要将这个Row映射成<sessionid,Row>的格式
		JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(

				/**
				 * PairFunction
				 * 第一个参数，相当于是函数的输入
				 * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
				 */
				new PairFunction<Row, String, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(Row row) throws Exception {
						return new Tuple2<String, Row>(row.getString(2), row);
					}

				});

		// 对行为数据按session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
				sessionid2ActionRDD.groupByKey();
	
		// 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		// 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(

				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
							throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();

						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

						Long userid = null;
                       // session的访问步长
						int stepLength = 0;
						// session的起始和结束时间
						Date startTime = null;
						Date endTime = null;
						// 遍历session所有的访问行为
						while(iterator.hasNext()) {
							//统计总的访问次数
							sessionAggrStatAccumulator.add("all");
							// 提取每个访问行为的搜索词字段和点击品类字段
							Row row = iterator.next();
							if(userid == null) {
								userid = row.getLong(1);
							}
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);

							// 实际上这里要对数据说明一下
							// 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
							// 其实，只有搜索行为，是有searchKeyword字段的
							// 只有点击品类的行为，是有clickCategoryId字段的
							// 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

							// 我们决定是否将搜索词或点击品类id拼接到字符串中去
							// 首先要满足：不能是null值
							// 其次，之前的字符串中还没有搜索词或者点击品类id

							if(StringUtils.isNotEmpty(searchKeyword)) {
								if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");
								}
							}
							if(clickCategoryId != null) {
								if(!clickCategoryIdsBuffer.toString().contains(
										String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
							Date actionTime = DateUtils.parseTime(row.getString(4));
							if(startTime == null) {
								startTime = actionTime;
							}
							if(endTime == null) {
								endTime = actionTime;
							}

							if(actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if(actionTime.after(endTime)) {
								endTime = actionTime;
							}

							stepLength++;
						}

						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						// 大家思考一下
						// 我们返回的数据格式，即使<sessionid,partAggrInfo>
						// 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
						// 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
						// 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
						// 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
						// 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

						// 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
						// 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
						// 然后再直接将返回的Tuple的key设置成sessionid
						// 最后的数据格式，还是<sessionid,fullAggrInfo>

						// 聚合数据，用什么样的格式进行拼接？
						// 我们这里统一定义，使用key=value|key=value
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds
								+ "|steplength="+stepLength
								+ "|timelength="+visitLength
								;
						calculateVisitLength(visitLength);
						calculateStepLength(stepLength);

						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
/*"all=0|"
		+"30s=0|"
		+"120s=0|"
		+"300s=0|"
		+"6000s=0|"
		+"step10=0|"
		+ "step30=0|"
		+ "step60=0"*/
					private void calculateVisitLength(long visitLength) {
						if(visitLength >=1 && visitLength <= 30) {
							sessionAggrStatAccumulator.add("30s");
						} else if(visitLength >=31 && visitLength <= 120) {
							sessionAggrStatAccumulator.add("120s");
						} else if(visitLength >=121 && visitLength <= 300) {
							sessionAggrStatAccumulator.add("300s");
						} else if(visitLength >=301 && visitLength <= 6000) {
							sessionAggrStatAccumulator.add("6000s");
						} else {
							sessionAggrStatAccumulator.add("mt6000s");
						}
					}


					private void calculateStepLength(long stepLength) {
						if(stepLength >= 1 && stepLength <= 10) {
							sessionAggrStatAccumulator.add("step10");
						} else if(stepLength >= 11 && stepLength <= 30) {
							sessionAggrStatAccumulator.add("step30");
						} else if(stepLength >= 31 && stepLength <= 60) {
							sessionAggrStatAccumulator.add("step60=0");
						}else  {
							sessionAggrStatAccumulator.add("mtstep60");
						}
					}

				});

		// 查询所有用户数据，并映射成<userid,Row>的格式
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(

				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}

				});

		// 将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
				userid2PartAggrInfoRDD.join(userid2InfoRDD);

		// 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Long, Tuple2<String, Row>> tuple)
							throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;

						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);

						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex
								+ "|" +userInfoRow.getLong(0);
								;
						//System.out.println(fullAggrInfo);
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}

				});

		return sessionid2FullAggrInfoRDD;
	}




	
}
