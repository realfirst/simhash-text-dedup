package com.zhongsou.spider.hadoop.jobcontrol;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.zhongsou.spider.bean.JobInfo;
import com.zhongsou.spider.exception.EmptyParamValueException;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;
import com.zhongsou.spider.hadoop.job.CounterHelper;

/**
 * 
 * url打分与控制的job，完成两个功能，url打分与下载
 * 
 * <pre>
 * 1  url打分，扫描整个的hbase，计算出下次下载的url，待到剩余的url的文件个数小于某个值时启动一个新的job
 * 2  下载的job，下载网页
 * </pre>
 * 
 * 
 * @author Xi Qi
 * 
 */

public class ScoreAndClawerJob extends SpiderJob {

	static Logger logger = Logger.getLogger(ScoreAndClawerJob.class);
	private static String SCORE_OUTPUT_TEMP = "/user/kaifa/score_output";

	public ScoreAndClawerJob(Configuration conf) throws IOException {
		super(conf);
		// TODO Auto-generated constructor stub
		SCORE_OUTPUT_TEMP = conf.get("score_output_temp", SCORE_OUTPUT_TEMP);
		this.jobInfoMap = jobMap.get(this.getClass().getName());
		this.scoreThread = new ScoreURLThread(false);
		this.downloadThread = new DownloadURLThread(false);
		scoreNewThread = new ScoreURLThread(true);
		downloadNewThread = new DownloadURLThread(true);
		scanSortURLFolder();
		System.out.println("jobInfo " + jobInfoMap.size());
	}

	LinkedBlockingQueue<String> oldURLSeedQueue = new LinkedBlockingQueue<String>();
	LinkedBlockingQueue<String> newURLSeedQueue = new LinkedBlockingQueue<String>();
	ScoreURLThread scoreThread;
	DownloadURLThread downloadThread;

	ScoreURLThread scoreNewThread;
	DownloadURLThread downloadNewThread;

	int leftURLThreshold = 0;
	int max_score_url_retry = conf.getInt("max_score_url_retry", 10);
	int download_retry_times = conf.getInt("download_retry_times", 3);

	@Override
	void checkFolderExists() {
		// TODO Auto-generated method stub
		HDFSUtil.mkdirs(fs, ROOT_DIR + "/" + SCORE_OUTPUT_DIR);
		HDFSUtil.mkdirs(fs, ROOT_DIR + "/" + SELECT_URL_OUTPUT_DIR);
		HDFSUtil.mkdirs(fs, ROOT_DIR + "/" + SORT_URL_OUTPUT_DIR);
		HDFSUtil.mkdirs(fs, ROOT_DIR + "/" + CLAWER_OUTPUT_DIR);
		HDFSUtil.mkdirs(fs, ROOT_DIR + "/" + PIPES_BIN_DIR);
	}

	HashMap<String, JobInfo> jobInfoMap;

	public int scanSortURLFolder() {
		ArrayList<FileInfo> tlist = new ArrayList<FileInfo>();
		HDFSUtil.listFile(fs, ROOT_DIR + "/" + SORT_URL_OUTPUT_DIR, tlist, true);
		for (FileInfo fileinfo : tlist) {
			String path = fileinfo.getPath();
			String name = path.substring(path.lastIndexOf("/") + 1);
			if (!fileinfo.isDir() && path.matches(".*old_\\d{14}.*")&&name.matches("\\d{14}")
					&& !this.oldURLSeedQueue.contains(path)) {
				this.oldURLSeedQueue.add(path);
				logger.info("add existed score url to old url queue,path="
						+ path + "\tqueue size=" + this.oldURLSeedQueue.size());
			} else if (!fileinfo.isDir() && path.matches(".*new_\\d{14}.*")&&name.matches("\\d{14}")
					&& !this.newURLSeedQueue.contains(path)) {
				this.newURLSeedQueue.add(path);
				logger.info("add existed score url to new url queue,path="
						+ path + "\tqueue size=" + this.newURLSeedQueue.size());
			}
		}
		return this.oldURLSeedQueue.size() + this.newURLSeedQueue.size();

	}

	/**
	 * 
	 * 强制sleep 2秒，从而保证任何调用本方法的时间的秒都不相同
	 * 
	 * @return
	 */
	private synchronized long getCurrentTime() {
		long t = System.currentTimeMillis();
		try {
			Thread.sleep(1000 * 2);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return t;
	}

	class ScoreURLThread implements Runnable {
		boolean isNew = false;
		JobInfo scoreJob;
		JobInfo selectURLJob;
		JobInfo sortURLJobinfo;
		LinkedBlockingQueue<String> urlSeedQueue;

		public ScoreURLThread(boolean newURL) {
			this.isNew = newURL;
			if (!isNew) {
				scoreJob = jobInfoMap.get("score_url");
				selectURLJob = jobInfoMap.get("select_url");
				this.urlSeedQueue = oldURLSeedQueue;
				sortURLJobinfo = jobInfoMap.get("sort_url_by_docid");
			} else {
				scoreJob = jobInfoMap.get("score_new_url");
				selectURLJob = jobInfoMap.get("select_new_url");
				sortURLJobinfo = jobInfoMap.get("sort_new_url_by_docid");
				this.urlSeedQueue = newURLSeedQueue;
				
			}
			
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			int errorCount = 0;
			while (running) {
				synchronized (this) {
					while (running && urlSeedQueue.size() > leftURLThreshold) {
						try {
							this.wait(10 * 1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				if (!sumbitScoreJob()) {
					logger.info("score url job failed");
					try {
						Thread.sleep(1000 * 30);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					errorCount++;
				} else {
					errorCount = 0;
				}
				if (errorCount > max_score_url_retry) {
					logger.error("score url failed excced max failed times"
							+ max_score_url_retry);
					System.exit(0);
				}

			}
		}

		public boolean sumbitScoreJob() {
			boolean flag = false;

			Date d = new Date();
			String time = DATE_FORMAT.format(d);
			String score_output = ROOT_DIR + "/" + SCORE_OUTPUT_DIR;
			String selectOutput = ROOT_DIR + "/" + SELECT_URL_OUTPUT_DIR + "/";

			String sortURLOutput = ROOT_DIR + "/" + SORT_URL_OUTPUT_DIR + "/";
			if (this.isNew) {
				sortURLOutput += "new_" + time;
				score_output += "_new";
				selectOutput += "new_" + time;
			} else {
				sortURLOutput += "old_" + time;
				selectOutput += "old_" + time;
			}
			String bindir = ROOT_DIR + "/" + PIPES_BIN_DIR;

			String pwd_home = propertiesMap.get("$PWD_HOME");
			// 每次下载之前删除以前的二进制文件，上传新的二进制文件
			HDFSUtil.rmdirs(fs, bindir + "/score_url");
			HDFSUtil.rmdirs(fs, score_output);
			HDFSUtil.upload(fs, pwd_home + "/spider_c++_common/bin/score_url",
					bindir);
			scoreJob.getParamMap().put("$score_url_bin", bindir + "/score_url");
			scoreJob.getParamMap().put("$score_output", score_output);
			try {
				String command = scoreJob.generateCommand();
				logger.info("submit score job comand =" + command);
				RunningJob job = (RunningJob) (JobCreateFactory
						.createJobObject(scoreJob));
				job.waitForCompletion();
				logger.info("score job execute finished" + command + " "
						+ job.isSuccessful());
				if (job.isSuccessful()) {
					long reduceRecords = CounterHelper.getCounterValue(job,
							Task.Counter.REDUCE_OUTPUT_RECORDS);
					logger.info("socre job succesful reduce records"
							+ reduceRecords);
					if (reduceRecords > 0) {
						selectURLJob.getParamMap().put("$reduce_records",
								String.valueOf(reduceRecords));
						selectURLJob.getParamMap().put("$sort_select_output",
								selectOutput);
						selectURLJob.getParamMap().put("$score_output",
								score_output);

						Job selectjob = (Job) (JobCreateFactory
								.createJobObject(selectURLJob));
						logger.info("select url job start"
								+ selectURLJob.generateCommand());
						selectjob.waitForCompletion(true);
						logger.info("select url job finished"
								+ selectURLJob.generateCommand() + "\t"
								+ selectjob.isSuccessful());
						if (selectjob.isSuccessful()) {

							sortURLJobinfo.getParamMap().put("$select_output",
									selectOutput);
							sortURLJobinfo.getParamMap().put(
									"$sort_url_output", sortURLOutput);

							Job sortURLJob = (Job) (JobCreateFactory
									.createJobObject(sortURLJobinfo));

							logger.info("sort url job start "
									+ sortURLJobinfo.generateCommand());
							sortURLJob.waitForCompletion(true);
							logger.info("sort url job end");

							if (sortURLJob.isSuccessful()) {

								List<FileInfo> flist = new LinkedList<FileInfo>();
								HDFSUtil.listFile(fs, sortURLOutput, flist,
										false);
								if (flist.size() == 0) {
									logger.error("score output size="
											+ flist.size());
								} else {
									int succeed = 0;
									for (int i = 0; i < flist.size(); i++) {
										long timestamp = getCurrentTime();
										Date mtime = new Date(timestamp);
										FileInfo fileInfo = flist.get(i);
										if (fileInfo.isDir()
												|| fileInfo.getFileLen() == 0)
											continue;

										String fileName = fileInfo.getPath();
										int lastIndex = fileName
												.lastIndexOf("/");
										String folder = fileName.substring(0,
												lastIndex + 1);
										String newName = folder
												+ DATE_FORMAT.format(mtime);
										if (!HDFSUtil.renameFile(fs, fileName,
												newName)) {
											logger.error("rename score file failed "
													+ fileName
													+ "\t newName="
													+ newName);
											continue;
										} else {
											try {
												urlSeedQueue.put(newName);
												succeed++;
												logger.info("add select url file to queue succeed name="
														+ newName);
											} catch (InterruptedException e) {
												// TODO Auto-generated catch
												// block
												e.printStackTrace();
											}
										}

									}
									if (succeed > 0)
										flag = true;

								}

								String newSelctOuput = selectOutput
										.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
								if (!HDFSUtil.renameFile(fs, selectOutput,
										newSelctOuput)) {
									logger.error("rename select url outptu failed,src ="
											+ selectOutput
											+ "\tdst="
											+ newSelctOuput);
								}

							}

						} else {
							logger.error("select url failed " + command);
						}
					} else {
						logger.error("score job failed reduce records equals 0 command="
								+ command);
					}

				} else {
					logger.error("score job failed +command=" + command);
				}

			} catch (EmptyParamValueException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				// 删除score的临时目录
				// HDFSUtil.rmdirs(fs, score_output);
			}

			return flag;
		}

	}

	class DownloadURLThread implements Runnable {
		public boolean isNew;
		LinkedBlockingQueue<String> urlSeedQueue;

		public DownloadURLThread(boolean isNew) {
			this.isNew = isNew;
			if (this.isNew) {
				this.urlSeedQueue = newURLSeedQueue;
			} else {
				this.urlSeedQueue = oldURLSeedQueue;
			}
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (running) {
				synchronized (this) {
					while (running && urlSeedQueue.size() == 0) {
						try {
							this.wait(10 * 1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				String fileName = urlSeedQueue.poll();
				if (fileName == null) {
					logger.error("error seed url file name"
							+ urlSeedQueue.size());
					continue;
				}
				int failed = 0;
				do {
					boolean t = submitDownloadJob(fileName);
					if (t)
						break;
					else
						failed++;
				} while (failed < download_retry_times);
				if (failed == download_retry_times) {
					logger.error("failed down " + fileName
							+ " reach max retry,exit");
					System.exit(0);
				}

				// 将下载的url列表，移到备份的目录之下，不管是否成功，都将此url seed文件移走
				String bakFileName = fileName.replaceFirst(ROOT_DIR,
						ROOT_DIR_BAK);
				HDFSUtil.renameFile(fs, fileName, bakFileName);
			}
		}

		public boolean submitDownloadJob(String fileName) {
			boolean flag = false;
			JobInfo jobInfo = jobInfoMap.get("claw_url");
			String longtime = fileName.substring(fileName.lastIndexOf("/") + 1);

			String bindir = ROOT_DIR + "/" + PIPES_BIN_DIR;

			String pwd_home = propertiesMap.get("$PWD_HOME");
			// 每次下载之前删除以前的二进制文件，上传新的二进制文件
			HDFSUtil.rmdirs(fs, bindir + "/clawer_url");
			HDFSUtil.upload(fs, pwd_home + "/spider_c++_common/bin/clawer_url",
					bindir);

			String output = ROOT_DIR + "/" + CLAWER_OUTPUT_DIR + "/" + longtime;
			jobInfo.getParamMap().put("$output_folder", output + "_bak");
			jobInfo.getParamMap().put("$inputFile", fileName);
			jobInfo.getParamMap().put("$name", "clawer_url_" + longtime);
			jobInfo.getParamMap()
					.put("$clawer_url_bin", bindir + "/clawer_url");

			RunningJob job = (RunningJob) JobCreateFactory
					.createJobObject(jobInfo);
			try {
				if (fs.exists(new Path(output + "_bak"))) {
					HDFSUtil.rmdirs(fs, output + "_bak");
				}
				logger.info("claw job start command="
						+ jobInfo.generateCommand());
				job.waitForCompletion();
				logger.info("claw job finished " + job.isSuccessful());
				if (job.isSuccessful()) {
					flag = true;
					// 完成下载之后，将下载的文件夹进行改名，防止分析的job分析没有完成下载的文件夹
					HDFSUtil.renameFile(fs, output + "_bak", output);

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (EmptyParamValueException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {

			}

			return flag;
		}

	}

	@Override
	public void runJob() {
		// TODO Auto-generated method stub
		Thread score = new Thread(this.scoreThread);
		Thread download = new Thread(this.downloadThread);
		Thread scoreNew = new Thread(this.scoreNewThread);
		Thread downloadNew = new Thread(this.downloadNewThread);
		score.start();
		download.start();
		 scoreNew.start();
		 downloadNew.start();
		while ((this.running)) {
			try {
				Thread.sleep(30 * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String args[]) {
		try {
			Properties p = new Properties();
			p.load(SelectAndSendJob.class
					.getResourceAsStream("/log4j.properties"));
			PropertyConfigurator.configure(p);
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Configuration conf = HBaseConfiguration.create();
		try {
			ScoreAndClawerJob scoreJob = new ScoreAndClawerJob(conf);
			scoreJob.runJob();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
