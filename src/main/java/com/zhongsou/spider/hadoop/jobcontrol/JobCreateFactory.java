package com.zhongsou.spider.hadoop.jobcontrol;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import com.zhongsou.spider.bean.JobInfo;
import com.zhongsou.spider.exception.EmptyParamValueException;

public class JobCreateFactory {

  static Logger log = Logger.getLogger(JobCreateFactory.class);

  /**
   *
   * 创建一个pipes job，并且执行直到返回
   *
   * @param info
   * @return
   */
  private static Object createPipesJob(JobInfo info) {

    PipesJobCreate o = new PipesJobCreate();
    try {
      RunningJob job = o.submitJob(info.generateCommand().split(" {1,}"));

      return job;
    } catch (EmptyParamValueException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  /**
   *
   * 创建一个mapreduce v1 的job
   *
   *
   * @param info
   * @return
   */

  private static Object createMapreduceV1Job(JobInfo info) {
    try {
      String command = info.generateCommand();
      int firstBlank = command.indexOf(" ");
      if (firstBlank == -1)
        return null;
      String className = command.substring(0, firstBlank);
      String args = command.substring(firstBlank);
      Class c = Class.forName(className.trim());
      Object o = c.newInstance();
      if (o instanceof MapreduceV1Job) {
        MapreduceV1Job t = (MapreduceV1Job) o;
        RunningJob job = t.createRunnableJob(args.split(" {1,}"));
        return job;
      } else {
        log.error(className + " is not a valid Mapreduce v1 job");
        return null;
      }

    } catch (EmptyParamValueException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;

  }

  private static Object createMapreduceV2Job(JobInfo info) {
    try {
      String command = info.generateCommand().trim();
      int firstBlank = command.indexOf(" ");
      if (firstBlank == -1)
        return null;
      String className = command.substring(0, firstBlank);
      String args = command.substring(firstBlank).trim();
      Class c = Class.forName(className.trim());
      log.info("create mapreduce v2 args " + args);
      Object o = c.newInstance();
      if (o instanceof MapreduceV2Job) {
        MapreduceV2Job t = (MapreduceV2Job) o;
        Job job = t.createRunnableJob(args.split(" {1,}"));
        return job;
      } else {
        log.error(className + " is not a valid Mapreduce v2 job");
        return null;
      }
    } catch (EmptyParamValueException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public static Object createJobObject(JobInfo jobinfo) {
    Object o = null;
    switch (jobinfo.getType()) {
      case pipes:
        return createPipesJob(jobinfo);
      case mapreduce_v1: {
        return createMapreduceV1Job(jobinfo);
      }

      case mapreduce_v2: {
        return createMapreduceV2Job(jobinfo);
      }
      default: {
        log.error("error message for job info " + jobinfo.toString());
      }
    }
    return o;
  }

}
