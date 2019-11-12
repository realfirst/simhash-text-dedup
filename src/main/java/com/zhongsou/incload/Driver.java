package com.zhongsou.incload;

import org.apache.hadoop.util.ToolRunner;

public class Driver {
  public static void main(String[] args) throws Exception {
    int i = 0;
    for (String arg : args) {
      System.out.println("arg " + i + arg);
      i++;
    }

    int dedupExitCode = ToolRunner.run(new DeDup(), args);
    if (dedupExitCode == 0) {
      System.exit(ToolRunner.run(new SelectLogic(), args));
    } else {
      System.err.println("dedup failure!");
    }

  }

}
