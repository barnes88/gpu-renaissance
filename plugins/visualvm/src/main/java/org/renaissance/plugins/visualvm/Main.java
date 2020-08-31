package org.renaissance.plugins.visualvm;


import org.renaissance.Plugin;

public class Main implements Plugin,
    Plugin.AfterBenchmarkSetUpListener,
    Plugin.BeforeBenchmarkTearDownListener {

  public Main() {
  }

  @Override
  public void afterBenchmarkSetUp(String benchmark) {
    System.out.println("\nSTARTED VISUALVM PROFILER PLUGIN\nPress any key to continue");
    try {System.in.read();}
    catch (Exception e){}
  }

  @Override
  public void beforeBenchmarkTearDown(String benchmark) {
    System.out.println("\nSTOPPED VISUALVM PROFILER PLUGIN\nPress any key to continue");
    try {System.in.read();}
    catch (Exception e){}
  }
}
