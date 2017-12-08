package edu.nyu.adb;

import java.util.*;

/**
 * @author Xi Huang
 */
public class Variable {
  private int index;
  private int value;
  private TreeMap<Long, Integer> versions;
  private long lastCommitTime;

  /**
   * constructor for Variable
   * @param index index for the variable.
   * @param value last commit value for this variable.
   */
  public Variable(int index, int value) {
    this.index = index;
    this.value = value;
    this.lastCommitTime = System.nanoTime();
    this.versions = new TreeMap<>();
    versions.put(this.lastCommitTime, this.value);
  }

  /**
   * return its last commited value (latest value)
   * @return
   */
  public int getValue() {
    return this.value;
  }

  /**
   * set its value and add to its version map.
   * @param value
   */
  public void setValue(int value) {
    long temp = System.nanoTime();
    this.setLastCommitTime(temp);
    versions.put(temp, value);
    this.value = value;
  }

  /**
   * return index of this variable.
   * @return
   */
  public int getIndex() {
    return this.index;
  }

  /**
   * set last commit time.
   * @param t
   */
  public void setLastCommitTime(long t) {
    this.lastCommitTime = t;
  }

  /**
   * return the latest commited value before the give time.
   * @param t nano time for time stamp
   * @return
   */
  public int getVersionValue (long t) {
    return versions.lowerEntry(t).getValue();
  }

  public TreeMap<Long, Integer> getVersions() {
    TreeMap<Long, Integer> temp = this.versions;
    return temp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = 1;
    hash = prime * hash + index;
    hash = prime * hash + value;
    hash = prime * hash + (int)(lastCommitTime ^ lastCommitTime >>> 32);
    return hash;
  }
}
