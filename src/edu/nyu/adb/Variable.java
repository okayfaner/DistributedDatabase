package edu.nyu.adb;

import java.util.*;

public class Variable {
  private int index;
  private int value;
  private TreeMap<Long, Integer> versions;
  private long lastCommitTime;
  private boolean accessibleForRead;

  /**
   *
   * @param index index for the variable.
   * @param value last commit value for this variable.
   */
  public Variable(int index, int value) {
    this.index = index;
    this.value = value;
    this.accessibleForRead = true;
    this.lastCommitTime = System.nanoTime();
    this.versions = new TreeMap<>();
    versions.put(this.lastCommitTime, this.value);
  }

  public int getValue() {
    return this.value;
  }

  public void setValue(int value) {
    long temp = System.nanoTime();
    this.setLastCommitTime(temp);
    versions.put(temp, value);
    this.value = value;
  }

  public int getIndex() {
    return this.index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public boolean isAccessibleForRead() {
    return this.accessibleForRead;
  }

  public void setAccessibleForRead(boolean t) {
    this.accessibleForRead = t;
  }

  public long getLastCommitTime() {
    return this.lastCommitTime;
  }

  public void setLastCommitTime(long t) {
    this.lastCommitTime = t;
  }

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
    hash = prime * hash + (accessibleForRead ? 0 : 1);
    return hash;
  }
}
