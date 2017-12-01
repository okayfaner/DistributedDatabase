package edu.nyu.adb;

import java.util.*;

public class Variable {
  private int index;
  private int value;
  private TreeMap<Date, Integer> versions;
  private Date lastCommitTime;
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
    this.lastCommitTime = new Date();
    this.versions = new TreeMap<>();
    versions.put(this.lastCommitTime, this.value);
  }

  public int getValue() {
    return this.value;
  }

  public void setValue(int value) {
    Date temp = new Date();
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

  public Date getLastCommitTime() {
    return this.lastCommitTime;
  }

  public void setLastCommitTime(Date t) {
    this.lastCommitTime = t;
  }

}
