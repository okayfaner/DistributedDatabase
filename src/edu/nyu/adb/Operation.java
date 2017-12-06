package edu.nyu.adb;

import java.util.Date;

public class Operation {

  private String type;
  private int variableIndex;
  private int value = 0;
  private Date timestamp;

  // For read operation
  public Operation(String type, int variableIndex, Date timestamp) {
    this.type = type;
    this.variableIndex = variableIndex;
    this.timestamp = timestamp;
  }

  // For write operation
  public Operation(String type, int variableIndex, int value, Date timestamp) {
    this.type = type;
    this.variableIndex = variableIndex;
    this.value = value;
    this.timestamp = timestamp;
  }

  public String getType() {
    return type;
  }

  public int getVariableIndex() {
    return variableIndex;
  }

  public int getValue() {
    return value;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  @Override
  public int hashCode() {
    int key = 31;
    int res = 1;
    res = key * res + ((this.type == null) ? 0 : this.type.hashCode());
    res = key * res + variableIndex;
    res = key * res + value;
    res = key * res + ((this.timestamp == null) ? 0 : this.timestamp.hashCode());
    return res;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Operation)) {
      return false;
    }
    else if (this == obj) {
      return true;
    }
    Operation other = (Operation) obj;
    if (this.type == null || other.type == null || !this.type.equals(other.type)) {
      return false;
    }
    if (this.variableIndex != other.variableIndex) {
      return false;
    }
    if (this.value != other.value) {
      return false;
    }
    if (this.timestamp == null || other.timestamp == null || this.timestamp.equals(other.timestamp)) {
      return false;
    }
    return true;
  }
}
