package edu.nyu.adb;

/**
 * @author Oukan Fan
 */
public class Operation {

  public enum OpType {
    read,
    write
  }

  private OpType opType;// operation type
  private int transId;// id for transaction
  private int variableIndex;// variable index
  private int value = 0;// the value to write in this variable if it is write operation type
  private long timestamp;// time stamp for this operation

  // For read operation
  public Operation(OpType type, int variableIndex, long timestamp, int transId) {
    this.opType = type;
    this.variableIndex = variableIndex;
    this.timestamp = timestamp;
    this.transId = transId;
  }

  // For write operation
  public Operation(OpType type, int variableIndex, int value, long timestamp, int transId) {
    this.opType = type;
    this.variableIndex = variableIndex;
    this.value = value;
    this.timestamp = timestamp;
    this.transId = transId;
  }

  /**
   * return transID
   * @return
   */
  public int getTransId() {
    return this.transId;
  }

  /**
   * return operation type.
   * @return
   */
  public OpType getType() {
    return this.opType;
  }

  /**
   * return variable index
   * @return
   */
  public int getVariableIndex() {
    return variableIndex;
  }

  /**
   * return the write value.
   * @return
   */
  public int getValue() {
    return value;
  }

  /**
   * return time stamp
   * @return
   */
  public long getTimestamp() {
    return timestamp;
  }


  @Override
  public int hashCode() {
    int key = 31;
    int res = 1;
    res = key * res + ((this.opType == null) ? 0 : this.opType.hashCode());
    res = key * res + variableIndex;
    res = key * res + value;
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
    if (this.opType == null || other.opType == null || !this.opType.equals(other.opType)) {
      return false;
    }
    if (this.variableIndex != other.variableIndex) {
      return false;
    }
    if (this.value != other.value) {
      return false;
    }
    if (this.timestamp == other.timestamp) {
      return false;
    }
    return true;
  }
}
