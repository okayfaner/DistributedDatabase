package edu.nyu.adb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Transaction {
  public static enum TranType {
    RO,
    RW
  }

  private int transactionId;
  private Date timeStamp;
  private TranType tranType;
  private List<Operation> operations;

  public Transaction(int transactionId, Date timeStamp, TranType tranType) {
      this.transactionId = transactionId;
      this.timeStamp = timeStamp;
      this.tranType = tranType;
      this.operations = new ArrayList<>();
  }

  public int getTransactionId() {
      return this.transactionId;
  }

  public Date getTimeStamp() {
      return this.timeStamp;
  }

  public TranType getType() {
      return this.tranType;
  }

  public void addOperations(Operation operation) {
    this.operations.add(operation);
  }

  public void removeAllOperations() {
    this.operations = new ArrayList<>();
  }

  public List<Operation> getOperations() {
    return this.operations;
  }

  @Override
  public int hashCode() {
      int prime = 31;
      int res = 1;
      res = prime * res + transactionId;
      return res;
  }

  @Override
  public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Transaction)) {
          return false;
      }
      else if (this == obj) {
          return true;
      }
      Transaction other = (Transaction) obj;
      if (transactionId != other.transactionId) {
          return false;
      }
      return true;
  }
}
