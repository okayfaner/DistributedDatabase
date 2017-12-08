package edu.nyu.adb;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Oukan Fan
 */
public class Transaction {
  public static enum TranType {
    RO,
    RW
  }

  private int transactionId;// id for this transaction
  private long timeStamp;// time stamp
  private TranType tranType;// transaction type
  private List<Operation> operations;// list of operation.

  /**
   * constructor
   * @param transactionId id for this transaction
   * @param timeStamp beginning time stamp for this transaction
   * @param tranType transaction type.
   */
  public Transaction(int transactionId, long timeStamp, TranType tranType) {
      this.transactionId = transactionId;
      this.timeStamp = timeStamp;
      this.tranType = tranType;
      this.operations = new ArrayList<>();
  }

  /**
   * return transaction id.
   * @return
   */
  public int getTransactionId() {
      return this.transactionId;
  }

  /**
   * return time stamp
   * @return
   */
  public long getTimeStamp() {
      return this.timeStamp;
  }

  /**
   * return transaction type.
   * @return
   */
  public TranType getType() {
      return this.tranType;
  }

  /**
   * add operation to the operation list.
   * @param operation
   */
  public void addOperations(Operation operation) {
    this.operations.add(operation);
  }

  /**
   * remove all operations in this transaction.
   */
  public void removeAllOperations() {
    this.operations = new ArrayList<>();
  }

  /**
   * return the operations list.
   * @return
   */
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
