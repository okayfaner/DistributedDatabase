package edu.nyu.adb;

import java.util.List;

public class Transaction {
  public static enum TranType {
    RO,
    RW
  }

  private int tranId;
  private int timestamp;
  private TranType tranType;
  private List<Operation> operations;

  public Transaction(int tranId, int timestamp, TranType tranType) {

  }
}
