package edu.nyu.adb;

import java.util.ArrayList;
import java.util.List;

public class Transaction {
    public static enum TranType {
      RO,
      RW
    }

    private int transactionId;
    private int timeStamp;
    private TranType tranType;
    private List<Operation> operations;

    public Transaction(int transactionId, int timeStamp, TranType tranType) {
        this.transactionId = transactionId;
        this.timeStamp = timeStamp;
        this.tranType = tranType;
        this.operations = new ArrayList<>();
    }

    public int getTransactionId() {
        return this.transactionId;
    }

    public int getTimeStamp() {
        return this.timeStamp;
    }

    public TranType getType() {
        return this.tranType;
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
