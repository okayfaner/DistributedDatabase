package edu.nyu.adb;

public class Transaction {

    private int transactionId;
    private int timeStamp;
    private String type;

    public Transaction(int transactionId, int timeStamp, String type) {
        this.transactionId = transactionId;
        this.timeStamp = timeStamp;
        this.type = type;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public int getTimeStamp() {
        return timeStamp;
    }

    public String getType() {
        return type;
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
