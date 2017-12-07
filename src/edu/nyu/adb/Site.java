package edu.nyu.adb;

import java.util.*;


public class Site {

  public enum SiteStatus {
    NORMAL,
    FAIL,
    RECOVERY
  }

  private SiteStatus siteStatus;
  private int siteIndex;
  private Map<Integer, Variable> variableTable;//varId,
  private Map<Integer, List<Lock>> lockTable;//varId
  private Map<Integer, Queue<Operation>> transTable; //tansId,

  public Site(int siteIndex) {
    this.siteStatus = SiteStatus.NORMAL;
    this.siteIndex = siteIndex;
    this.lockTable = new HashMap<>();
    this.variableTable = new HashMap<>();
    this.transTable = new HashMap<>();
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0 || siteIndex == 1 + i % 10) {
        variableTable.put(i, new Variable(i, 10 * i));
        lockTable.put(i, new ArrayList<>());
      }

    }
  }

  public SiteStatus getSiteStatus() {
    return this.siteStatus;
  }

  public void setSiteStatus(SiteStatus siteStatus) {
    this.siteStatus = siteStatus;
  }

  public int getSiteIndex() {
    return this.siteIndex;
  }


  public void setVarValue(Variable variable, int newValue) {
      variable.setValue(newValue);
  }

  public int getCurVarValue(int index) {
    return variableTable.get(index).getValue();
  }

  public int getVerVarValue(int index, Date date) {
    return variableTable.get(index).getVersionValue(date);
  }

  public void addLock(int varIndex, Lock lock) {
    List<Lock> temp = lockTable.get(varIndex);
    temp.add(lock);
    lockTable.put(varIndex, temp);
  }

  public void dropLock(int varIndex, int tranId) {
    // TODO
  }

  public List<Integer> getTransactions() {
    return new ArrayList<>(transTable.keySet());
  }

  public boolean removeTransactions(int transId) {
    if (transTable.containsKey(transId)) {
      transTable.remove(transId);
      return true;
    }

    return false;
  }

  public void addOperation(int transId, Operation operation) {
    Queue<Operation> queue = transTable.getOrDefault(transId, new LinkedList<Operation>());
    queue.offer(operation);
    transTable.put(transId, queue);
  }

  public List<Integer> fail() {
    this.siteStatus = SiteStatus.FAIL;

    // drop all locks in this site.
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0 || siteIndex == 1 + i % 10) {
        lockTable.put(i, new ArrayList<>());
      }
    }

    List<Integer> listTrans = new ArrayList<>(this.transTable.keySet());
    // drop all trans in this site.
    this.transTable = new HashMap<>();
    return listTrans;
  }

  public void recover() {
    this.siteStatus = SiteStatus.RECOVERY;
  }

  // dump site
  public void dump() {
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0 || siteIndex == 1 + i % 10) {
        Variable temp = variableTable.get(i);
        System.out.println("Variable X" + temp.getIndex() + " : " + temp.getValue());
      }
    }
  }

  // dump variable
  public void dump(int index) {
    Variable temp = variableTable.get(index);
    System.out.println("Variable X" + temp.getIndex() + " : " + temp.getValue());
  }

  // return commit success or not.
  public boolean commit(Operation operation, Transaction trans) {
    if (trans.getType() == Transaction.TranType.RO) {
      if (operation.getType() == Operation.OpType.read) {
        int varIndex = operation.getVariableIndex();

        // unreplic var
        if ((varIndex % 2) == 1){
          if (siteStatus == SiteStatus.FAIL) {
            // means can't read
            return false;
          }

          Variable variable = variableTable.get(varIndex);
          System.out.println("Variable x" + variable.getIndex() +
                  " from T" + trans.getTransactionId() + " has value " + variable.getVersionValue(operation.getTimestamp()));

          return true;
        } else {
          if (siteStatus == SiteStatus.NORMAL) {
            Variable variable = variableTable.get(varIndex);
            System.out.println("Variable x" + variable.getIndex() +
                    " from T" + trans.getTransactionId() + " has value " + variable.getVersionValue(operation.getTimestamp()));

            return true;
          }

          return false;
        }
      }
    }

    return true;
  }
}
