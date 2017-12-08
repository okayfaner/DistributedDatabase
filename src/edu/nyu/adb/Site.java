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

  public int getVerVarValue(int index, long date) {
    return variableTable.get(index).getVersionValue(date);
  }

  // true add successfully, false not
  public boolean addLock(int varIndex, Lock lock) {

    if (siteStatus == SiteStatus.FAIL) {
      return false;
    } else if (siteStatus == siteStatus.RECOVERY) {
      if (lock.getType() == Lock.lockType.READ && (varIndex % 2 == 0)) {
        return false;
      }
    }

    List<Lock> locksOnVar = lockTable.get(varIndex);
    if(lock.getType() == Lock.lockType.READ) {
      if (locksOnVar.size() == 0) {
        locksOnVar.add(lock);
        lockTable.put(varIndex, locksOnVar);
        return true;
      } else {
        boolean flag = false;
        for (Lock temp : locksOnVar) {
          if (temp.getType() == Lock.lockType.WRITE) {
            flag = true;
            break;
          }
        }
        if (flag) {
          return false;
        } else {
          locksOnVar.add(lock);
          lockTable.put(varIndex, locksOnVar);
          return true;
        }
      }
    } else {
      // write lock
      if (locksOnVar.size() > 1) {
        return false;
      } else if (locksOnVar.size() == 1) {
        if (locksOnVar.get(0).getTranscId() == lock.getTranscId()) {
          locksOnVar.add(lock);
          lockTable.put(varIndex, locksOnVar);
          return true;
        } else {
          return false;
        }
      } else {
        locksOnVar.add(lock);
        lockTable.put(varIndex, locksOnVar);
        return true;
      }
    }
  }

  public List<Lock> getLockTable(int varIndex) {
    return lockTable.get(varIndex);
  }

  public void dropLock(int varIndex, int tranId) {
    List<Lock> lockList = lockTable.get(varIndex);

    for (int i = 0; i < lockList.size(); i ++) {
      if (lockList.get(i).getTranscId() == tranId) {
        lockList.remove(i);
        break;
      }
    }

    lockTable.put(varIndex, lockList);
  }

  public List<Integer> getTransactions() {
    return new ArrayList<>(transTable.keySet());
  }

  public boolean removeTransactions(int transId) {
    if (transTable.containsKey(transId)) {
      transTable.remove(transId);
      List<Integer> list;
      for(Integer varId : lockTable.keySet()) {
        list = new ArrayList<>();
        for (int i = 0; i < lockTable.get(varId).size(); i ++) {
          if (lockTable.get(varId).get(i).getTranscId() == transId) {
            list.add(i);
          }
        }

        List<Lock> temp = lockTable.get(varId);

        for(Integer j : list) {
          temp.remove((int)j);
        }
        lockTable.put(varId,temp);
      }

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
        System.out.println("Variable x" + temp.getIndex() + " : " + temp.getValue());
      }
    }
  }

  // dump variable
  public void dump(int index) {
    Variable temp = variableTable.get(index);
    System.out.println("Variable x" + temp.getIndex() + " : " + temp.getValue());
  }

  // return commit success or not.
  public boolean commit(Operation operation, Transaction trans) {

    // check lock first.
    int varIndex = operation.getVariableIndex();


    if (trans.getType() == Transaction.TranType.RO) {
      if (operation.getType() == Operation.OpType.read) {
        // unreplic var
        if ((varIndex % 2) == 1){
          if (siteStatus == SiteStatus.FAIL) {
            // means can't read
            return false;
          }

          Variable variable = variableTable.get(varIndex);
          System.out.println("Variable x" + variable.getIndex() +
                  " from T" + trans.getTransactionId() + " READ value " + variable.getVersionValue(operation.getTimestamp()) + " at site" + siteIndex);

          return true;
        } else {
          if (siteStatus == SiteStatus.NORMAL) {
            Variable variable = variableTable.get(varIndex);
            System.out.println("Variable x" + variable.getIndex() +
                    " from T" + trans.getTransactionId() + " READ value " + variable.getVersionValue(operation.getTimestamp()) + " at site" + siteIndex);

            return true;
          }

          return false;
        }
      }
    } else {
      List<Lock> lockList = lockTable.get(varIndex);
      if (operation.getType() == Operation.OpType.read) {
        // check lock first

        boolean flag = false;

        for (Lock temp : lockList) {
          if (temp.getType() == Lock.lockType.WRITE && temp.getTranscId() != trans.getTransactionId()) {
            flag = true;
            break;
          }
        }

        if (flag) {
          return false;
        }

        if ((varIndex % 2) == 1){
          if (siteStatus == SiteStatus.FAIL) {
            // means can't read
            return false;
          }

          Variable variable = variableTable.get(varIndex);
          System.out.println("Variable x" + variable.getIndex() +
                  " from T" + trans.getTransactionId() + " READ value " + variable.getValue() + " at site" + siteIndex);
          return true;
        } else {
          if (siteStatus == SiteStatus.NORMAL) {
            Variable variable = variableTable.get(varIndex);
            System.out.println("Variable x" + variable.getIndex() +
                    " from T" + trans.getTransactionId() + " READ value " + variable.getValue() + " at site" + siteIndex);
            return true;
          }

          return false;
        }
      } else {
        // write
        boolean flag = true;

        for (Lock temp : lockList) {
          if (temp.getTranscId() == trans.getTransactionId() && temp.getType() == Lock.lockType.WRITE) {
            flag = false;
            break;
          }
        }

        if (flag) {
          return false;
        }

        if (siteStatus == SiteStatus.FAIL) {
          return false;
        } else if (siteStatus == SiteStatus.RECOVERY) {
          Variable var = variableTable.get(varIndex);
          var.setValue(operation.getValue());
          this.siteStatus = SiteStatus.NORMAL;
          System.out.println("Variable x" + var.getIndex() +
                  " from T" + trans.getTransactionId() + " WRITE value " + var.getValue() + " at site" + siteIndex);
          System.out.println("\nSite " + this.siteIndex + " become NORMAL due to T" + trans.getTransactionId() + " WRITE x"
                  + varIndex + " with value " + var.getValue());
          variableTable.put(varIndex, var);
          dropLock(varIndex, trans.getTransactionId());
          return true;
        } else {
          Variable var = variableTable.get(varIndex);
          var.setValue(operation.getValue());
          System.out.println("Variable x" + var.getIndex() +
                  " from T" + trans.getTransactionId() + " WRITE value " + var.getValue() + " at site" + siteIndex);
          variableTable.put(varIndex, var);
          dropLock(varIndex, trans.getTransactionId());
          return true;
        }
      }
    }
    return false;
  }
}
