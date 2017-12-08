package edu.nyu.adb;

import java.util.*;

/**
 * @author Xi Huang
 */
public class Site {

  public enum SiteStatus {
    NORMAL,
    FAIL,
    RECOVERY
  }

  private SiteStatus siteStatus;
  private int siteIndex;// id for site.
  private Map<Integer, Variable> variableTable;// <varId, variable>
  private Map<Integer, List<Lock>> lockTable;// <varId, list of locks>
  private Map<Integer, Queue<Operation>> transTable; // <transaction Id, queue for not commited operation in this site>

  /**
   * constructor for site. it will initialize all the tables in this site and give the variable its initial value.
   * @param siteIndex the id for this site.
   */
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

  /**
   * return site status.
   * @return
   */
  public SiteStatus getSiteStatus() {
    return this.siteStatus;
  }

  /**
   * try to add lock for this variable with this given lock.
   * @param varIndex
   * @param lock lock you want to add on this variable in this site.
   * @return true, if added this lock on this variable.
   *         false, if can't add lock.
   */
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

  /**
   * return the current lock table on given variable.
   * @param varIndex id for variable
   * @return the current lock table on given variable.
   */
  public List<Lock> getLockTable(int varIndex) {
    return lockTable.get(varIndex);
  }

  /**
   * drop the lock, which give transaction held on this variable.
   * @param varIndex id for variable.
   * @param tranId id for transaction
   */
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

  /**
   * remove the transaction in this transTable in this site
   * and remove all locks this transaction held in this site.
   * @param transId
   * @return remove it or not.
   */
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

  /**
   * add this operation in related transaction
   * @param transId
   * @param operation
   */
  public void addOperation(int transId, Operation operation) {
    Queue<Operation> queue = transTable.getOrDefault(transId, new LinkedList<Operation>());
    queue.offer(operation);
    transTable.put(transId, queue);
  }

  /**
   * fail this site and drop all the lock table and mark its status to fail.
   * @return
   */
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

  /**
   * revor this site and mark its status to recovery.
   */
  public void recover() {
    this.siteStatus = SiteStatus.RECOVERY;
  }

  /**
   * dump this site, it will print all variable Id and its last commited value(lates value).
   */
  public void dump() {
    for (int i = 1; i <= 20; i++) {
      if (i % 2 == 0 || siteIndex == 1 + i % 10) {
        Variable temp = variableTable.get(i);
        System.out.println("Variable x" + temp.getIndex() + " : " + temp.getValue());
      }
    }
  }

  /**
   * dump this variable, it will print this variable's id and last commited value.
   * @param index
   */
  public void dump(int index) {
    Variable temp = variableTable.get(index);
    System.out.println("Variable x" + temp.getIndex() + " : " + temp.getValue());
  }

  /**
   * try to commit given operation in this transaction.
   * First, this method will check transaction type is RO or RW,
   * in RO, just need to check the status of this site, which determine success or not.
   * in RW, it will check the lock status for this operation, if this transaction have the needed lock on this site for
   * this varaible, and the site status is right, then this operation can be commited.
   * or this operation can not be commited.
   * @param operation
   * @param trans
   * @return true, this operation commited;
   *         false, this operation can not be commited.
   */
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
