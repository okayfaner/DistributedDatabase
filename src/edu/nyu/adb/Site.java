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

  }

  public List<Integer> getTransactions() {
    return new ArrayList<>(transTable.keySet());
  }

  public void removeTransactions(int transId) {
    transTable.remove(transId);
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

}
