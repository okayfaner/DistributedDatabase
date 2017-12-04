package edu.nyu.adb;

public class Lock {
  public static enum lockType {
    READ,
    WRITE
  }

  private lockType type;
  private int varIndex;
  private int transcId;

  public Lock(int varIndex, int transcId, lockType type) {
    this.varIndex = varIndex;
    this.transcId = transcId;
    this.type = type;
  }

  public int getVarIndex() {
    return this.varIndex;
  }

  public lockType getType() {
    return this.type;
  }

  public int getTranscId() {
    return this.transcId;
  }

  public void setType(lockType type) {
    this.type = type;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = 1;
    hash = prime * hash + this.transcId;
    hash = prime * hash + this.varIndex;
    hash = prime * hash + ((this.type == null) ? 0 : this.type.hashCode());
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if(!(obj instanceof Lock)) {
      return false;
    }
    Lock other = (Lock) obj;
    if (this.transcId != other.transcId) {
      return false;
    }
    if (this.varIndex != other.varIndex) {
      return false;
    }
    if (this.type != other.type) {
      return false;
    }
    return true;
  }
}
