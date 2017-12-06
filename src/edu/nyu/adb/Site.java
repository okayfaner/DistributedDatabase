package edu.nyu.adb;

import java.util.HashSet;
import java.util.Date;

public class Site {

    private boolean siteStatus;
    private int siteIndex;
    private HashSet<Variable> variableSet;

    public Site(int siteIndex) {
        this.siteStatus = true;
        this.siteIndex = siteIndex;
        this.variableSet = new HashSet<>();
        for (int i = 1; i <= 20; i++) {
            if (i % 2 == 0 || siteIndex == 1 + i % 10) {
                variableSet.add(new Variable(i, 10 * i));
            }
        }
    }

    public boolean getSiteStatus() {
        return this.siteStatus;
    }

    public void setSiteStatus(boolean siteStatus) {
        this.siteStatus = siteStatus;
    }

    public int getSiteIndex() {
        return this.siteIndex;
    }

    public void setSiteIndex(int siteIndex) {
        this.siteIndex = siteIndex;
    }

    public void setVarValue(Variable variable, int newValue) {
        variable.setValue(newValue);
    }

}
