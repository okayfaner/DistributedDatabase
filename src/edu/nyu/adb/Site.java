import java.util.HashSet;
import java.util.Date;

public class SiteManager {

  private boolean siteStatus;
  private int siteIndex;
  private HashSet<Variable> variableSet;


  public Site(int siteIndex) {
    this.siteStatus = true;
    this.siteIndex = siteIndex;
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

  public void addVersion(Variable variable, int newValue) {
    Date currentTime = new Date();
    variable.getVersions.put(currentTime, newValue);
    variable.setLastCommitTime(currentTime);
  }

  
}
