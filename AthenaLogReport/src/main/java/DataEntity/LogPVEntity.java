package DataEntity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogPVEntity {
    private String actionDT;
    private String actionMinu;
    private String appId;
    private String funcId;
    private String funcName;
    private String orgCode;
    private String orgName;
    private Long logPV;
    private Long logUV;
    public String getActionDT() {
        return actionDT;
    }

    public void setActionDT(String actionDT) {
        this.actionDT = actionDT;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getFuncId() {
        return funcId;
    }

    public void setFuncId(String funcId) {
        this.funcId = funcId;
    }

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public Long getLogPV() {
        return logPV;
    }

    public void setLogPV(Long logPV) {
        this.logPV = logPV;
    }

    public Long getLogUV() {
        return logUV;
    }

    public void setLogUV(Long logUV) {
        this.logUV = logUV;
    }

    public String getActionMinu() {
        return actionMinu;
    }

    public void setActionMinu(String actionMinu) {
        this.actionMinu = actionMinu;
    }
}
