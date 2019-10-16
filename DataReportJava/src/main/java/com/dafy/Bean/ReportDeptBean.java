package com.dafy.Bean;

public class ReportDeptBean {

    private  long eventTime = 0;
    private  String deptCode="";
    private  String deptName = "";
    private  String busiAreaCode = "";
    private  String busiAreaName = "";
    private  String adminAreaCode = "";
    private  String adminAreaName = "";
    private  String fundcode = "";
    private  Integer lendCnt = 0;
    private  Integer lamount = 0;


    public ReportDeptBean(long eventTime, String deptCode, String deptName, String busiAreaCode, String busiAreaName, String adminAreaCode, String adminAreaName, String fundcode, Integer lendCnt, Integer lamount) {
        this.eventTime = eventTime;
        this.deptCode = deptCode;
        this.deptName = deptName;
        this.busiAreaCode = busiAreaCode;
        this.busiAreaName = busiAreaName;
        this.adminAreaCode = adminAreaCode;
        this.adminAreaName = adminAreaName;
        this.fundcode = fundcode;
        this.lendCnt = lendCnt;
        this.lamount = lamount;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getDeptCode() {
        return deptCode;
    }

    public void setDeptCode(String deptCode) {
        this.deptCode = deptCode;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public String getBusiAreaCode() {
        return busiAreaCode;
    }

    public void setBusiAreaCode(String busiAreaCode) {
        this.busiAreaCode = busiAreaCode;
    }

    public String getBusiAreaName() {
        return busiAreaName;
    }

    public void setBusiAreaName(String busiAreaName) {
        this.busiAreaName = busiAreaName;
    }

    public String getAdminAreaCode() {
        return adminAreaCode;
    }

    public void setAdminAreaCode(String adminAreaCode) {
        this.adminAreaCode = adminAreaCode;
    }

    public String getAdminAreaName() {
        return adminAreaName;
    }

    public void setAdminAreaName(String adminAreaName) {
        this.adminAreaName = adminAreaName;
    }

    public String getFundcode() {
        return fundcode;
    }

    public void setFundcode(String fundcode) {
        this.fundcode = fundcode;
    }

    public Integer getLendCnt() {
        return lendCnt;
    }

    public void setLendCnt(Integer lendCnt) {
        this.lendCnt = lendCnt;
    }

    public Integer getLamount() {
        return lamount;
    }

    public void setLamount(Integer lamount) {
        this.lamount = lamount;
    }
}
