package com.rmzx.java;

/**
 * @Auther:changjiang
 * @Date:2020-05-09 19:47
 */
public class RiskLabel {
    private String name;
    private int num = 0;
    private String type;

    public RiskLabel() {
    }

    public RiskLabel(String name, int num, String type) {
        this.name = name;
        this.num = num;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void increase() {
        this.num += 1;
    }

}
