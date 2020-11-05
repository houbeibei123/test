package com.rmzx.java;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author changjiang
 * @Date:2020-05-09 19:45
 */
public class DocumentCompany {
    private String name;
    private int num;
    private double score;
    private List<RiskLabel> riskLabel;
    private List<RiskLabel> frontLabel;

    public DocumentCompany() {
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

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public List<RiskLabel> getRiskLabel() {
        return riskLabel;
    }

    public void setRiskLabel(List<RiskLabel> riskLabel) {
        this.riskLabel = riskLabel;
    }

    public List<RiskLabel> getFrontLabel() {
        return frontLabel;
    }

    public void setFrontLabel(List<RiskLabel> frontLabel) {
        this.frontLabel = frontLabel;
    }

    public void increase() {
        this.num += 1;
    }

    public void plusScore(double score) {
        this.score += score;
    }

    public void addRiskLabel(List<RiskLabel> riskLabel) {
        if (this.riskLabel == null) {
            this.riskLabel = riskLabel;
        } else {
            // merge(this.riskLabel, riskLabel);
            this.riskLabel = mergeLabel(this.riskLabel,riskLabel);
        }
    }

    public void addFrontLabel(List<RiskLabel> frontLabel) {
        if (this.frontLabel == null) {
            this.frontLabel = frontLabel;
        } else {
            // merge(this.frontLabel,frontLabel);
            this.frontLabel = mergeLabel(this.frontLabel,frontLabel);
        }
    }

    private void merge(List<RiskLabel> labels,List<RiskLabel> otherLabels) {
        for (RiskLabel otherLabel : otherLabels) {
            boolean contains = false;
            for (RiskLabel label : labels) {
                if (label.getName().equals(otherLabel.getName())
                        && label.getType().equals(otherLabel.getType())) {
                    contains = true;
                    label.setNum(label.getNum()+otherLabel.getNum());
                }
            }
            if(!contains) {
                labels.add(otherLabel);
            }
        }
    }

    private List<RiskLabel> mergeLabel(List<RiskLabel> labels,List<RiskLabel> otherLabels) {
        List<RiskLabel> newLabels = Lists.newArrayList();
        for (RiskLabel otherLabel : otherLabels) {
            boolean contains = false;
            for (RiskLabel label : labels) {
                if (label.getName().equals(otherLabel.getName())
                        && label.getType().equals(otherLabel.getType())) {
                    contains = true;
                    label.setNum(label.getNum()+otherLabel.getNum());
                    newLabels.add(label);
                    break;
                }
            }
            if(!contains) {
                newLabels.add(otherLabel);
            }
        }

        return newLabels;
    }
}
