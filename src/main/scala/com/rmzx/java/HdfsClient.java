package com.rmzx.java;

import com.google.common.collect.Maps;
import com.hankcs.hanlp.collection.AhoCorasick.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;

public class HdfsClient implements Serializable {


    //核心媒体信源（media_source）的hdfs路径
    private String media_source_path;
    //信息关键词（information_keywords）的hdfs路径
    private String information_keywords_path;
    //舆情健康度关键词（health_level_keywords）的hdfs路径
    private String health_level_keywords_path;

    //一级媒体站点关键词
    private AhoCorasickDoubleArrayTrie<String> media_source_first;
    //二级媒体站点关键词
    private AhoCorasickDoubleArrayTrie<String> media_source_second;

    //常规关键词
    private AhoCorasickDoubleArrayTrie<String> generalKeywords;
    //机构关键词
    private AhoCorasickDoubleArrayTrie<String> organizationKeywords;
    //事件引导
    private AhoCorasickDoubleArrayTrie<String> eventGuideKeywords;
    //社会责任
    private AhoCorasickDoubleArrayTrie<String> socialKeywords;

    //群体事件
    private AhoCorasickDoubleArrayTrie<String> groupEventKeywords;
    //高管/董事长/首席  高管关键词
    private AhoCorasickDoubleArrayTrie<String> positionKeywords;
    //经营发生严重问题模块下的高管危害关键词
    private AhoCorasickDoubleArrayTrie<String> jobHarmKeywords;
    //经营发生严重问题 关键词
    private AhoCorasickDoubleArrayTrie<String> ecomomicProblemKeywords;
    //客户投诉
    private AhoCorasickDoubleArrayTrie<String> customerComplaintKeywords;
    //高管不当言行类下的高管危害关键词
    private AhoCorasickDoubleArrayTrie<String> wrongWordsKeywords;
    //业务及经营问题
    private AhoCorasickDoubleArrayTrie<String> managementProblemKeywords;
    //一般性投诉
    private AhoCorasickDoubleArrayTrie<String> generalQuestionKeywords;
    //技术性问题
    private AhoCorasickDoubleArrayTrie<String> technicalQuestionKeywords;
    //巨额亏损
    private AhoCorasickDoubleArrayTrie<String> hugeLossKeywords;
    //某一传播点受到质疑
    private AhoCorasickDoubleArrayTrie<String> doubtKeywords;
    //受监管处罚
    private AhoCorasickDoubleArrayTrie<String> supervisoryPunishmentKeywords;
    //一般员工不当言行
    private AhoCorasickDoubleArrayTrie<String> employeesKeywords;

    public HdfsClient(String media_source_path, String information_keywords_path, String health_level_keywords_path) {
        this.media_source_path = media_source_path;
        this.information_keywords_path = information_keywords_path;
        this.health_level_keywords_path = health_level_keywords_path;
    }

    public void buildKeywords() throws IOException {
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://10.28.1.1:9000"), new org.apache.hadoop.conf.Configuration());
            inputStream = fileSystem.open(new Path(media_source_path));
            if (inputStream == null) {
                throw new RuntimeException("核心媒体信源文件加载失败!!!");
            } else {
                Workbook wb = new XSSFWorkbook(inputStream);
                Sheet sheet = wb.getSheetAt(0);
                int firstRowIndex = sheet.getFirstRowNum() + 1;

                //一级媒体站点
                Row rowMediaFirst = sheet.getRow(firstRowIndex);
                List<String> media_first_list = new ArrayList<String>();
                if (rowMediaFirst != null) {
                    String cellvalue = rowMediaFirst.getCell(1).getStringCellValue();
                    media_first_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> media_first_map = Maps.newTreeMap();
                for (String str : media_first_list) {
                    media_first_map.put(str, str);
                }
                media_source_first = new AhoCorasickDoubleArrayTrie<>();
                media_source_first.build((TreeMap<String, String>) media_first_map);

                //二级媒体站点
                Row rowMediaSecond = sheet.getRow(firstRowIndex + 1);
                List<String> media_second_list = new ArrayList<String>();
                if (rowMediaSecond != null) {
                    String cellvalue = rowMediaSecond.getCell(1).getStringCellValue();
                    media_second_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> media_second_map = Maps.newTreeMap();
                for (String str : media_second_list) {
                    media_second_map.put(str, str);
                }
                media_source_second = new AhoCorasickDoubleArrayTrie<>();
                media_source_second.build((TreeMap<String, String>) media_second_map);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally {
//            inputStream.close();
//            fileSystem.close();
//        }
    }



    public void buildKeywords2() throws IOException {
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://10.28.1.1:9000"), new org.apache.hadoop.conf.Configuration());
            inputStream = fileSystem.open(new Path(information_keywords_path));
            if (inputStream == null) {
                throw new RuntimeException("信息关键字文件加载失败!!!");
            } else {
                Workbook wb = new XSSFWorkbook(inputStream);
                Sheet sheet = wb.getSheetAt(0);
                int firstRowIndex = sheet.getFirstRowNum() + 1;

                //常规关键字
                Row rowFirst = sheet.getRow(firstRowIndex);
                List<String> row_first_list = new ArrayList<String>();
                if (rowFirst != null) {
                    String cellvalue = rowFirst.getCell(1).getStringCellValue();
                    row_first_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_first_map = Maps.newTreeMap();
                for (String str : row_first_list) {
                    row_first_map.put(str, str);
                }
                generalKeywords = new AhoCorasickDoubleArrayTrie<>();
                generalKeywords.build((TreeMap<String, String>) row_first_map);

                //机构
                Row rowSecond = sheet.getRow(firstRowIndex + 1);
                List<String> row_second_list = new ArrayList<String>();
                if (rowSecond != null) {
                    String cellvalue = rowSecond.getCell(1).getStringCellValue();
                    row_second_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_second_map = Maps.newTreeMap();
                for (String str : row_second_list) {
                    row_second_map.put(str, str);
                }
                organizationKeywords = new AhoCorasickDoubleArrayTrie<>();
                organizationKeywords.build((TreeMap<String, String>) row_second_map);

                //时间引导
                Row rowThree = sheet.getRow(firstRowIndex + 2);
                List<String> row_three_list = new ArrayList<String>();
                if (rowThree != null) {
                    String cellvalue = rowThree.getCell(1).getStringCellValue();
                    row_three_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_three_map = Maps.newTreeMap();
                for (String str : row_three_list) {
                    row_three_map.put(str, str);
                }
                eventGuideKeywords = new AhoCorasickDoubleArrayTrie<>();
                eventGuideKeywords.build((TreeMap<String, String>) row_three_map);

                //社会责任
                Row rowFour = sheet.getRow(firstRowIndex + 3);
                List<String> row_four_list = new ArrayList<String>();
                if (rowFour != null) {
                    String cellvalue = rowFour.getCell(1).getStringCellValue();
                    row_four_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_four_map = Maps.newTreeMap();
                for (String str : row_four_list) {
                    row_four_map.put(str, str);
                }
                socialKeywords = new AhoCorasickDoubleArrayTrie<>();
                socialKeywords.build((TreeMap<String, String>) row_four_map);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally {
//            inputStream.close();
//            fileSystem.close();
//        }
    }


    public void buildKeywords3() throws IOException {
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://10.28.1.1:9000"), new org.apache.hadoop.conf.Configuration());
            inputStream = fileSystem.open(new Path(health_level_keywords_path));
            if (inputStream == null) {
                throw new RuntimeException("舆情健康度文件加载失败!!!");
            } else {
                Workbook wb = new XSSFWorkbook(inputStream);
                Sheet sheet = wb.getSheetAt(0);
                int firstRowIndex = sheet.getFirstRowNum() + 1;

                //群体事件
                Row rowFirst = sheet.getRow(firstRowIndex);
                List<String> row_first_list = new ArrayList<String>();
                if (rowFirst != null) {
                    String cellvalue = rowFirst.getCell(2).getStringCellValue();
                    row_first_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_first_map = Maps.newTreeMap();
                for (String str : row_first_list) {
                    row_first_map.put(str, str);
                }
                groupEventKeywords = new AhoCorasickDoubleArrayTrie<>();
                groupEventKeywords.build((TreeMap<String, String>) row_first_map);

                // //高管/董事长/首席  高管关键词
                Row rowSecond = sheet.getRow(firstRowIndex + 1);
                List<String> row_second_position_list = new ArrayList<String>();
                List<String> row_second_jobHarm_list = new ArrayList<String>();
                List<String> row_second_ecomomicProblem_list = new ArrayList<String>();
                if (rowSecond != null) {
                    String cellvalue = rowSecond.getCell(2).getStringCellValue();
                    row_second_position_list = Arrays.asList(cellvalue.split("@")[0].split("#"));
                    row_second_jobHarm_list = Arrays.asList(cellvalue.split("@")[1].split("#"));
                    row_second_ecomomicProblem_list = Arrays.asList(cellvalue.split("@")[2].split("#"));
                }

                Map<String, String> row_second_positon_map = Maps.newTreeMap();
                for (String str : row_second_position_list) {
                    row_second_positon_map.put(str, str);
                }
                positionKeywords = new AhoCorasickDoubleArrayTrie<>();
                positionKeywords.build((TreeMap<String, String>) row_second_positon_map);

                //经营发生严重问题模块下的高管危害关键词
                Map<String, String> row_second_jobHarm_map = Maps.newTreeMap();
                for (String str : row_second_jobHarm_list) {
                    row_second_jobHarm_map.put(str, str);
                }
                jobHarmKeywords = new AhoCorasickDoubleArrayTrie<>();
                jobHarmKeywords.build((TreeMap<String, String>) row_second_jobHarm_map);

                //经营发生严重问题 关键词
                Map<String, String> row_second_ecomomicProblem_map = Maps.newTreeMap();
                for (String str : row_second_ecomomicProblem_list) {
                    row_second_ecomomicProblem_map.put(str, str);
                }
                ecomomicProblemKeywords = new AhoCorasickDoubleArrayTrie<>();
                ecomomicProblemKeywords.build((TreeMap<String, String>) row_second_ecomomicProblem_map);


                //客户投诉
                Row rowThree = sheet.getRow(firstRowIndex + 2);
                List<String> row_three_list = new ArrayList<String>();
                if (rowThree != null) {
                    String cellvalue = rowThree.getCell(2).getStringCellValue();
                    row_three_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_three_map = Maps.newTreeMap();
                for (String str : row_three_list) {
                    row_three_map.put(str, str);
                }
                customerComplaintKeywords = new AhoCorasickDoubleArrayTrie<>();
                customerComplaintKeywords.build((TreeMap<String, String>) row_three_map);

                //高管不当言行类
                Row rowFour = sheet.getRow(firstRowIndex + 3);
                List<String> row_four_list = new ArrayList<String>();
                if (rowFour != null) {
                    String cellvalue = rowFour.getCell(2).getStringCellValue();
                    row_four_list = Arrays.asList(cellvalue.split("@")[1].split("#"));
                }

                Map<String, String> row_four_map = Maps.newTreeMap();
                for (String str : row_four_list) {
                    row_four_map.put(str, str);
                }
                wrongWordsKeywords = new AhoCorasickDoubleArrayTrie<>();
                wrongWordsKeywords.build((TreeMap<String, String>) row_four_map);

                //业务及经营问题
                Row rowFive = sheet.getRow(firstRowIndex + 4);
                List<String> row_five_list = new ArrayList<String>();
                if (rowFive != null) {
                    String cellvalue = rowFive.getCell(2).getStringCellValue();
                    row_five_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_five_map = Maps.newTreeMap();
                for (String str : row_five_list) {
                    row_five_map.put(str, str);
                }
                managementProblemKeywords = new AhoCorasickDoubleArrayTrie<>();
                managementProblemKeywords.build((TreeMap<String, String>) row_five_map);

                //一般性投诉
                Row rowSix = sheet.getRow(firstRowIndex + 5);
                List<String> row_six_list = new ArrayList<String>();
                if (rowSix != null) {
                    String cellvalue = rowSix.getCell(2).getStringCellValue();
                    row_six_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_six_map = Maps.newTreeMap();
                for (String str : row_six_list) {
                    row_six_map.put(str, str);
                }
                generalQuestionKeywords = new AhoCorasickDoubleArrayTrie<>();
                generalQuestionKeywords.build((TreeMap<String, String>) row_six_map);

                //技术性问题
                Row rowSeven = sheet.getRow(firstRowIndex + 6);
                List<String> row_seven_list = new ArrayList<String>();
                if (rowSeven != null) {
                    String cellvalue = rowSeven.getCell(2).getStringCellValue();
                    row_seven_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_seven_map = Maps.newTreeMap();
                for (String str : row_seven_list) {
                    row_seven_map.put(str, str);
                }
                technicalQuestionKeywords = new AhoCorasickDoubleArrayTrie<>();
                technicalQuestionKeywords.build((TreeMap<String, String>) row_seven_map);

                //巨额亏损
                Row rowEight = sheet.getRow(firstRowIndex + 7);
                List<String> row_eight_list = new ArrayList<String>();
                if (rowEight != null) {
                    String cellvalue = rowEight.getCell(2).getStringCellValue();
                    row_eight_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_eight_map = Maps.newTreeMap();
                for (String str : row_eight_list) {
                    row_eight_map.put(str, str);
                }
                hugeLossKeywords = new AhoCorasickDoubleArrayTrie<>();
                hugeLossKeywords.build((TreeMap<String, String>) row_eight_map);

                //某一传播点受到质疑
                Row rowNine = sheet.getRow(firstRowIndex + 8);
                List<String> row_nine_list = new ArrayList<String>();
                if (rowNine != null) {
                    String cellvalue = rowNine.getCell(2).getStringCellValue();
                    row_nine_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_nine_map = Maps.newTreeMap();
                for (String str : row_nine_list) {
                    row_nine_map.put(str, str);
                }
                doubtKeywords = new AhoCorasickDoubleArrayTrie<>();
                doubtKeywords.build((TreeMap<String, String>) row_nine_map);

                //受监管处罚
                Row rowTen = sheet.getRow(firstRowIndex + 9);
                List<String> row_ten_list = new ArrayList<String>();
                if (rowTen != null) {
                    String cellvalue = rowTen.getCell(2).getStringCellValue();
                    row_ten_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_ten_map = Maps.newTreeMap();
                for (String str : row_ten_list) {
                    row_ten_map.put(str, str);
                }
                supervisoryPunishmentKeywords = new AhoCorasickDoubleArrayTrie<>();
                supervisoryPunishmentKeywords.build((TreeMap<String, String>) row_ten_map);

                //一般员工不当言行
                Row rowEleven = sheet.getRow(firstRowIndex + 10);
                List<String> row_eleven_list = new ArrayList<String>();
                if (rowEleven != null) {
                    String cellvalue = rowEleven.getCell(2).getStringCellValue();
                    row_eleven_list = Arrays.asList(cellvalue.split("#"));
                }

                Map<String, String> row_eleven_map = Maps.newTreeMap();
                for (String str : row_eleven_list) {
                    row_eleven_map.put(str, str);
                }
                employeesKeywords = new AhoCorasickDoubleArrayTrie<>();
                employeesKeywords.build((TreeMap<String, String>) row_eleven_map);


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            inputStream.close();
            fileSystem.close();
        }
    }

    public String getMedia_source_path() {
        return media_source_path;
    }

    public void setMedia_source_path(String media_source_path) {
        this.media_source_path = media_source_path;
    }

    public String getInformation_keywords_path() {
        return information_keywords_path;
    }

    public void setInformation_keywords_path(String information_keywords_path) {
        this.information_keywords_path = information_keywords_path;
    }

    public String getHealth_level_keywords_path() {
        return health_level_keywords_path;
    }

    public void setHealth_level_keywords_path(String health_level_keywords_path) {
        this.health_level_keywords_path = health_level_keywords_path;
    }

    public AhoCorasickDoubleArrayTrie<String> getMedia_source_first() {
        return media_source_first;
    }

    public void setMedia_source_first(AhoCorasickDoubleArrayTrie<String> media_source_first) {
        this.media_source_first = media_source_first;
    }

    public AhoCorasickDoubleArrayTrie<String> getMedia_source_second() {
        return media_source_second;
    }

    public void setMedia_source_second(AhoCorasickDoubleArrayTrie<String> media_source_second) {
        this.media_source_second = media_source_second;
    }

    public AhoCorasickDoubleArrayTrie<String> getGeneralKeywords() {
        return generalKeywords;
    }

    public void setGeneralKeywords(AhoCorasickDoubleArrayTrie<String> generalKeywords) {
        this.generalKeywords = generalKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getOrganizationKeywords() {
        return organizationKeywords;
    }

    public void setOrganizationKeywords(AhoCorasickDoubleArrayTrie<String> organizationKeywords) {
        this.organizationKeywords = organizationKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getEventGuideKeywords() {
        return eventGuideKeywords;
    }

    public void setEventGuideKeywords(AhoCorasickDoubleArrayTrie<String> eventGuideKeywords) {
        this.eventGuideKeywords = eventGuideKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getSocialKeywords() {
        return socialKeywords;
    }

    public void setSocialKeywords(AhoCorasickDoubleArrayTrie<String> socialKeywords) {
        this.socialKeywords = socialKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getGroupEventKeywords() {
        return groupEventKeywords;
    }

    public void setGroupEventKeywords(AhoCorasickDoubleArrayTrie<String> groupEventKeywords) {
        this.groupEventKeywords = groupEventKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getPositionKeywords() {
        return positionKeywords;
    }

    public void setPositionKeywords(AhoCorasickDoubleArrayTrie<String> positionKeywords) {
        this.positionKeywords = positionKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getJobHarmKeywords() {
        return jobHarmKeywords;
    }

    public void setJobHarmKeywords(AhoCorasickDoubleArrayTrie<String> jobHarmKeywords) {
        this.jobHarmKeywords = jobHarmKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getEcomomicProblemKeywords() {
        return ecomomicProblemKeywords;
    }

    public void setEcomomicProblemKeywords(AhoCorasickDoubleArrayTrie<String> ecomomicProblemKeywords) {
        this.ecomomicProblemKeywords = ecomomicProblemKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getCustomerComplaintKeywords() {
        return customerComplaintKeywords;
    }

    public void setCustomerComplaintKeywords(AhoCorasickDoubleArrayTrie<String> customerComplaintKeywords) {
        this.customerComplaintKeywords = customerComplaintKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getWrongWordsKeywords() {
        return wrongWordsKeywords;
    }

    public void setWrongWordsKeywords(AhoCorasickDoubleArrayTrie<String> wrongWordsKeywords) {
        this.wrongWordsKeywords = wrongWordsKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getManagementProblemKeywords() {
        return managementProblemKeywords;
    }

    public void setManagementProblemKeywords(AhoCorasickDoubleArrayTrie<String> managementProblemKeywords) {
        this.managementProblemKeywords = managementProblemKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getGeneralQuestionKeywords() {
        return generalQuestionKeywords;
    }

    public void setGeneralQuestionKeywords(AhoCorasickDoubleArrayTrie<String> generalQuestionKeywords) {
        this.generalQuestionKeywords = generalQuestionKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getTechnicalQuestionKeywords() {
        return technicalQuestionKeywords;
    }

    public void setTechnicalQuestionKeywords(AhoCorasickDoubleArrayTrie<String> technicalQuestionKeywords) {
        this.technicalQuestionKeywords = technicalQuestionKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getHugeLossKeywords() {
        return hugeLossKeywords;
    }

    public void setHugeLossKeywords(AhoCorasickDoubleArrayTrie<String> hugeLossKeywords) {
        this.hugeLossKeywords = hugeLossKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getDoubtKeywords() {
        return doubtKeywords;
    }

    public void setDoubtKeywords(AhoCorasickDoubleArrayTrie<String> doubtKeywords) {
        this.doubtKeywords = doubtKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getSupervisoryPunishmentKeywords() {
        return supervisoryPunishmentKeywords;
    }

    public void setSupervisoryPunishmentKeywords(AhoCorasickDoubleArrayTrie<String> supervisoryPunishmentKeywords) {
        this.supervisoryPunishmentKeywords = supervisoryPunishmentKeywords;
    }

    public AhoCorasickDoubleArrayTrie<String> getEmployeesKeywords() {
        return employeesKeywords;
    }

    public void setEmployeesKeywords(AhoCorasickDoubleArrayTrie<String> employeesKeywords) {
        this.employeesKeywords = employeesKeywords;
    }
}
