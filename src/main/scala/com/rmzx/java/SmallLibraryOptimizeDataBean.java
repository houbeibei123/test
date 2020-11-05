package com.rmzx.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * 誉云小库Bean
 *
 * @author changjiang
 */
public class SmallLibraryOptimizeDataBean implements Serializable {

    //文章id
    private String doc_id;
    //文章类别1.网站2.论坛 3.博客 4.报刊 5.APP 6.广播 9.微信 10.视频 11.微博
    private int source_type;
    //标题
    private String title = null;
    //作者
    private String author = null;
    //内容摘要
    private String content_abstract = null;
    //关键词
    private String keyword = null;
    //正文
    private String content = null;
    //语义指纹
    private String fingerprint = null;
    //发布来源
    private String source_website = null;
    //版权来源
    private String capture_website = null;
    //原文url
    private String url = null;
    //发布时间
    private long publish_time;
    //站点id
    private long website_id = 0;
    //子站点id
    private long childsite_id = 0;
    //频道Id
    private long channel_id = 0;
    //是否境外
    private int is_foreign_media = 0;
    private String weibo_user_id = null;
    //微信biz
    private String biz = null;
    //头像地址
    private String user_head = null;
    //内容图片
    private String image_url = null;
    //版面名称
    private String layout_name = null;
    //地域属性
    private String area_attr = null;
    //正负面属性
    private String plus_attr = null;
    //文章字数
    private long content_length = 0;
    //入库时间date
    private long load_time;
    //采集时间
    private long crawler_time;
    //服务器Id
    private int service_id = 0;
    //网站分类
    private int sort_id = 0;
    //是否删除 0：未删除  1：已删除
    private int del_flag;
    //媒体权重
    private int site_weight = 0;
    //文档质量评分
    private double doc_weight;
    //媒体分类
    private int site_classify;
    //是否首发媒体
    private int first_publish;
    //文章内容正负面 1：正面    2：敏感
    private int content_is_negative;
    //是否垃圾文章 0：否    1：是
    private int garbage_classify;
    //新媒体一级分类
    private int media_primary_classify;
    //新媒体二级分类
    private int media_secondary_classify;
    //ICP互联网内容提供商
    private String[] internet_content_provider;
    //行业分类属性
    private String industry_category_attr = null;
    //北京市公安局1:正面 2:负面
    private String ext8 = null;
    //正面分值
    private double emothion_positive;
    //负面分值
    private double emothion_negative;
    //人物
    private String[] person;
    //地区
    private String[] area;
    //机构
    private String[] organization;
    //标题语义指纹
    private String title_fingerprint = null;
    //媒体权重
    private String meidia_weight = null;
    //主题地域
    private String[] topic_area_ids;
    //泛主题地域
    private String[] pan_topic_area_ids;
    private long root_weibo_id;
    //微博主性别
    private String gender = null;
    //微博主粉丝数
    private int followers_count;
    //博主地域
    private int province_id;
    private String root_weibo_pub_time;
    //发博总数
    private int statuses_count;
    private String root_weibo_url = null;
    //微博主认证
    private String user_vip = null;
    private String root_weibo_user_name = null;
    //原微博uid
    private String root_uid;
    //原微博用户uid
    private String root_user_id;
    // 是否原发
    private String original;
    // 转发数（微博）
    private int share_num;
    // 评论数（微博）
    private int common_num;
    //点赞数（微博）
    private int like_num;
    //阅读数（微信）
    private int read_num;
    //在看数（微信）
    private int watch_num;
    //行业id
    private String industry;


    //分类
    private String category;
    //文章标签
    private String tag;
    //垃圾细分类 1：标题为垃圾 2：内容为垃圾 3：网站为垃圾 4：内容不足100字符 5：图片数量大于8个  6：标题长度超过40字符 7：标题长度小于6字符 8：微信号为垃圾 9:主体或提及公司数量大于10
    //10:舆情数据屏蔽词库 11:指定信源屏蔽词
    private String garbage_fine_classify;
    //文章图片列表
    private String img_url_list;
    //一级行业
    private String industry_first;
    //二级行业
    private String industry_second;
    //相似指纹1
    private String similar_1;
    //相似指纹2
    private String similar_2;
    //相似指纹3
    private String similar_3;
    //风险标签
    private List<String> risk_label;
    //标题一级地域
    private List<String> title_area_first;
    //标题二级地域
    private List<String> title_area_second;
    //主体公司
    private List<String> company_core;
    //提及公司
    private List<DocumentCompany> company_ment;
    //情感分类 1:正面 2:敏感 0:中性
    private int emotion_classify;
    //文章图片数量
    private int img_count;
    //情感分
    private float emotion_score;

    //0非目标信息,1可能目标信息,2目标信息
    private String relevance;

    //媒体级别打标   0:其他 1:一级媒体 2：二级媒体
    private String media_level = "0";
    //行业信息-常规关键字打标   0：不符合关键词提取规则 1：符合关键词提取规则
    private String industry_keywords ="0";
    //行业信息-机构打标	 0：不符合关键词提取规则 1：符合关键词提取规则
    private String industry_body ="0";
    //舆情健康度打标  0:其他 1：危害1级 2：危害2级 3：危害3级
    private String health_level="0";
    //舆情健康度细分打标 0：其他 1：群体事件 2：经营发生严重问题 3：客户投诉 4：高管不当言行类 5：业务及经营问题 6：一般性投诉 7：技术性问题 8：巨额亏损 9：某一传播点受到质疑 10：受监管处罚 11：一般员工不当言行
    private String health_level_subsection="0";
    //0:不符合规则 1：符合规则
    private String knowledge_read="0";
    //0:不符合规则 1：符合规则
    private String policy_read="0";
    //0:不符合规则 1：符合规则
    private String time_boot="0";
    //0:不符合规则 1：符合规则
    private String social_responsibility="0";


    public String getDoc_id() {
        return doc_id;
    }

    public void setDoc_id(String doc_id) {
        this.doc_id = doc_id;
    }

    public int getSource_type() {
        return source_type;
    }

    public void setSource_type(int source_type) {
        this.source_type = source_type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getContent_abstract() {
        return content_abstract;
    }

    public void setContent_abstract(String content_abstract) {
        this.content_abstract = content_abstract;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getSource_website() {
        return source_website;
    }

    public void setSource_website(String source_website) {
        this.source_website = source_website;
    }

    public String getCapture_website() {
        return capture_website;
    }

    public void setCapture_website(String capture_website) {
        this.capture_website = capture_website;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getPublish_time() {
        return publish_time;
    }

    public void setPublish_time(long publish_time) {
        this.publish_time = publish_time;
    }

    public long getWebsite_id() {
        return website_id;
    }

    public void setWebsite_id(long website_id) {
        this.website_id = website_id;
    }

    public long getChildsite_id() {
        return childsite_id;
    }

    public void setChildsite_id(long childsite_id) {
        this.childsite_id = childsite_id;
    }

    public long getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(long channel_id) {
        this.channel_id = channel_id;
    }

    public int getIs_foreign_media() {
        return is_foreign_media;
    }

    public void setIs_foreign_media(int is_foreign_media) {
        this.is_foreign_media = is_foreign_media;
    }

    public String getWeibo_user_id() {
        return weibo_user_id;
    }

    public void setWeibo_user_id(String weibo_user_id) {
        this.weibo_user_id = weibo_user_id;
    }

    public String getBiz() {
        return biz;
    }

    public void setBiz(String biz) {
        this.biz = biz;
    }

    public String getUser_head() {
        return user_head;
    }

    public void setUser_head(String user_head) {
        this.user_head = user_head;
    }

    public String getImage_url() {
        return image_url;
    }

    public void setImage_url(String image_url) {
        this.image_url = image_url;
    }

    public String getLayout_name() {
        return layout_name;
    }

    public void setLayout_name(String layout_name) {
        this.layout_name = layout_name;
    }

    public String getArea_attr() {
        return area_attr;
    }

    public void setArea_attr(String area_attr) {
        this.area_attr = area_attr;
    }

    public String getPlus_attr() {
        return plus_attr;
    }

    public void setPlus_attr(String plus_attr) {
        this.plus_attr = plus_attr;
    }

    public long getContent_length() {
        return content_length;
    }

    public void setContent_length(long content_length) {
        this.content_length = content_length;
    }

    public long getLoad_time() {
        return load_time;
    }

    public void setLoad_time(long load_time) {
        this.load_time = load_time;
    }

    public long getCrawler_time() {
        return crawler_time;
    }

    public void setCrawler_time(long crawler_time) {
        this.crawler_time = crawler_time;
    }

    public int getService_id() {
        return service_id;
    }

    public void setService_id(int service_id) {
        this.service_id = service_id;
    }

    public int getSort_id() {
        return sort_id;
    }

    public void setSort_id(int sort_id) {
        this.sort_id = sort_id;
    }

    public int getDel_flag() {
        return del_flag;
    }

    public void setDel_flag(int del_flag) {
        this.del_flag = del_flag;
    }

    public int getSite_weight() {
        return site_weight;
    }

    public void setSite_weight(int site_weight) {
        this.site_weight = site_weight;
    }

    public double getDoc_weight() {
        return doc_weight;
    }

    public void setDoc_weight(double doc_weight) {
        this.doc_weight = doc_weight;
    }

    public int getSite_classify() {
        return site_classify;
    }

    public void setSite_classify(int site_classify) {
        this.site_classify = site_classify;
    }

    public int getFirst_publish() {
        return first_publish;
    }

    public void setFirst_publish(int first_publish) {
        this.first_publish = first_publish;
    }

    public int getContent_is_negative() {
        return content_is_negative;
    }

    public void setContent_is_negative(int content_is_negative) {
        this.content_is_negative = content_is_negative;
    }

    public int getGarbage_classify() {
        return garbage_classify;
    }

    public void setGarbage_classify(int garbage_classify) {
        this.garbage_classify = garbage_classify;
    }

    public int getMedia_primary_classify() {
        return media_primary_classify;
    }

    public void setMedia_primary_classify(int media_primary_classify) {
        this.media_primary_classify = media_primary_classify;
    }

    public int getMedia_secondary_classify() {
        return media_secondary_classify;
    }

    public void setMedia_secondary_classify(int media_secondary_classify) {
        this.media_secondary_classify = media_secondary_classify;
    }

    public String getIndustry_category_attr() {
        return industry_category_attr;
    }

    public void setIndustry_category_attr(String industry_category_attr) {
        this.industry_category_attr = industry_category_attr;
    }

    public String getExt8() {
        return ext8;
    }

    public void setExt8(String ext8) {
        this.ext8 = ext8;
    }

    public double getEmothion_positive() {
        return emothion_positive;
    }

    public void setEmothion_positive(double emothion_positive) {
        this.emothion_positive = emothion_positive;
    }

    public double getEmothion_negative() {
        return emothion_negative;
    }

    public void setEmothion_negative(double emothion_negative) {
        this.emothion_negative = emothion_negative;
    }

    public String[] getPerson() {
        return person;
    }

    public void setPerson(String[] person) {
        this.person = person;
    }

    public String[] getArea() {
        return area;
    }

    public void setArea(String[] area) {
        this.area = area;
    }

    public String[] getOrganization() {
        return organization;
    }

    public void setOrganization(String[] organization) {
        this.organization = organization;
    }

    public String getTitle_fingerprint() {
        return title_fingerprint;
    }

    public void setTitle_fingerprint(String title_fingerprint) {
        this.title_fingerprint = title_fingerprint;
    }

    public String getMeidia_weight() {
        return meidia_weight;
    }

    public void setMeidia_weight(String meidia_weight) {
        this.meidia_weight = meidia_weight;
    }

    public String[] getTopic_area_ids() {
        return topic_area_ids;
    }

    public void setTopic_area_ids(String[] topic_area_ids) {
        this.topic_area_ids = topic_area_ids;
    }

    public String[] getPan_topic_area_ids() {
        return pan_topic_area_ids;
    }

    public void setPan_topic_area_ids(String[] pan_topic_area_ids) {
        this.pan_topic_area_ids = pan_topic_area_ids;
    }

    public long getRoot_weibo_id() {
        return root_weibo_id;
    }

    public void setRoot_weibo_id(long root_weibo_id) {
        this.root_weibo_id = root_weibo_id;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getFollowers_count() {
        return followers_count;
    }

    public void setFollowers_count(int followers_count) {
        this.followers_count = followers_count;
    }

    public int getProvince_id() {
        return province_id;
    }

    public void setProvince_id(int province_id) {
        this.province_id = province_id;
    }

    public String getRoot_weibo_pub_time() {
        return root_weibo_pub_time;
    }

    public void setRoot_weibo_pub_time(String root_weibo_pub_time) {
        this.root_weibo_pub_time = root_weibo_pub_time;
    }

    public int getStatuses_count() {
        return statuses_count;
    }

    public void setStatuses_count(int statuses_count) {
        this.statuses_count = statuses_count;
    }

    public String getRoot_weibo_url() {
        return root_weibo_url;
    }

    public void setRoot_weibo_url(String root_weibo_url) {
        this.root_weibo_url = root_weibo_url;
    }

    public String getUser_vip() {
        return user_vip;
    }

    public void setUser_vip(String user_vip) {
        this.user_vip = user_vip;
    }

    public String getRoot_weibo_user_name() {
        return root_weibo_user_name;
    }

    public void setRoot_weibo_user_name(String root_weibo_user_name) {
        this.root_weibo_user_name = root_weibo_user_name;
    }

    public String getRoot_uid() {
        return root_uid;
    }

    public void setRoot_uid(String root_uid) {
        this.root_uid = root_uid;
    }

    public String getRoot_user_id() {
        return root_user_id;
    }

    public void setRoot_user_id(String root_user_id) {
        this.root_user_id = root_user_id;
    }

    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

    public int getShare_num() {
        return share_num;
    }

    public void setShare_num(int share_num) {
        this.share_num = share_num;
    }

    public int getCommon_num() {
        return common_num;
    }

    public void setCommon_num(int common_num) {
        this.common_num = common_num;
    }

    public int getLike_num() {
        return like_num;
    }

    public void setLike_num(int like_num) {
        this.like_num = like_num;
    }

    public int getRead_num() {
        return read_num;
    }

    public void setRead_num(int read_num) {
        this.read_num = read_num;
    }

    public int getWatch_num() {
        return watch_num;
    }

    public void setWatch_num(int watch_num) {
        this.watch_num = watch_num;
    }


    public String getIndustry() {
        return industry;
    }

    public void setIndustry(String industry) {
        this.industry = industry;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getGarbage_fine_classify() {
        return garbage_fine_classify;
    }

    public void setGarbage_fine_classify(String garbage_fine_classify) {
        this.garbage_fine_classify = garbage_fine_classify;
    }

    public String getImg_url_list() {
        return img_url_list;
    }

    public void setImg_url_list(String img_url_list) {
        this.img_url_list = img_url_list;
    }

    public String getIndustry_first() {
        return industry_first;
    }

    public void setIndustry_first(String industry_first) {
        this.industry_first = industry_first;
    }

    public String getIndustry_second() {
        return industry_second;
    }

    public void setIndustry_second(String industry_second) {
        this.industry_second = industry_second;
    }

    public String getSimilar_1() {
        return similar_1;
    }

    public void setSimilar_1(String similar_1) {
        this.similar_1 = similar_1;
    }

    public String getSimilar_2() {
        return similar_2;
    }

    public void setSimilar_2(String similar_2) {
        this.similar_2 = similar_2;
    }

    public String getSimilar_3() {
        return similar_3;
    }

    public void setSimilar_3(String similar_3) {
        this.similar_3 = similar_3;
    }

    public List<String> getRisk_label() {
        return risk_label;
    }

    public void setRisk_label(List<String> risk_label) {
        this.risk_label = risk_label;
    }

    public List<String> getTitle_area_first() {
        return title_area_first;
    }

    public void setTitle_area_first(List<String> title_area_first) {
        this.title_area_first = title_area_first;
    }

    public List<String> getTitle_area_second() {
        return title_area_second;
    }

    public void setTitle_area_second(List<String> title_area_second) {
        this.title_area_second = title_area_second;
    }

    public List<String> getCompany_core() {
        return company_core;
    }

    public void setCompany_core(List<String> company_core) {
        this.company_core = company_core;
    }

    public List<DocumentCompany> getCompany_ment() {
        return company_ment;
    }

    public void setCompany_ment(List<DocumentCompany> company_ment) {
        this.company_ment = company_ment;
    }

    public int getEmotion_classify() {
        return emotion_classify;
    }

    public void setEmotion_classify(int emotion_classify) {
        this.emotion_classify = emotion_classify;
    }

    public int getImg_count() {
        return img_count;
    }

    public void setImg_count(int img_count) {
        this.img_count = img_count;
    }

    public float getEmotion_score() {
        return emotion_score;
    }

    public void setEmotion_score(float emotion_score) {
        this.emotion_score = emotion_score;
    }

    public String getRelevance() {
        return relevance;
    }

    public void setRelevance(String relevance) {
        this.relevance = relevance;
    }

    public String getMedia_level() {
        return media_level;
    }

    public void setMedia_level(String media_level) {
        this.media_level = media_level;
    }

    public String getIndustry_keywords() {
        return industry_keywords;
    }

    public void setIndustry_keywords(String industry_keywords) {
        this.industry_keywords = industry_keywords;
    }

    public String getIndustry_body() {
        return industry_body;
    }

    public void setIndustry_body(String industry_body) {
        this.industry_body = industry_body;
    }

    public String getHealth_level() {
        return health_level;
    }

    public void setHealth_level(String health_level) {
        this.health_level = health_level;
    }

    public String getHealth_level_subsection() {
        return health_level_subsection;
    }

    public void setHealth_level_subsection(String health_level_subsection) {
        this.health_level_subsection = health_level_subsection;
    }

    public String getKnowledge_read() {
        return knowledge_read;
    }

    public void setKnowledge_read(String knowledge_read) {
        this.knowledge_read = knowledge_read;
    }

    public String getPolicy_read() {
        return policy_read;
    }

    public void setPolicy_read(String policy_read) {
        this.policy_read = policy_read;
    }

    public String getTime_boot() {
        return time_boot;
    }

    public void setTime_boot(String time_boot) {
        this.time_boot = time_boot;
    }

    public String getSocial_responsibility() {
        return social_responsibility;
    }

    public void setSocial_responsibility(String social_responsibility) {
        this.social_responsibility = social_responsibility;
    }

    public void caseBeanFromFkData(String json) {
        JSONObject jsonObject = JSON.parseObject(json);

        this.doc_id = jsonObject.getString("doc_id");
        this.source_type = jsonObject.getInteger("source_type");
        this.title = jsonObject.getString("title");
        this.author = jsonObject.getString("author");
        this.content_abstract = jsonObject.getString("content_abstract");
        this.keyword = jsonObject.getString("keyword");
        this.content = jsonObject.getString("content");
        this.source_website = jsonObject.getString("source_website");
        this.capture_website = jsonObject.getString("copyright_source");
        this.url = jsonObject.getString("url");
        this.publish_time = jsonObject.getLongValue("publish_time");
        this.website_id = jsonObject.getLongValue("website_id");
        this.childsite_id = jsonObject.getLongValue("childsite_id");
        this.channel_id = jsonObject.getLongValue("channel_id");
        this.is_foreign_media = jsonObject.getInteger("is_foreign_media");
        this.biz = jsonObject.getString("biz");
        this.content_length = jsonObject.getLongValue("content_length");
        this.load_time = System.currentTimeMillis();
        this.crawler_time = jsonObject.getLongValue("crawler_time");

        JSONArray icpArray = jsonObject.getJSONArray("icp_ids");
        if (icpArray != null && icpArray.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object obj : icpArray) {
                sb.append(obj).append(";");
            }
            this.internet_content_provider = StringUtils.removeEnd(sb.toString(), ";").split(";");
        }
        this.garbage_classify = jsonObject.getInteger("garbage_classify");
        String media_primary_classify_str = jsonObject.getString("media_primary_classify");
        if (StringUtils.isNotBlank(media_primary_classify_str)) {
            this.media_primary_classify = Integer.valueOf(media_primary_classify_str);
        }
        String media_secondary_classify_str = jsonObject.getString("media_secondary_classify");
        if (StringUtils.isNotBlank(media_secondary_classify_str)) {
            this.media_secondary_classify = Integer.valueOf(media_secondary_classify_str);
        }
        this.emothion_positive = jsonObject.getDouble("emothion_positive");
        this.emothion_negative = jsonObject.getDouble("emothion_negative");
        try {
            JSONArray personArray = jsonObject.getJSONArray("person");
            if (personArray != null && personArray.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Object obj : personArray) {
                    sb.append(obj).append(";");
                }
                this.person = StringUtils.removeEnd(sb.toString(), ";").split(";");
            }
        } catch (Exception e) {
            try {
                String personNames = jsonObject.getString("person");
                if (StringUtils.isNotBlank(personNames)) {
                    this.person = StringUtils.removeEnd(personNames, ";").split(";");
                }
            } catch (Exception e1) {
                throw e1;
            }
        }
        String areaStr = jsonObject.getString("area");
        if (StringUtils.isNotBlank(areaStr)) {
            this.area = areaStr.split(";");
        }
        String organizationStr = jsonObject.getString("organization");
        if (StringUtils.isNotBlank(organizationStr)) {
            this.organization = organizationStr.split(";");
        }

        JSONArray topicAreaidsArray = jsonObject.getJSONArray("topic_area_ids");
        if (topicAreaidsArray != null && topicAreaidsArray.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object obj : topicAreaidsArray) {
                sb.append(obj).append(";");
            }
            this.topic_area_ids = StringUtils.removeEnd(sb.toString(), ";").split(";");
        }
        JSONArray panTopicAreaidsArray = jsonObject.getJSONArray("pan_topic_area_ids");
        if (panTopicAreaidsArray != null && panTopicAreaidsArray.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object obj : panTopicAreaidsArray) {
                sb.append(obj).append(";");
            }
            this.pan_topic_area_ids = StringUtils.removeEnd(sb.toString(), ";").split(";");
        }


        this.category = jsonObject.getString("category");
        this.tag = jsonObject.getString("tag");
        this.garbage_fine_classify = jsonObject.getString("garbage_fine_classify");
        this.img_url_list = jsonObject.getString("img_url_list");
        this.industry_first = jsonObject.getString("industry_first");
        this.industry_second = jsonObject.getString("industry_second");
        this.similar_1 = jsonObject.getString("similar_1");
        this.similar_2 = jsonObject.getString("similar_2");
        this.similar_3 = jsonObject.getString("similar_3");

        JSONArray riskLabelArray = jsonObject.getJSONArray("risk_label");
        if (riskLabelArray != null && riskLabelArray.size() > 0) {
            Set<String> set = Sets.newHashSet();
            for (Object obj : riskLabelArray) {
                set.add((String) obj);
            }
            this.risk_label = Lists.newArrayList(set);
        }
        JSONArray titleAreaFirstArray = jsonObject.getJSONArray("title_area_first");
        if (titleAreaFirstArray != null && titleAreaFirstArray.size() > 0) {
            Set<String> set = Sets.newHashSet();
            for (Object obj : titleAreaFirstArray) {
                set.add((String) obj);
            }
            this.title_area_first = Lists.newArrayList(set);
        }
        JSONArray titleAreaSecondArray = jsonObject.getJSONArray("title_area_second");
        if (titleAreaSecondArray != null && titleAreaSecondArray.size() > 0) {
            Set<String> set = Sets.newHashSet();
            for (Object obj : titleAreaSecondArray) {
                set.add((String) obj);
            }
            this.title_area_second = Lists.newArrayList(set);
        }
        this.emotion_classify = jsonObject.getInteger("emotion_classify");
        this.img_count = jsonObject.getInteger("img_count");
        this.emotion_score = jsonObject.getFloat("emotion_score");

        JSONArray companyCoreArray = jsonObject.getJSONArray("company_core");
        if (companyCoreArray != null && companyCoreArray.size() > 0) {
            List<String> list = Lists.newArrayList();
            for (int i = 0; i < companyCoreArray.size(); i++) {
                JSONObject jo = companyCoreArray.getJSONObject(i);
                String name = jo.getString("name");
                int num = jo.getInteger("num");
                double score = jo.getDouble("score");
                JSONArray riskArray = jo.getJSONArray("riskLabel");
                JSONArray frontArray = jo.getJSONArray("frontLabel");
                List<RiskLabel> riskLabels = Lists.newArrayList();
                List<RiskLabel> frontLabels = Lists.newArrayList();
                if (riskArray != null && riskArray.size() > 0) {
                    for (int j = 0; j < riskArray.size(); j++) {
                        JSONObject rjo = riskArray.getJSONObject(j);
                        String name1 = rjo.getString("name");
                        int num1 = rjo.getInteger("num");
                        String type = rjo.getString("type");
                        riskLabels.add(new RiskLabel(name1, num1, type));
                    }
                }
                if (frontArray != null && frontArray.size() > 0) {
                    for (int j = 0; j < frontArray.size(); j++) {
                        JSONObject rjo = frontArray.getJSONObject(j);
                        String name1 = rjo.getString("name");
                        int num1 = rjo.getInteger("num");
                        String type = rjo.getString("type");
                        frontLabels.add(new RiskLabel(name1, num1, type));
                    }
                }
                DocumentCompany documentCompany = new DocumentCompany();
                documentCompany.setName(name);
                documentCompany.setNum(num);
                documentCompany.setScore(score);
                documentCompany.setRiskLabel(riskLabels);
                documentCompany.setFrontLabel(frontLabels);
                String s = JSON.toJSONString(documentCompany);
                list.add(s);
            }
            this.company_core = list;
        }

        JSONArray companyMentArray = jsonObject.getJSONArray("company_ment");
        if (companyMentArray != null && companyMentArray.size() > 0) {
            List<DocumentCompany> list = Lists.newArrayList();
            for (int i = 0; i < companyMentArray.size(); i++) {
                JSONObject jo = companyMentArray.getJSONObject(i);
                String name = jo.getString("name");
                int num = jo.getInteger("num");
                double score = jo.getDouble("score");
                JSONArray riskArray = jo.getJSONArray("riskLabel");
                JSONArray frontArray = jo.getJSONArray("frontLabel");
                List<RiskLabel> riskLabels = Lists.newArrayList();
                List<RiskLabel> frontLabels = Lists.newArrayList();
                if (riskArray != null && riskArray.size() > 0) {
                    for (int j = 0; j < riskArray.size(); j++) {
                        JSONObject rjo = riskArray.getJSONObject(j);
                        String name1 = rjo.getString("name");
                        int num1 = rjo.getInteger("num");
                        String type = rjo.getString("type");
                        riskLabels.add(new RiskLabel(name1, num1, type));
                    }
                }
                if (frontArray != null && frontArray.size() > 0) {
                    for (int j = 0; j < frontArray.size(); j++) {
                        JSONObject rjo = frontArray.getJSONObject(j);
                        String name1 = rjo.getString("name");
                        int num1 = rjo.getInteger("num");
                        String type = rjo.getString("type");
                        frontLabels.add(new RiskLabel(name1, num1, type));
                    }
                }
                DocumentCompany documentCompany = new DocumentCompany();
                documentCompany.setName(name);
                documentCompany.setNum(num);
                documentCompany.setScore(score);
                documentCompany.setRiskLabel(riskLabels);
                documentCompany.setFrontLabel(frontLabels);
                list.add(documentCompany);
            }
            this.company_ment = list;
        }
    }

    public void caseBeanFromWeiboData(String json) {
        JSONObject jsonObject = JSON.parseObject(json);
        this.doc_id = jsonObject.getString("doc_id");
        this.source_type = jsonObject.getInteger("source_type");
        this.title = jsonObject.getString("title");
        this.author = jsonObject.getString("author");
        this.content_abstract = jsonObject.getString("content_abstract");
        this.keyword = jsonObject.getString("keyword");
        this.content = jsonObject.getString("content");
        this.fingerprint = jsonObject.getString("fingerprint");
        this.source_website = jsonObject.getString("source_website");
        this.capture_website = jsonObject.getString("capture_website");
        this.url = jsonObject.getString("url");
        this.publish_time = jsonObject.getLong("publish_time");
        this.website_id = jsonObject.getLong("website_id");
        this.childsite_id = jsonObject.getLong("childsite_id");
        this.channel_id = jsonObject.getLong("channel_id");
        this.is_foreign_media = jsonObject.getInteger("is_foreign_media");
        this.weibo_user_id = jsonObject.getString("weibo_user_id");
        this.user_head = jsonObject.getString("user_head");
        this.image_url = jsonObject.getString("image_url");
        this.area_attr = jsonObject.getString("area_attr");
        this.plus_attr = jsonObject.getString("plus_attr");
        this.content_length = jsonObject.getLong("content_length");
        this.load_time = System.currentTimeMillis();
        this.crawler_time = jsonObject.getLong("crawler_time");
        this.service_id = jsonObject.getInteger("service_id");
        this.sort_id = jsonObject.getInteger("sort_id");
        this.del_flag = jsonObject.getInteger("del_flag");
        this.site_weight = jsonObject.getInteger("site_weight");
        this.doc_weight = jsonObject.getInteger("doc_weight");
        this.site_classify = jsonObject.getInteger("site_classify");
        this.first_publish = jsonObject.getInteger("first_publish");
        this.content_is_negative = jsonObject.getInteger("content_is_negative");
        this.garbage_classify = jsonObject.getInteger("garbage_classify");
        this.media_primary_classify = jsonObject.getInteger("media_primary_classify");
        this.media_secondary_classify = jsonObject.getInteger("media_secondary_classify");
        JSONArray icpArray = jsonObject.getJSONArray("internet_content_provider");
        if (icpArray != null && icpArray.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Object obj : icpArray) {
                sb.append(obj).append(";");
            }
            this.internet_content_provider = StringUtils.removeEnd(sb.toString(), ";").split(";");
        }
        this.industry_category_attr = jsonObject.getString("industry_category_attr");
        this.ext8 = jsonObject.getString("ext8");
        this.emothion_positive = jsonObject.getDouble("emothion_positive");
        this.emothion_negative = jsonObject.getDouble("emothion_negative");
        this.title_fingerprint = jsonObject.getString("title_fingerprint");
        this.meidia_weight = jsonObject.getString("meidia_weight");
        this.root_weibo_id = jsonObject.getLong("root_weibo_id");
        this.gender = jsonObject.getString("gender");
        this.followers_count = jsonObject.getInteger("followers_count");
        this.province_id = jsonObject.getInteger("province_id");
        this.root_weibo_pub_time = jsonObject.getString("root_weibo_pub_time");
        this.statuses_count = jsonObject.getInteger("statuses_count");
        this.root_weibo_url = jsonObject.getString("root_weibo_url");
        this.user_vip = jsonObject.getString("user_vip");
        this.root_weibo_user_name = jsonObject.getString("root_weibo_user_name");
        this.root_uid = jsonObject.getString("root_uid");
        this.root_user_id = jsonObject.getString("root_user_id");
        this.original = jsonObject.getString("original");

        this.share_num = jsonObject.getInteger("share_num") != null ? jsonObject.getInteger("share_num") : 0;
        this.common_num = jsonObject.getInteger("common_num") != null ? jsonObject.getInteger("common_num") : 0;
        this.like_num = jsonObject.getInteger("like_num") != null ? jsonObject.getInteger("like_num") : 0;
        this.read_num = jsonObject.getInteger("read_num") != null ? jsonObject.getInteger("read_num") : 0;
        this.watch_num = jsonObject.getInteger("watch_num") != null ? jsonObject.getInteger("watch_num") : 0;
        this.industry = jsonObject.getString("industry");
    }
}
