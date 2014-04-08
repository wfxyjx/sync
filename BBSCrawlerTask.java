package crawler.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import bbs.inter.BBSBlock;
import bbs.inter.BBSMiner;
import bbs.inter.BBSSplitter;
import bbs.inter.PageDataChecker;
import bbs.inter.TopicDataParser;
import bbs.inter.TopicListParser;
import bbs.inter.UserParser;
import bbs.utils.BBSParseException;
import bbs.utils.BBSReply;
import bbs.utils.BBSTopic;
import bbs.utils.BBSUser;
import bbs.utils.PageData;
import bbs.utils.PageRequest;
import bbs.utils.PrepareException;
import bbs.utils.TopicDataContext;

/**
 * 用于爬行BBS数据，一般而言是一个爬行一个分块
 * 
 * @author ponder21
 *
 */
public class BBSCrawlerTask extends CrawlerTask {
	// 爬行引擎
	private BBSMiner mMiner;

	// 分块描述符，当前的爬虫任务块
	private String mBlockDesc;
	
	// 上下文信息
	private CrawlerContext mContext;
	
	// 当前要处理的块数据
	private BBSBlock mBlock;
	
	// 各种常用的爬虫部件
	
	// 分块工具，用于解释块信息
	private BBSSplitter mSpliter;
	// 帖子列表解析器
	private TopicListParser mListParser;
	// 帖子数据解析器
	private TopicDataParser mDataParser;
	// 页面数据校验，判断是否是404、维护信息页面 
	private PageDataChecker mChecker;
	// 用户信息处理器
	private UserParser mUserParser;
	
	@Override
	public boolean configure(String descriptor, CrawlerContext context) {
		mContext = context;
		
		String[] seps = descriptor.split("\t");
		if (seps.length < 2) {
			return false;
		}

		try {
			mMiner = (BBSMiner) MinerLoader.getInstance().createMiner("bbs." + seps[0]);
			
			// 检测是否存在存在bbs控制器
			if (null == mMiner) {
				mContext.error("BBSCrawlerTask.configure", 
						"createMiner返回了null，可能是Miner未载入或者未定义", 
						"MinerId", "bbs." + seps[0]);
			}
		} catch (Exception e) {
			mContext.errorWithException(
					"BBSCrawlerTask.configure", "createMiner抛出异常", e,
					"MinerId", "bbs." + seps[0]);
			return false;
		}
		
		mBlockDesc = descriptor.substring(seps[0].length() + 1);
		
		//获取必要的解析器
		mSpliter = mMiner.getSplitter();
		mDataParser = mMiner.getTopicDataParser();
		mChecker = mMiner.getPageChecker();
		mListParser = mMiner.getTopicListParser();
		mUserParser = mMiner.getUserParser();
		
		return true;
	}
	
	/**
	 * 执行分块下载
	 */
	@Override
	public void execute() {
		
		// 准备阶段，执行登录等操作
		PageRequest request = null;
		PageData pageData = null;
		try {
			while (null != (request = mMiner.prepare(pageData))) {
				mContext.getPageDownloader().execute(request);
				
				if (!mChecker.check(request.getResponse())) {
					mContext.error("BBSCrawlerTask.execute", 
							"prepare时的请求返回结果未通过校验", 
							"MinerId", mMiner.getBBSName(),
							"PageUrl", request.getUrl());
					return ;
				}
				
				pageData = request.getResponse();
			}
		} catch (PrepareException e) {
			if (request == null)
				request = PageRequest.getHttpGet("");
			
			mContext.errorWithException("BBSCrawlerTask.execute", "prepare时出现了PrepareException", e, 
					"MinerId", mMiner.getBBSName(),
					"PageUrl", request.getUrl());
			return ;
		} catch (Exception e) {
			if (request == null)
				request = PageRequest.getHttpGet("");
			
			mContext.errorWithException("BBSCrawlerTask.execute", "prepare时出现了未知异常", e, 
					"MinerId", mMiner.getBBSName(),
					"PageUrl", request.getUrl());
			return ;
		}
		
		// 解析分块
		mBlock = mSpliter.fromDescriptor(mBlockDesc);
		
		if (mBlock == null) {
			mContext.error("BBSCrawlerTask.execute", "mSpliter.fromDescriptor返回块解析失败",
					"MinerId", mMiner.getBBSName(),
					"mBlockDesc", mBlockDesc);
			return ;
		}
		
		// 帖子列表
		List<BBSTopic> allTopics = new ArrayList<BBSTopic>();
		
		// 读取分块中所有帖子列表
		pageData = null;	// 初始化为nul获取第一页url
		
		String url;
		// 依次读取block中的每一页，获取帖子列表信息
		while (mBlock.hasNext(pageData)) {
			url = mBlock.nextPage(pageData);
			
			if (mContext.isVerbose()) {
				System.out.println("读取列表：" + url);
			}

			// 获取帖子列表数据
			pageData = getPageData(url);
			if (pageData == null) {
				mContext.error("BBSCrawlerTask.execute", "下载帖子列表页面出错，可能是未通过校验或者超时，请查询log日志", 
						"MinerId", mMiner.getBBSName(),
						"PageUrl", url);
				break ;
			}
			
			// 解析当前页中的帖子列表
			List<BBSTopic> topics = null;
			try {
				topics = mListParser.parse(pageData);
				
				if (topics != null)
					allTopics.addAll(topics);
			} catch (BBSParseException e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "解析帖子列表时出现BBSParseException", e,
							"MinerId", mMiner.getBBSName(),
							"PageUrl", url);
				break ;
			} catch (Exception e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "解析帖子列表时出现未知异常", e,
						"MinerId", mMiner.getBBSName(),
						"PageUrl", url);
				break ;
			}
		}
		
		if (mContext.isVerbose())
			System.out.println("读取帖子列表完成，共得到：" + allTopics.size() + "个帖子");
		
		Set<String> related_users = new HashSet<String>();
		
		// 下载帖子信息并存储每个帖子
		for (BBSTopic topic : allTopics) {
			if (mContext.isVerbose())
				System.out.println("读取帖子：" +  topic.title() + " " + topic.url());
			
			// 下载并解析帖子数据
			try {
				boolean get_topic_ok;
				
				if (!mDataParser.canGetAllLinksOnce()) {
					get_topic_ok = getTopicPageByPage(topic);
				} else {
					get_topic_ok = getTopic(topic);
				}
				
				if (!get_topic_ok)
					continue;
					
			} catch (BBSParseException e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "解析帖子链接或创建上下文时出现异常", e,
						"MinerId", mMiner.getBBSName(),
						"PageUrl", topic.url(),
						"TopicTitle", topic.title());
				continue;
			} catch (Exception e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "解析帖子链接或创建上下文出现未知异常", e,
						"MinerId", mMiner.getBBSName(),
						"PageUrl", topic.url(),
						"TopicTitle", topic.title());
				
				continue ;
			}
			
			// 构造存储请求并执行
			try {
				mContext.getStorageExecutor().execute(makeTopicStorageRequest(topic));
			} catch (Exception e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "存储帖子数据时出现未知异常", e,
						"MinerId", mMiner.getBBSName(),
						"PageUrl", topic.url(),
						"TopicTitle", topic.title());
				continue ;
			}
			
			// 收集所有的用户
			related_users.addAll(topic.relatedUsers());
			
			topic.contents().clear();
		}
		
		// 将引用消除，以免在长期的下载中保持内存不释放。
		allTopics.clear();
		allTopics = null;
		
		System.out.println("All users count:" + related_users.size());
		
		// 下载所有的用户信息
		for (String user : related_users) {
			if (mContext.isVerbose()) {
				System.out.println("下载用户信息：" + user);
			}
			
			String user_info_url = mUserParser.formUserInfoUrl(user);
			
			if (mContext.isVerbose()) {
				System.out.println("用户信息链接为：" + user_info_url);
			}
			
			// 下载用户信息页面
			pageData = getPageData(user_info_url);
			if (pageData == null) {
				mContext.error("BBSCrawlerTask.execute", "下载用户信息页面出错，可能是未通过校验或者超时，请查询log日志", 
						"MinerId", mMiner.getBBSName(),
						"PageUrl", user_info_url);
				continue ;
			}
			
			try {
				BBSUser userInfo = mUserParser.parse(pageData);
				
				// 构造存储请求并执行
				try {
					mContext.getStorageExecutor().execute(makeUserStorageRequest(userInfo));
				} catch (Exception e) {
					mContext.errorWithException("BBSCrawlerTask.execute", "存储用户数据时出现未知异常", e,
							"MinerId", mMiner.getBBSName(),
							"user", userInfo.useId());
					e.printStackTrace();
				}
				
			} catch (BBSParseException e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "解析用户信息时抛出了异常", e, 
						"MinerName", mMiner.getBBSName(), 
						"用户ID", user, 
						"url", user_info_url);
				
				continue;
			} catch (Exception e) {
				mContext.errorWithException("BBSCrawlerTask.execute", "解析并存储用户信息时抛出了异常", e,
						"MinerName", mMiner.getBBSName(),
						"用户ID:", user, 
						"url:", user_info_url);
				continue;
			}
		}
	}
	
	/**
	 * 下载整个帖子，一次性可以获取所有页的方式
	 * @throws BBSParseException 
	 */
	private boolean getTopic(BBSTopic topic) throws BBSParseException {
		// 首先获取第一页数据
		PageData pData = getPageData(topic.url());
		if (pData == null)
			return false;
		
		// 存放各页的链接
		List<String> linkStrings;
		// 是否跳过首页下载。
		boolean skipDownload = true;
				
		// 一次性获取所有页的链接
		linkStrings = mDataParser.getPageLinks(pData);
		
		for (String link : linkStrings) {
			// 防止第一页被重复下载，节约IO
			if (!skipDownload) {
				if (null == (pData = getPageData(link))) 
					return false;
			} else
				skipDownload = false;
			
			List<BBSReply> reps = null;
			
			try {
				reps = mDataParser.parse(pData);
			} catch (Exception e) {
				mContext.errorWithException("BBSCrawlerTask.getTopic", "解析帖子数据时出现异常", e, 
						"帖子", topic.url(),
						"页面", link);
				return false;
			}
			topic.appendReply(reps);
		}
			
		topic.trim();
		return true;
	}
	
	/**
	 * 逐页的下载整个帖子
	 * @throws BBSParseException 
	 */
	private boolean getTopicPageByPage(BBSTopic topic) throws BBSParseException {
		// 首先获取第一页数据
		PageData pData = getPageData(topic.url());
		if (pData == null)
			return false;
		
		// 存放各页的链接
		List<String> linkStrings;
		// 是否跳过首页下载。
		boolean skipDownload = true;
		
		// 初始化上下文
		TopicDataContext context = mDataParser.initContext(pData);
		
		// 获取当前可以获取各页的链接
		linkStrings = mDataParser.getPageLinks(pData, context);
		
		while (linkStrings.size() != 0) {
			for (String link : linkStrings) {
				// 防止第一页被重复下载，节约IO
				if (!skipDownload) {
					pData = getPageData(link);
					if (null == pData)
						return false;
				} else
					skipDownload = false;
				
				List<BBSReply> reps = null;;
				
				try { 
					reps = mDataParser.parse(pData, context);
				} catch (Exception e) {
					mContext.errorWithException("BBSCrawlerTask.getTopicPageByPage", "解析帖子数据时出现异常", e, 
							"MinerID", mMiner.getBBSName(),
							"帖子Url", topic.url(),
							"页面Url", link);
					return false;
				}
				
				topic.appendReply(reps);
			}
			
			// 从最后一页获取新一轮循环的链接
			try {
				linkStrings = mDataParser.getPageLinks(pData, context);
			} catch (Exception e) {
				mContext.errorWithException("BBSCrawlerTask.getTopicPageByPage", "解析帖子下一页链接时出现异常", e, 
						"MinerID", mMiner.getBBSName(),
						"帖子Url", topic.url(),
						"页面Url", linkStrings.get(linkStrings.size() - 1));
				return false;
			}
		}
		
		topic.trim();
		return true;
	}
	
	/**
	 * 将一个帖子转换为存储请求
	 * @param topic：待转换的帖子
	 * @return
	 */
	private StorageRequest makeTopicStorageRequest(BBSTopic topic) {
		// 产生一个HDFS存储请求
		StorageRequest request = new StorageRequest(StorageRequest.StorageType.HDFS_STORAGE);

		request.setParam(StorageExecutor.FILE_NAME, "topics");
		
		request.setData("title", topic.title());	// 标题
		request.setData("forum", mBlock.forumName());		// 版块名
		request.setData("type", mContext.getMinerType());		// 类型，bbs或者twitter
		request.setData("repcount", topic.getReplyCount());	// 回复数
		request.setData("id", topic.topicIentifier());	// 论坛内唯一标志
		request.setData("url", topic.url());
		request.setData("bbsid", topic.getBBSIdentifier());// 所在论坛标识符
		request.setData("bbsname", topic.getBBSName());	// 所在论坛名称
		
		// 将每一楼的用户id，用户名，帖子内容和日期放入请求数据中
		int i = 1;
		for (BBSReply reply : topic.contents()) {
			request.setData(i + "#uid", reply.authorId());
			request.setData(i + "#uname", reply.authorName());
			request.setData(i + "#body", reply.content());
			request.setData(i + "#date", reply.postDate().getTime());
			
			i ++;
		}
		
		return request;
	}
	
	/**
	 * 构造一个用户数据的存储请求 
	 * @param topic：Topic内
	 * @return
	 */
	private StorageRequest makeUserStorageRequest(BBSUser user) {
		// 产生一个HDFS存储请求
		StorageRequest request = new StorageRequest(StorageRequest.StorageType.HDFS_STORAGE);

		Date tmpDate = null;
		
		request.setParam(StorageExecutor.FILE_NAME, "users");
		
		request.setData("uid", user.useId());
		request.setData("uname", user.userName());
		request.setData("level", user.level());
		request.setData("sex", user.sex());
		request.setData("score", user.score());
		
		if (null != (tmpDate = user.regDate())) {
			request.setData("regtime", tmpDate.getTime());
		}
		
		if (null != (tmpDate = user.birthDay())) {
			request.setData("birthday", tmpDate.getTime());
		}
		
		if (null != (tmpDate = user.lastLoginDate())) {
			request.setData("lastlogin", tmpDate.getTime());
		}
		
		return request;
	}
	
	/**
	 * 指定url的情况下直接构造一个Get方法并提交以下载某个页面的数据
	 * @param url：待下载链接
	 * @return：若下载成功并且通过了检查，则直接返回下载得到的数据。
	 * 			若下载数据没有通过检查，则返回null
	 */
	private PageData getPageData(String url) {
		PageRequest request = PageRequest.getHttpGet(url);
		
		try {
			mContext.getPageDownloader().execute(request);
		} catch (Exception e) {
			mContext.errorWithException("BBSCrawlerTask.getPageData", "getPageDownloader().execute(request)抛出未知异常", e,
						"MinerId", mMiner.getBBSName(),
						"url", url);
			return null;
		}
		
		PageData ret = request.getResponse();
		
		if (!mChecker.check(ret)) {
			mContext.log(mMiner.getIdentifier(), "数据未通过检查，被跳过：" + url);
			
			mContext.error("BBSCrawlerTask.getPageData", "mChecker.check(ret)失败，未通过校验",
					"MinerId", mMiner.getBBSName(),
					"url", url);
			return null;
		}
		
		return ret;
	}
}
