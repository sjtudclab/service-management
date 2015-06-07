package cn.edu.sjtu.se.dclab.service_management;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;

/**
 *2015年6月7日 下午12:11:00
 *@author changyi yuan
 */
public class ServiceManager {
	
	private static ServiceManager instance = null;
	
	private ZkClient client;	
	
	private ServiceManager(){
		client = new ZkClient(Config.serverIp + ":" + Config.serverPort);
	}
	
	public static ServiceManager getInstance(){
		if(instance == null){
			synchronized (ServiceManager.class) {
				if(instance == null)
					instance = new ServiceManager();
			}
		}
		return instance;
	}
	
	/**
	 * *向client注册服务
	 * @param node 注册节点，可以是域名
	 * @param data 注册内容
	 * @param dataListener 监听器，监听数据变化
	 */
	public void registe(String node, Content data, DataListener dataListener, NodeListener nodeListener){
		String path = getPath(node);
		if(client.exists(path))
			throw new RuntimeException("the node existed!");
		
		client.createPersistent(path, true);
		client.writeData(path, data);
		
		if(dataListener != null)
			client.subscribeDataChanges(path, dataListener);
		if(nodeListener != null)
			client.subscribeChildChanges(path, nodeListener);
	}
	
	/**
	 * 检索服务
	 * @param node 服务节点，可以是域名
	 * @return 该节点及其子节点下的所有数据
	 */
	public List<Content> retrieve(String node){
		List<Content> results = new ArrayList<Content>();
		
		String path = getPath(node);
		Content content = client.readData(path);
		if(content != null)
			results.add(content);
		
		List<String> paths = client.getChildren(getPath(node));
		
		for(String p : paths){
			Content cont = client.readData(path + "/" + p, true);
			if(cont != null)
				results.add(cont);
		}
		
		return results;
	}
	
	/**
	 * 更新服务
	 * @param node 服务节点，可以是域名
	 * @param data 更新数据
	 */
	public void update(String node, Content data){
		String path = getPath(node);
		client.writeData(path, data);
	}
	
	/**
	 * 移除服务
	 * @param node 服务节点，可以是域名
	 */
	public void remove(String node){
		client.deleteRecursive(getPath(node));
	}
	
	private String getPath(String node){
		String path = null;
		if(node == null || node.trim().length() == 0)
			throw new RuntimeException("node name error");
		if(node.startsWith("/"))
			path = node;
		else
			path = "/" + node;
		return path;
	}
/*	
	public static void main(String[] args) throws InterruptedException {
		ServiceManager manager = ServiceManager.getInstance();
		
		class MyContent extends Content{
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = -4740801574433386564L;
			String ip;
			String port;
			public MyContent(String ip, String port){
				this.ip = ip;
				this.port = port;
			}
			@Override
			public String toString(){
				return ip + ":" + port;
			}
		}
		
		String node = "/onlineStatus";
		String node1 = "/onlineStatus/1";
		String node2 = "/onlineStatus/2";
		
		manager.remove(node);
		
		manager.registe(node1, new MyContent("192.168.1.100","100") , new DataListener() {
			
			public void handleDataDeleted(String dataPath) throws Exception {
				System.out.println("data deleted");
			}
			
			public void handleDataChange(String dataPath, Object data) throws Exception {
				System.out.println("data changed");
			}
		}, new NodeListener() {
			
			public void handleChildChange(String parentPath, List<String> currentChilds)
					throws Exception {
				System.out.println("node removed");
			}
		});
		manager.registe(node2, new MyContent("192.168.1.102", "102"), null, null);
		
		System.out.println(manager.retrieve(node));
		manager.update(node1, new MyContent("192.168.1.101","101"));
		System.out.println(manager.retrieve(node1));		
		Thread.sleep(1000);
		manager.remove(node);
		Thread.sleep(5000);
	}
*/	
}
