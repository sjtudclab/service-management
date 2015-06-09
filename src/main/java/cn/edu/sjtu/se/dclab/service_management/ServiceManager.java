package cn.edu.sjtu.se.dclab.service_management;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;

/**
 * 2015年6月7日 下午12:11:00
 *
 * @author changyi yuan
 */
public class ServiceManager {

	private static ServiceManager instance = null;

	private ZkClient client;

	private ServiceManager() {
		client = new ZkClient(Config.serverIp + ":" + Config.serverPort);
	}

	public static ServiceManager getInstance() {
		if (instance == null) {
			synchronized (ServiceManager.class) {
				if (instance == null)
					instance = new ServiceManager();
			}
		}
		return instance;
	}

	/**
	 * 
	 * 向client注册服务
	 * 
	 * @param node
	 *            注册节点，可以是域名
	 * @param data
	 *            注册内容
	 * @param dataListener
	 *            数据监听器
	 * @param nodeListener
	 *            节点监听器
	 */
	public void registe(String persistentNode, String ephemeralNode,
			Content data, DataListener dataListener, NodeListener nodeListener) {
		String path = getPath(persistentNode);

		if (!client.exists(path))
			client.createPersistent(path);

		boolean flag = false;
		if (ephemeralNode != null && ephemeralNode.trim().length() != 0) {
			if (ephemeralNode.startsWith("/")) {
				if (ephemeralNode.trim().length() > 1) {
					path += ephemeralNode;
					flag = true;
				}
			} else {
				path += "/" + ephemeralNode;
				flag = true;
			}
		}

		String str = null;
		if (data != null)
			str = data.getStr();
		if (flag) {
			if (client.exists(path))
				throw new RuntimeException("the node existed!");
			client.createEphemeral(path);
		}
		client.writeData(path, str);

		registeListener(path, dataListener, nodeListener);
	}

	/**
	 * 注册监听器
	 * 
	 * @param node
	 *            节点
	 * @param dataListener
	 *            数据监听器
	 * @param nodeListener
	 *            节点监听器
	 */
	public void registeListener(String node, DataListener dataListener,
			NodeListener nodeListener) {
		String path = getPath(node);

		if (dataListener != null)
			client.subscribeDataChanges(path, dataListener);
		if (nodeListener != null)
			client.subscribeChildChanges(path, nodeListener);
	}

	/**
	 * 检索服务
	 * 
	 * @param node
	 *            服务节点，可以是域名
	 * @return 该节点及其子节点下的所有数据
	 */
	public List<String> retrieve(String node) {
		List<String> results = new ArrayList<String>();

		String path = getPath(node);
		String content = client.readData(path);
		if (content != null)
			results.add(content);

		List<String> paths = client.getChildren(getPath(node));

		for (String p : paths) {
			String cont = client.readData(path + "/" + p, true);
			if (cont != null)
				results.add(cont);
		}

		return results;
	}

	/**
	 * 更新服务
	 * 
	 * @param node
	 *            服务节点，可以是域名
	 * @param data
	 *            更新数据
	 */
	public void update(String node, Content data) {
		String str = null;
		if (data != null)
			str = data.getStr();
		String path = getPath(node);
		client.writeData(path, str);
	}

	/**
	 * 移除服务
	 * 
	 * @param node
	 *            服务节点，可以是域名
	 */
	public void remove(String node) {
		client.deleteRecursive(getPath(node));
	}

	private String getPath(String node) {
		String path = null;
		if (node == null || node.trim().length() == 0)
			throw new RuntimeException("node name error");
		if (node.startsWith("/"))
			path = node;
		else
			path = "/" + node;
		return path;
	}

	public static void main(String[] args) throws InterruptedException {
		ServiceManager manager = ServiceManager.getInstance();

		class MyContent extends Content {
			/**
			 * 
			 */
			private static final long serialVersionUID = -4740801574433386564L;
			String ip;
			String port;

			public MyContent(String ip, String port) {
				this.ip = ip;
				this.port = port;
			}

			@Override
			public String toString() {
				return ip + ":" + port;
			}

			@Override
			public String getStr() {
				return ip + ":" + port;
			}
		}

		String node = "/onlineStatus";
		String node1 = "1";
		String node2 = "2";

		// manager.remove(node);

		manager.registe(node, node1, new MyContent("192.168.1.100", "100"),
				new DataListener() {

					public void handleDataDeleted(String dataPath)
							throws Exception {
						System.out.println("data deleted");
					}

					public void handleDataChange(String dataPath, Object data)
							throws Exception {
						System.out.println("data changed");
					}
				}, new NodeListener() {

					public void handleChildChange(String parentPath,
							List<String> currentChilds) throws Exception {
						System.out.println("node removed");
					}
				});
		manager.registe(node, node2, new MyContent("192.168.1.102", "102"),
				null, null);

		System.out.println(manager.retrieve(node));
		manager.update(node + "/" + node1,
				new MyContent("192.168.1.101", "101"));
		System.out.println(manager.retrieve(node + "/" + node1));
		Thread.sleep(1000);
		manager.remove(node);
		Thread.sleep(5000);
	}

}
