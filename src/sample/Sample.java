package sample;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.piax.ov.OvConfigValues;
import org.piax.ov.OverlayMgr;
import org.piax.ov.Peer;
import org.piax.ov.common.Range;
import org.piax.ov.common.geog.Circle;
import org.piax.ov.common.geog.Location;
import org.piax.ov.ovs.dht.DHT;
import org.piax.ov.ovs.mskip.MSkipGraph;
import org.piax.trans.common.PeerId;
import org.piax.trans.common.PeerLocator;
import org.piax.trans.common.ReturnSet;
import org.piax.trans.rpc.RPCWrapper;
import org.piax.trans.ts.tcp.TcpLocator;
import org.piax.trans.util.LocalInetAddrs;

public class Sample {
	static Location location = null;
	private static Peer peer;
	private static OverlayMgr mgr;
	private static RPCWrapper rpc;
	private static DHT dht;

	/**
	 * サンプルプログラムの使い方を表示する。
	 */
	public static void printUsage() {
		System.out.println("usage:");
		System.out.println("java -jar Sample.jar [<自ホスト名>:]<待ち受けポート番号> [<seedホスト名>:<seedポート番号> ...]");
		System.out.println("  seedを指定しなかった場合、自分をseedとして起動する。");
	}

	/**
	 * 文字列を読み込んでInetSOcketAddress形式に変換する。
	 * ホスト名を省略した場合、ローカルホストが指定されたものとする。
	 * @param str [<ホスト名>:]<ポート番号>形式の文字列。
	 * @return InetSOcketAddress形式。
	 * @throws Exception ホスト名やポート番号が想定の形式と異なった。
	 */
	public static InetSocketAddress parseAddr(String str) throws Exception {
		InetAddress ip;
        int port;
        String[] addrEle = str.split(":");
        if (addrEle.length == 1) {
            ip = LocalInetAddrs.choice();
            port = Integer.parseInt(addrEle[0]);
        } else {
            ip = InetAddress.getByName(addrEle[0]);
            ip = LocalInetAddrs.choiceIfIsLocal(ip);
            port = Integer.parseInt(addrEle[1]);
        }
        return new InetSocketAddress(ip, port);
	}
	
	/**
	 * 入力文字列を解析してコマンドに対応するメソッドをキックする。
	 * @param input 入力文字列(1行)
	 * @return byeが入力された(終了する)場合false, ソレ以外の場合true。
	 * @throws Exception
	 */
	public static boolean inverpret(String input) throws Exception {
		String tokens[] = input.split("\\s+");
		if (tokens.length == 0 || tokens[0].equals("")) {
			// nothing
			
		} else if (tokens[0].equals("join") && tokens.length == 2) {
			join(tokens[1]);
			
		} else if (tokens[0].equals("leave") && tokens.length == 2) {
			leave(tokens[1]);
			
		} else if (tokens[0].equals("move") && tokens.length == 3) {
			move(Double.parseDouble(tokens[1]), Double.parseDouble(tokens[2]));
			
		} else if (tokens[0].equals("talk") && tokens.length == 3) {
			talk(tokens[1], tokens[2]);
			
		} else if (tokens[0].equals("emit") && tokens.length == 3) {
			emit(tokens[1], tokens[2]);
			
		} else if (tokens[0].equals("neer") && tokens.length == 5) {
			neer(Double.parseDouble(tokens[1]), Double.parseDouble(tokens[2]),
					Double.parseDouble(tokens[3]), tokens[4]);
			
		} else if (tokens[0].equals("put") && tokens.length == 3) {
			put(tokens[1], tokens[2]);
			
		} else if (tokens[0].equals("get") && tokens.length == 2) {
			get(tokens[1]);
			
		} else if (tokens[0].equals("bye")) {
			bye();
			return false;
			
		} else if (tokens[0].equals("help")) {
			help();
			
		} else {
			System.out.println("Illegal command was input.");
			help();
		}
		return true;
	}
	
	/**
	 * 適当なOverlayを選択してPeerに属性をつける。
	 * Overlayの選択にはPIAXのデフォルト設定を元にしている。
	 * Location(位置属性)→LL-Net
	 * ALMGroupId→ALM
	 * StringのOverlayは指定されていなかったので、Multi-key Skip Graphを利用する。
	 * @param key 属性。
	 * @throws Exception
	 */
	public static void join(String key) throws Exception {
		String ovClassName = mgr.getOverlayFromKeyType(key);
		if (ovClassName == null) ovClassName = MSkipGraph.class.getName();
		mgr.addKey(ovClassName, key);
	}
	
	/**
	 * Peerの属性を解除する。
	 * @param key 属性。
	 * @throws Exception
	 */
	public static void leave(String key) throws Exception {
		String ovClassName = mgr.getOverlayFromKeyType(key);
		if (ovClassName == null) ovClassName = MSkipGraph.class.getName();
		mgr.removeKey(ovClassName, key);
	}
	
	/**
	 * Peerの位置属性を登録する。
	 * OverlayにはLL-Netが使われるはず。
	 * 位置属性がすでに登録指定ある場合、既存位置属性を削除する。
	 * @param x 経度(-180 ~ 180)
	 * @param y 緯度(-90 ~ 90)
	 * @throws Exception
	 */
	public static void move(Double x, Double y) throws Exception {
		if (location != null) {
			String ovClassName = mgr.getOverlayFromKeyType(location);
			mgr.removeKey(ovClassName, location);
		}
		Location location = new Location(x, y);
		String ovClassName = mgr.getOverlayFromKeyType(location);
		mgr.addKey(ovClassName, location);
		Sample.location = location;
	}
	
	/**
	 * PeerIDを指定してメッセージを送信する。
	 * 送信先Peerに登録してあるReceiverのrecvメソッドを引数messageで呼び出す。
	 * 引数はシリアライズ可能であれば色々送信可能。
	 * @param id　送信先PeerID。
	 * @param message 送信メッセージ。
	 * @throws Exception
	 */
	public static void talk(String id, String message) throws Exception {
		PeerId peerId = new PeerId(id);
		System.out.println(rpc.remoteCall(peerId, Receiver.SERVICE_NAME, "recv", message));
	}
	
	/**
	 * 属性を持つPeerにメッセージを送信する。
	 * Multi-key Skip Graphでは属性は範囲指定可能だが、今回は特定の属性に絞る。
	 * @param key 送信先Peerの属性。
	 * @param message 送信メッセージ。
	 * @throws Exception
	 */
	public static void emit(String key, String message) throws Exception {
		String ovClassName = mgr.getOverlayFromKeyType(key);
		if (ovClassName == null) ovClassName = MSkipGraph.class.getName();
		ReturnSet<Object> rset = mgr.forwardQuery(ovClassName, new Range(key, key), message);
		while (rset.hasNext()) {
            try {
                System.out.println(rset.getNext(OvConfigValues.returnSetGetNextTimeout));
            } catch (InterruptedException e) {
                break;
            } catch (NoSuchElementException e) {
                break;
            } catch (InvocationTargetException e) {
                continue;
            }
        }
	}
	
	/**
	 * 指定座標に近い位置属性を持つPeerにメッセージを送信する。
	 * @param x 経度(-180 ~ 180)。
	 * @param y 緯度(-90 ~ 90)。
	 * @param r 半径[度]。
	 * @param message 送信メッセージ。
	 * @throws Exception
	 */
	public static void neer(Double x, Double y, Double r, String message) throws Exception {
		String ovClassName = mgr.getOverlayFromKeyType(new Location(0, 0));
		ReturnSet<Object> rset = mgr.forwardQuery(ovClassName, new Circle(x, y, r), message);
		while (rset.hasNext()) {
            try {
                System.out.println(rset.getNext(OvConfigValues.returnSetGetNextTimeout));
            } catch (InterruptedException e) {
                break;
            } catch (NoSuchElementException e) {
                break;
            } catch (InvocationTargetException e) {
                continue;
            }
        }
	}
	
	/**
	 * 分散ハッシュテーブルに値を保存する。
	 * 既に同一キーに値が格納されている場合、上書きされる。
	 * @param key 格納キー。
	 * @param value 格納する値。
	 */
	public static void put(String key, String value) {
		dht.put(key, value);
	}
	
	/**
	 * 分散ハッシュテーブルから値を取り出して表示する。
	 * 値が格納されていなかった場合nullになる。
	 * @param key 各納キー。
	 * @throws Exception
	 */
	public static void get(String key) throws Exception {
		System.out.println(dht.get(key));
	}
	
	/**
	 * P2Pネットワークから切断する。
	 * @throws Exception
	 */
	public static void bye() throws Exception {
		peer.offline();
		peer.fin();
	}
	
	/**
	 * コマンド一覧を表示する。
	 */
	public static void help() {
		System.out.println("join <room name>\n" +
						"leave <room name>\n" +
						"move <longitude> <latitude>\n" +
						"talk <id> <message>\n" +
						"emit <room name> <message>\n" +
						"neer <longitude> <latitude> <radius> <message>\n" +
						"put <key> <value>\n" +
						"get <key>\n" +
						"bye\n" +
						"help\n"
				);
	}

	/**
	 * サンプルプログラムのエントリポイント
	 * 1つめの引数を自ポート番号、2つめ以降の引数をseedのホスト名とポート番号と解釈する
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			printUsage();
			System.exit(-1);
		}
		
		InetSocketAddress localAddr;
		Set<InetSocketAddress> seedAddrs = new HashSet<InetSocketAddress>();
		try {
			localAddr = parseAddr(args[0]);
			for (int i = 1; i < args.length; i ++) {
				seedAddrs.add(parseAddr(args[i]));
			}
		} catch (Exception e) {
			e.printStackTrace();
			printUsage();
			System.exit(-1);
			return;
		}
		
		try {
			// IPからLocatorを作る
			PeerLocator localLocator = new TcpLocator(localAddr);
			Set<PeerLocator> seedLocators = new HashSet<PeerLocator>();
			for (InetSocketAddress seedAddr : seedAddrs) {
				seedLocators.add(new TcpLocator(seedAddr));
			}
			// seedが指定されなかった場合、自分=seedとなる。
			if (seedLocators.size() == 0) {
				seedLocators.add(new TcpLocator(localAddr));
			}
			// 自分のIPとseedのIPを元にPeerを作成
			peer = new Peer(localLocator, seedLocators);
			peer.online();
			
			// OverlayMgerにメッセージ受信用のReceiverを設定
			mgr = peer.getOverlayMgr();
			rpc = mgr.getRPCWrapper();
			Receiver receiver = new Receiver();
			mgr.registerCallback(receiver);
			rpc.register(receiver);
			
			// 分散ハッシュテーブルをget
			dht = mgr.getDHT();
			
			// コマンド一覧と自分のIDを表示する。単一Peerにメッセージを送信するときに利用する。
			help();
			System.out.println("my PeerId is " + peer.getPeerId());
			
			// 標準入力を1行づつ読み込んで命令を解析する
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			String line = null;
			while(line == null || inverpret(line)) {
				Thread.sleep(1000);
				line = reader.readLine();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
