package sample;

import java.io.Serializable;
import java.util.Set;

import org.piax.ov.AggregationAbortException;
import org.piax.ov.OverlayCallback;
import org.piax.ov.TraversalAbortException;
import org.piax.trans.common.ReturnSet;
import org.piax.trans.rpc.OldRPCService;

/**
 * Peerからのメッセージを受信するためのクラス。
 * @author Yuji Ito
 */
public class Receiver implements OldRPCService, OverlayCallback {
	static String SERVICE_NAME = Receiver.class.getName();
	
	/**
	 * RPCWrapper.remoteCallので指定されるサービス名を戻す。
	 */
	@Override
	public String getServiceName() {
		return SERVICE_NAME;
	}
	
	/**
	 * 受信したメッセージを表示する。
	 * RPCWrapper.remoteCallで送るメッセージを受信する。
	 * メソッド名や引数はなんでも良いがremoteCall呼び出し引数と合わせる必要がある。
	 * @param message 受信メッセージ。
	 * @return
	 */
	public String recv(String message) {
		System.out.println(message);
		return "Received:" + this.toString();
	}

	/**
	 * 受信したメッセージを表示する。
	 * OverlayMgr.forwardQueryで送信したメッセージを受信する。
	 */
	@Override
	public Object execQuery(Object arg0, Object arg1) {
		System.out.println("execQuery(" + arg0 + ", " + arg1 + ")");
		return null;
	}

	/**
	 * 受信したメッセージを表示する。
	 * OverlayMgr.forwardQueryで送信したメッセージを受信する。
	 */
	@Override
	public ReturnSet<Object> execQuery(Set<Comparable<?>> arg0, Object arg1) {
		System.out.println("execQuery((Set)" + arg0 + ", " + arg1 + ")");
		return null;
	}

	/**
	 * 試していないけど、OverlayMgr.forwardQueryAggregationと関連した動きをするのではないか？
	 */
	@Override
	public Serializable execQueryPostAggregation(boolean arg0,
			Set<Comparable<?>> arg1, Serializable arg2, Serializable arg3,
			Serializable... arg4) {
		System.out.println("execQueryPostAggregation(" +
			arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ")");
		return null;
	}

	/**
	 * 試していないけど、OverlayMgr.forwardQueryAggregationと関連した動きをするのではないか？
	 */
	@Override
	public Serializable execQueryPreAggregation(boolean arg0,
			Set<Comparable<?>> arg1, Serializable arg2, Serializable arg3)
			throws AggregationAbortException {
		System.out.println("execQueryPreAggregation(" +
			arg1 + ", " + arg2 + ", " + arg3 + ")");
		return null;
	}

	/**
	 * 試していないけど、OverlayMgr.forwardQueryTraversalと関連した動きをするのではないか？
	 */
	@Override
	public Serializable execQueryTraversal(boolean arg0,
			Set<Comparable<?>> arg1, Serializable arg2, Serializable arg3)
			throws TraversalAbortException {
		System.out.println("execQueryTraversal(" +
			arg1 + ", " + arg2 + ", " + arg3 + ")");
		return null;
	}
}
