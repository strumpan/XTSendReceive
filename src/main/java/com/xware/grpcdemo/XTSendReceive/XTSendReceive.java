package com.xware.grpcdemo.XTSendReceive;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.netty.handler.ssl.SslContext;
import xware.xt.grpc.application.LogOnMessage;
import xware.xt.grpc.application.LogOnReply;
import xware.xt.grpc.application.MsgGrpc;
import xware.xt.grpc.application.MsgSetStatusMessage;
import xware.xt.grpc.application.MsgStatusCommand;
import xware.xt.grpc.application.SubmitMsgMessage;
import xware.xt.grpc.application.SubmitMsgReply;
import xware.xt.grpc.application.WaitMsgMessage;
import xware.xt.grpc.application.WaitMsgReply;
import xware.xt.grpc.application.MsgGrpc.MsgBlockingStub;
import xware.xt.grpc.application.MsgGrpc.MsgStub;

/**
 * XTSendReceive!
 *
 */
public class XTSendReceive
{
    public static void main( String[] args ) throws Exception
    {
		String configFile = args[0];
		String uri = "";
		String password = "";
		String connect = "";
		String connIP = "";
		Integer connPort = 0;
		JSONParser parser = new JSONParser();
		InputStream caStream = null;
		String contract = "";
		String outFilename = "";
		String inFile = "";

		if(args.length == 4) {
			new File(args[0]).isFile();
			outFilename = args[1];
			contract = args[2];
			inFile = args[3];
			
			System.out.println("Contract=" + contract);
		} 
		else {
			System.out.println("Missing arguments! Configuration filename, Output filename, Contract and inFile");
			System.exit(0);
		}

		try {
			Object obj = parser.parse(new FileReader(configFile));
			
			JSONObject jsonObject = (JSONObject) obj;
			JSONObject app = (JSONObject) jsonObject.get("application");
			password = (String) app.get("password");
			String ca = (String) jsonObject.get("cabundle");
			uri = (String) app.get("uri");
			connect = (String) jsonObject.get("connect");
			String[] arrOfConn = connect.split(":", 2);
			connIP = arrOfConn[0];
			connPort = Integer.parseInt(arrOfConn[1]);
			caStream = new ByteArrayInputStream(ca.getBytes(StandardCharsets.UTF_8));
			System.out.println("application/password=" + password);
			System.out.println("ca=" + ca);
			System.out.println("application/uri=" + uri);
			System.out.println("1=" + arrOfConn[0]);
			System.out.println("2=" + arrOfConn[1]);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
		Certificate cert = certificateFactory.generateCertificate(caStream);
		
		X509Certificate x509Cert = (X509Certificate)cert;
		
		SslContext sslctx = GrpcSslContexts.forClient().trustManager(x509Cert).build();
		
		NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(connIP, connPort);
		Metadata auth_meta = new Metadata();
		channelBuilder.sslContext(sslctx);
		// Add a metadata interceptor for authentication when the channel is created
		channelBuilder.intercept(MetadataUtils.newAttachHeadersInterceptor(auth_meta));
		Channel channel = channelBuilder.build();

		MsgBlockingStub xt = MsgGrpc.newBlockingStub(channel);
		MsgStub xt_async = MsgGrpc.newStub(channel);

		
		LogOnReply logonReply = xt.logOn(LogOnMessage.newBuilder()
				.setApplication(uri)
				.setPassword(password)
				.build());
		
		// Set the authentication data returned from LogOn in the metadata interceptor		
		auth_meta.put(Key.of(logonReply.getMetaKey(), Metadata.ASCII_STRING_MARSHALLER), logonReply.getTicket());

		FileInputStream payload = new FileInputStream(inFile);
		
		SendMsg Send = new SendMsg();
		String dataref = Send.SendData(xt_async, payload);
		System.out.println("Dataref: " + dataref);
		payload.close();
		

		// Send the message to a contract that returns it to this Application
		{
			SubmitMsgReply reply = xt.submitMsg(SubmitMsgMessage.newBuilder().setContract(contract).setDataref(dataref).build());
			if (reply.getIdsCount() > 0) {
				System.out.println("Message id " + reply.getIds(0).getMsgid());
			}
		}

		{
			WaitMsgReply reply = xt.waitMsg(WaitMsgMessage.newBuilder().build()); // Timeout is 0 (no wait)
			for(int i=0; i<reply.getIdsCount(); i++)
			{
				FileOutputStream fs = new FileOutputStream(outFilename, false);
				ReceiveMsg Receive = new ReceiveMsg();
				int sz = Receive.ReceiveData(xt_async, fs, reply.getIds(i).getId());
//				int sz = receiveData(xt_async, fs, reply.getIds(i).getId());
				fs.close();
				// Mark the message as received
				xt.msgSetStatus(MsgSetStatusMessage.newBuilder().setId(reply.getIds(i).getId()).setCmd(MsgStatusCommand.STATUS_OK).build());
				
				System.out.println("Received msg " + reply.getIds(i).getId().getMsgid() + " bytes: " + sz);
			}
		}

        System.out.println( "Done!" );
    }
}
