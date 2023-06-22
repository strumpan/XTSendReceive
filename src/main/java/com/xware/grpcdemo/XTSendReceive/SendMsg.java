package com.xware.grpcdemo.XTSendReceive;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import xware.xt.grpc.application.ByteChunk;
import xware.xt.grpc.application.WriteMsgDataStreamReply;
import xware.xt.grpc.application.MsgGrpc.MsgStub;

public class SendMsg {
	
	public String SendData(MsgStub stub, InputStream data) throws RuntimeException, IOException, InterruptedException {
		
		  final CountDownLatch finishLatch = new CountDownLatch(1);
		 		  
		  // Observer for the server reply
		  class ResponseObserver implements StreamObserver<WriteMsgDataStreamReply> { 
			  private String dataref;
			  
			  public Status status = Status.OK;
			  
			  @Override
			  public void onNext(WriteMsgDataStreamReply r) {
				  setDataref(r.getRef()); 
			  }
			  @Override
			  public void onError(Throwable t) {
				  status = Status.fromThrowable(t);
		      	  finishLatch.countDown();
			  }
			  @Override
			  public void onCompleted() {
				  finishLatch.countDown();
		    }
			public void setDataref(String dataref) {
				this.dataref = dataref;
			}
		  };
		  		  
		  // Observer for the client stream
		  ResponseObserver responseObserver = new ResponseObserver();
		  StreamObserver<ByteChunk> requestObserver = stub.writeMsgDataStream(responseObserver);
		  
		  // Send 32k chunks of stream data
		  final int BLOCKSIZE = 32768;
		  byte[] buffer = new byte[BLOCKSIZE];
		  int nread = 0;
		  int total = 0;
		  		  
		  do {
			  nread = data.readNBytes(buffer, 0, BLOCKSIZE);
			  if (nread > 0) {
				  requestObserver.onNext(ByteChunk.newBuilder().setChunk(ByteString.copyFrom(buffer, 0, nread)).build());
				  total += nread;
			  }		  
		  } while (nread > 0 && responseObserver.status.isOk());
	
		  System.out.println("Read " + total + " bytes from stream");
		  
		  // Mark the end of requests
		  requestObserver.onCompleted();

		  // Receiving happens asynchronously
		  finishLatch.await(1, TimeUnit.MINUTES);
		  
		  if (!responseObserver.status.isOk())
			  throw responseObserver.status.asRuntimeException();
		  
		  return responseObserver.dataref;
	}

}
