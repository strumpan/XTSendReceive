package com.xware.grpcdemo.XTSendReceive;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import xware.xt.grpc.application.GetMsgDataMessage;
import xware.xt.grpc.application.MsgIdUri;
import xware.xt.grpc.application.ResultByteChunk;
import xware.xt.grpc.application.MsgGrpc.MsgStub;

public class ReceiveMsg {
	
	public int ReceiveData(MsgStub stub, OutputStream dest, MsgIdUri msgid) throws IOException, InterruptedException {

		final CountDownLatch finishLatch = new CountDownLatch(1);

		// Observer for the server replies
		class ResponseObserver implements StreamObserver<ResultByteChunk> {
			ResponseObserver(OutputStream os) {
				ostream = os;
			}

			private OutputStream ostream;

			public Status status = Status.OK;
			private int errorCode = 0;
			public int datasize = 0;

			@Override
			public void onNext(ResultByteChunk r) {
				if (r.getErrorcode() == 0) {
					try {
						ostream.write(r.getChunk().toByteArray());
						datasize += r.getChunk().size();
					}
					catch (IOException ex) {
						status = Status.INTERNAL.withCause(ex);
						finishLatch.countDown();
					}
				}
				else {
					setErrorCode(r.getErrorcode());
					finishLatch.countDown();
				}
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
			public void setErrorCode(int errorCode) {
				this.errorCode = errorCode;
			}
		};

		ResponseObserver responseObserver = new ResponseObserver(dest);
		GetMsgDataMessage request = GetMsgDataMessage.newBuilder().setId(msgid).build();
		stub.getMsgData(request, responseObserver);

		finishLatch.await(1, TimeUnit.MINUTES);

		if (!responseObserver.status.isOk())
			throw responseObserver.status.asRuntimeException();

		return responseObserver.datasize;
	}

}
