package com.longyb.mylive.server.handlers;

import lombok.Data;

@Data
public class RtmpHeader {
	/**
	 * chunk stream ID（流通道Id）
	 */
	int csid;
	int fmt;
	int timestamp;
	/**
	 * Length(长度)：是指Message Payload（消息负载）即音视频等信息的数据的长度，3个字节
	 */
	int messageLength;
	short messageTypeId;
	int messageStreamId;

	int timestampDelta;

	long extendedTimestamp;
	
	//used when response an ack
	int headerLength;
	
	public boolean mayHaveExtendedTimestamp() {
		return  (fmt==0 && timestamp ==  0xFFFFFF) || ( (fmt==1 || fmt==2) && timestampDelta ==  0xFFFFFF); 
	}
}
