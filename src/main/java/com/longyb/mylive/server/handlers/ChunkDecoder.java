package com.longyb.mylive.server.handlers;

import java.util.HashMap;
import java.util.List;

import com.longyb.mylive.server.rtmp.RtmpMessageDecoder;
import com.longyb.mylive.server.rtmp.messages.RtmpMessage;
import com.longyb.mylive.server.rtmp.messages.SetChunkSize;

import static com.longyb.mylive.server.rtmp.Constants.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author longyubo
 * @version 2019年12月14日 下午3:40:18
 *
 */
@Slf4j
public class ChunkDecoder extends ReplayingDecoder<DecodeState> {

	// changed by client command
	//默认128字节
	int clientChunkSize = 128;

	HashMap<Integer/* csid */, RtmpHeader> prevousHeaders = new HashMap<>(4);
	HashMap<Integer/* csid */, ByteBuf> inCompletePayload = new HashMap<>(4);

	ByteBuf currentPayload = null;
	int currentCsid;

	int ackWindowSize = -1;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		//解码状态机
		DecodeState state = state();

		if (state == null) {
			//初始设置为 解码header 状态
			state(DecodeState.STATE_HEADER);
		}
		if (state == DecodeState.STATE_HEADER) {
			//获取chunk 的header信息
			RtmpHeader rtmpHeader = readHeader(in);
			log.debug("rtmpHeader read:{}", rtmpHeader);
			//填充 头信息（针对fmt >0时的header）
			completeHeader(rtmpHeader);
			currentCsid = rtmpHeader.getCsid();

			// initialize the payload
			if (rtmpHeader.getFmt() != CHUNK_FMT_3) {
				//初始化一个数据缓冲区
				ByteBuf buffer = Unpooled.buffer(rtmpHeader.getMessageLength(), rtmpHeader.getMessageLength());
				inCompletePayload.put(rtmpHeader.getCsid(), buffer);
				//保存覆盖本次头信息（fmt=3时会查询前一个chunk header 信息）
				prevousHeaders.put(rtmpHeader.getCsid(), rtmpHeader);
			}

			currentPayload = inCompletePayload.get(rtmpHeader.getCsid());
			if (currentPayload == null) {
				// when fmt=3 and previous body completely read, the previous msgLength play the
				// role of length
				RtmpHeader previousHeader = prevousHeaders.get(rtmpHeader.getCsid());
				log.debug("current payload null,previous header:{}", previousHeader);
				currentPayload = Unpooled.buffer(previousHeader.getMessageLength(), previousHeader.getMessageLength());
				inCompletePayload.put(rtmpHeader.getCsid(), currentPayload);
				log.debug("current payload assign as :{}",currentPayload);
			}
			//设置状态为解码 payload
			checkpoint(DecodeState.STATE_PAYLOAD);
		} else if (state == DecodeState.STATE_PAYLOAD) {

			final byte[] bytes = new byte[Math.min(currentPayload.writableBytes(), clientChunkSize)];
			in.readBytes(bytes);
			//数据写入当前负载
			currentPayload.writeBytes(bytes);
			//
			checkpoint(DecodeState.STATE_HEADER);
			//检测当前流是否已满，已满则表示当前流处理完毕，删除流缓存，否则return等待下一个chunk
			if (currentPayload.isWritable()) {
				return;
			}
			inCompletePayload.remove(currentCsid);

			// then we can decode out payload
			ByteBuf payload = currentPayload;
			RtmpHeader header = prevousHeaders.get(currentCsid);
			//解码
			RtmpMessage msg = RtmpMessageDecoder.decode(header, payload);
			if (msg == null) {
				log.error("RtmpMessageDecoder.decode NULL");
				return;
			}
			//是否是控制信息
			if (msg instanceof SetChunkSize) {
				// we need chunksize to decode the chunk
				SetChunkSize scs = (SetChunkSize) msg;
				clientChunkSize = scs.getChunkSize();
				log.debug("------------>client set chunkSize to :{}", clientChunkSize);
			} else {
				out.add(msg);
			}
		}

	}

	/**
	 * 通过 缓冲区获取头信息；参考地址https://www.cnblogs.com/jimodetiantang/p/8974075.html
	 * https://www.cnblogs.com/lidabo/p/9106468.html
	 * @param in
	 * @return
	 */
	private RtmpHeader readHeader(ByteBuf in) {
		RtmpHeader rtmpHeader = new RtmpHeader();

		// alway from the beginning
		int headerLength = 0;

		byte firstByte = in.readByte();
		headerLength += 1;

		// CHUNK HEADER is divided into
		// BASIC HEADER
		// MESSAGE HEADER
		// EXTENDED TIMESTAMP

		// BASIC HEADER
		// fmt and chunk steam id in first byte
		int fmt = (firstByte & 0xff) >> 6;
		int csid = (firstByte & 0x3f);
		//0，1，2由协议保留表示特殊信息。
		// 0代表Basic Header总共要占用2个字节，CSID在［64，319］之间，
		// 1代表占用3个字节，CSID在［64，65599］之间，
		// 2代表该chunk是控制信息和一些命令信息，后面会有详细的介绍。
		if (csid == 0) {
			// 2 byte form
			csid = in.readByte() & 0xff + 64;
			headerLength += 1;
		} else if (csid == 1) {
			// 3 byte form
			byte secondByte = in.readByte();
			byte thirdByte = in.readByte();
			//需要注意的是，Basic Header是采用小端存储的方式，越往后的字节数量级越高，因此通过这3个字节每一位的值来计算CSID时，应该是:<第三个字节的值>x256+<第二个字节的值>+64
			csid = (thirdByte & 0xff) << 8 + (secondByte & 0xff) + 64;
			headerLength += 2;
		} else if (csid >= 2) {
			// that's it!
		}

		rtmpHeader.setCsid(csid);
		rtmpHeader.setFmt(fmt);

		// basic header complete

		// MESSAGE HEADER
		switch (fmt) {
		case CHUNK_FMT_0: {
			//type=0时Message Header占用11个字节，
			// 其他三种能表示的数据它都能表示，
			// 但在chunk stream的开始的第一个chunk和头信息中的时间戳后退（即值与上一个chunk相比减小，通常在回退播放的时候会出现这种情况）的时候必须采用这种格式。
			int timestamp = in.readMedium();//timestamp（时间戳）：占用3个字节，因此它最多能表示到16777215=0xFFFFFF=2^24-1, 当它的值超过这个最大值时，这三个字节都置为1，这样实际的timestamp会转存到Extended Timestamp字段中，接受端在判断timestamp字段24个位都为1时就会去Extended timestamp中解析实际的时间戳。
			int messageLength = in.readMedium();//message length（消息数据的长度）：占用3个字节，表示实际发送的消息的数据如音频帧、视频帧等数据的长度，单位是字节。注意这里是Message的长度，也就是chunk属于的Message的总数据长度，而不是chunk本身Data的数据的长度。
			short messageTypeId = (short) (in.readByte() & 0xff);//message type id(消息的类型id)：占用1个字节，表示实际发送的数据的类型，如8代表音频数据、9代表视频数据。
			int messageStreamId = in.readIntLE();//msg stream id（消息的流id）：占用4个字节，表示该chunk所在的流的ID，和Basic Header的CSID一样，它采用小端存储的方式，
			headerLength += 11;
			//计算扩展时间
			if (timestamp == MAX_TIMESTAMP) {
				long extendedTimestamp = in.readInt();
				rtmpHeader.setExtendedTimestamp(extendedTimestamp);
				headerLength += 4;
			}

			rtmpHeader.setTimestamp(timestamp);
			rtmpHeader.setMessageTypeId(messageTypeId);
			rtmpHeader.setMessageStreamId(messageStreamId);
			rtmpHeader.setMessageLength(messageLength);

		}
			break;

		case CHUNK_FMT_1: {
			//type=1时Message Header占用7个字节，省去了表示msg stream id的4个字节，表示此chunk和上一次发的chunk所在的流相同，如果在发送端只和对端有一个流链接的时候可以尽量去采取这种格式。
			int timestampDelta = in.readMedium();
			int messageLength = in.readMedium();
			short messageType = (short) (in.readByte() & 0xff);

			headerLength += 7;
			if (timestampDelta == MAX_TIMESTAMP) {
				long extendedTimestamp = in.readInt();
				rtmpHeader.setExtendedTimestamp(extendedTimestamp);
				headerLength += 4;
			}

			rtmpHeader.setTimestampDelta(timestampDelta);
			rtmpHeader.setMessageLength(messageLength);
			rtmpHeader.setMessageTypeId(messageType);
		}
			break;
		case CHUNK_FMT_2: {
			//type=2时Message Header占用3个字节，相对于type＝1格式又省去了表示消息长度的3个字节和表示消息类型的1个字节，表示此chunk和上一次发送的chunk所在的流、消息的长度和消息的类型都相同。余下的这三个字节表示timestamp delta，使用同type＝1
			int timestampDelta = in.readMedium();
			headerLength += 3;
			rtmpHeader.setTimestampDelta(timestampDelta);

			if (timestampDelta == MAX_TIMESTAMP) {
				long extendedTimestamp = in.readInt();
				rtmpHeader.setExtendedTimestamp(extendedTimestamp);
				headerLength += 4;
			}

		}
			break;

		case CHUNK_FMT_3: {
			//Type = 3
			//0字节！！！好吧，它表示这个chunk的Message Header和上一个是完全相同的，自然就不用再传输一遍了。当它跟在Type＝0的chunk后面时，表示和前一个chunk的时间戳都是相同的。什么时候连时间戳都相同呢？就是一个Message拆分成了多个chunk，这个chunk和上一个chunk同属于一个Message。而当它跟在Type＝1或者Type＝2的chunk后面时，表示和前一个chunk的时间戳的差是相同的。比如第一个chunk的Type＝0，timestamp＝100，第二个chunk的Type＝2，timestamp delta＝20，表示时间戳为100+20=120，第三个chunk的Type＝3，表示timestamp delta＝20，时间戳为120+20=140
			// nothing
		}
			break;

		default:
			throw new RuntimeException("illegal fmt type:" + fmt);
		}

		rtmpHeader.setHeaderLength(headerLength);

		return rtmpHeader;
	}

	private void completeHeader(RtmpHeader rtmpHeader) {
		RtmpHeader prev = prevousHeaders.get(rtmpHeader.getCsid());
		if (prev == null) {
			return;
		}
		switch (rtmpHeader.getFmt()) {
		case CHUNK_FMT_1:
			//填充message stream id
			rtmpHeader.setMessageStreamId(prev.getMessageStreamId());
//			rtmpHeader.setTimestamp(prev.getTimestamp());
			break;
		case CHUNK_FMT_2:
//			rtmpHeader.setTimestamp(prev.getTimestamp());
			rtmpHeader.setMessageLength(prev.getMessageLength());
			rtmpHeader.setMessageStreamId(prev.getMessageStreamId());
			rtmpHeader.setMessageTypeId(prev.getMessageTypeId());
			break;
		case CHUNK_FMT_3:
			rtmpHeader.setMessageStreamId(prev.getMessageStreamId());
			rtmpHeader.setMessageTypeId(prev.getMessageTypeId());
			rtmpHeader.setTimestamp(prev.getTimestamp());
			rtmpHeader.setTimestampDelta(prev.getTimestampDelta());
			break;
		default:
			break;
		}

	}

}
