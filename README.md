# GoRTMP [![Build Status](https://secure.travis-ci.org/zhangpeihao/gortmp.png)](http://travis-ci.org/zhangpeihao/gortmp)
======

RTMP protocol implementation.

## Spec: 
* RTMP - http://www.adobe.com/devnet/rtmp.html
* AMF0 - http://download.macromedia.com/pub/labs/amf/amf0_spec_121207.pdf
* AMF3 - http://download.macromedia.com/pub/labs/amf/amf3_spec_121207.pdf


## Todo:
* Inbound side

## Examples:

```golang
// To connect FMS server
obConn, err := rtmp.Dial(url, handler, 100)

// To connect
err = obConn.Connect()

// When new stream created, handler event OnStreamCreated() would been called
func (handler *TestOutboundConnHandler) OnStreamCreated(stream rtmp.OutboundStream) {
	// To play
	err = stream.Play(*streamName, nil, nil, nil)
	// Or publish
	err = stream.Publish(*streamName, "live")
}

// To publish data
stream.PublishAudioData(data, deltaTimestamp)
// or
stream.PublishVideoData(data, deltaTimestamp)
// or
stream.PublishData(tagHeader.TagType, data, deltaTimestamp)

// You can close stream by
stream.Close()

// You can close connection by
obConn.Close()
```