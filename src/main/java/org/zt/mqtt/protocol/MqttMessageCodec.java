package org.zt.mqtt.protocol;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DisconnectDeviceMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.core.message.firmware.RequestFirmwareMessageReply;
import org.jetlinks.core.message.firmware.UpgradeFirmwareMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.message.property.WritePropertyMessage;
import org.jetlinks.core.message.property.WritePropertyMessageReply;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class MqttMessageCodec implements DeviceMessageCodec {
    @Override
    public Transport getSupportTransport() {
        return DefaultTransport.MQTT;
    }

    //消息解码中心
    @Nonnull
    @Override
    public Flux<? extends Message> decode(@Nonnull MessageDecodeContext context) {
        return Flux.defer(()-> {
            //转为原始的mqtt消息
            MqttMessage mqttMessage = (MqttMessage)context.getMessage();

            String topicDid = mqttMessage.getTopic();//原始topic
            byte[] data = mqttMessage.payloadAsBytes();//原始数据
            int sepIndex = topicDid.lastIndexOf("/");
            String topic = topicDid.substring(0,sepIndex);//主题
            String deviceId = topicDid.substring(sepIndex + 1).trim();//设备号

            switch(topic){
                case "/zt/up/property" : return propertyUp(deviceId,data,context);
                case "/zt/up/function" : return functionUp(deviceId,data);
                case "/zt/up/writeLedReply" : return writeLedReply(deviceId,data);
                default : return Flux.empty();//handleEvent(topic,deviceId);
            }
        });
    }
    //LED大屏数据回复消息解码
    private Flux<DeviceMessage> writeLedReply(String deviceId,byte[] data){
        return Flux.just(WritePropertyMessageReply
                .create()
                .deviceId(deviceId)
                .messageId(deviceId)
                .success()
                .message("led reply"));

    }

    //设备功能调用结果上报消息解码
    private Flux<DeviceMessage> functionUp(String deviceId,byte[] data){
        return Flux.just(FunctionInvokeMessageReply
                .create()
                .deviceId(deviceId)
                .success()
                .messageId(deviceId));
    }

    //设备属性上报消息解码
    private Flux<DeviceMessage> propertyUp(String deviceId,byte[] data,MessageDecodeContext context) {
        int skipLen = 0;//跳过的位数，可选
        return context.getDevice(deviceId)
                    .flatMap(DeviceOperator :: getMetadata)
                    .flatMapMany(dm-> Flux.fromIterable(dm.getProperties().stream().filter(pm -> {
                            String rule = pm.getExpands().get("rule") + "";//解析规则
                            int i = rule.length();
                            while(i > 0) if(rule.charAt(--i) != '0') break;
                            return i < data.length - skipLen;
                        }).map(pm -> {
                            String id = pm.getId();//待更新的属性
                            String rule = pm.getExpands().get("rule") + "";//解析规则
                            String zoom = pm.getExpands().get("zoom") + "";//缩放倍数
                            String type = pm.getValueType().getType();//类型
                            int scale=0;
                            if(type.equalsIgnoreCase("float") || type.equalsIgnoreCase("double")){
                                try {
                                    scale = Integer.parseInt(pm.toJson().getJSONObject("valueType").getString("scale"));
                                } catch(Exception e) {
                                    log.error("属性值的精度数据错误.");
                                }
                            }
                            //以下是根据解析规则查找数据的位置
                            int start = 0;
                            for(;start < rule.length();start++) if(rule.charAt(start) != 48) break;
                            int end = start;
                            for(;end < rule.length();end++) if(rule.charAt(end) == 48) break;

                            start += skipLen;
                            end += skipLen;
                            byte[] num = new byte[end - start];
                            for(int i = start;i < end;i++){
                                int ist = rule.charAt(i - skipLen);//解析位置
                                num[ist - 65] = data[i];
                            }

                            Map<String,Object> pJson = new HashMap<>();
                            switch(type){
                                case "int":
                                    int intValue = (int)(Integer.parseInt(new String(num),16) * Float.parseFloat(zoom));
                                    pJson.put(id,intValue);
                                    break;
                                case "long":
                                    long longValue = (long)(Long.parseLong(new String(num),16) * Float.parseFloat(zoom));
                                    pJson.put(id,longValue);
                                    break;
                                case "float":
                                    if(id.startsWith("float-")){
                                        num = new byte[(end - start) * 2];
                                        for(int i = start;i < end;i++){
                                            int ist = rule.charAt(i - skipLen);//解析位置
                                            num[(ist - 65) * 2] = data[2 * i - start];
                                            num[(ist - 65) * 2 + 1] = data[2 * i - start + 1];
                                        }
                                        float value754 = Float.intBitsToFloat(Integer.parseInt(new String(num),16)) * Float.parseFloat(zoom);
                                        value754 = getDesignatedFraction(value754,scale);
                                        pJson.put(id,value754);
                                        break;
                                    }
                                    float floatValue = Integer.parseInt(new String(num),16) * Float.parseFloat(zoom);
                                    floatValue = getDesignatedFraction(floatValue,scale);
                                    pJson.put(id,floatValue);
                                    break;
                                case "double":
                                    if(id.startsWith("double-")){
                                        num = new byte[(end - start) * 2];
                                        for(int i = start;i < end;i++){
                                            int ist = rule.charAt(i - skipLen);//解析位置
                                            num[(ist - 65) * 2] = data[2 * i - start];
                                            num[(ist - 65) * 2 + 1] = data[2 * i - start + 1];
                                        }
                                        double value754 = Double.longBitsToDouble(Long.parseLong(new String(num),16)) * Float.parseFloat(zoom);
                                        value754 = getDesignatedFraction(value754,scale);
                                        pJson.put(id,value754);
                                        break;
                                    }
                                    double doubleValue = Long.parseLong(new String(num),16) * Double.parseDouble(zoom);
                                    doubleValue = getDesignatedFraction(doubleValue,scale);
                                    pJson.put(id,doubleValue);
                                    break;
                                case "boolean":
                                    int boolValue = Integer.parseInt(new String(num),16);
                                    pJson.put(id,boolValue != 0);
                                    break;
                                case "string":
                                    if(id.startsWith("wind-")){
                                        int w = Integer.parseInt(new String(num));
                                        String strValue = "无风";
                                        switch(w){
                                            case 0: strValue = "东北偏北"; break;
                                            case 1: strValue = "东北"; break;
                                            case 2: strValue = "东北偏东"; break;
                                            case 3: strValue = "正东"; break;
                                            case 4: strValue = "东南偏东"; break;
                                            case 5: strValue = "东南"; break;
                                            case 6: strValue = "东南偏南"; break;
                                            case 7: strValue = "正南"; break;
                                            case 8: strValue = "西南偏南"; break;
                                            case 9: strValue = "西南"; break;
                                            case 10: strValue = "西南偏西"; break;
                                            case 11: strValue = "正西"; break;
                                            case 12: strValue = "西北偏西"; break;
                                            case 13: strValue = "西北"; break;
                                            case 14: strValue = "西北偏北"; break;
                                            case 15: strValue = "正北"; break;
                                        }
                                        pJson.put(id,strValue);
                                    } else {
                                        String strValue = new String(num);
                                        pJson.put(id,strValue);
                                    }
                                    break;
                                case "geoPoint":
                                    int degreeLat = Integer.parseInt(new String(num,0,4),16);
                                    int branchLat = Integer.parseInt(new String(num,4,4),16);
                                    int lat = Integer.parseInt(new String(num,8,4),16);
                                    int degreeLon = Integer.parseInt(new String(num,12,4),16);
                                    int branchLon = Integer.parseInt(new String(num,16,4),16);
                                    int lon = Integer.parseInt(new String(num,20,4),16);

                                    String longitude = lon==1?"西经:":"东经:";
                                    String latitude = lat==1?"北纬:":"南纬:";

                                    String geoValue=degreeLon+"°"+branchLon+"′,";
                                    geoValue+=degreeLat+"°"+branchLat+"′";

                                    pJson.put(id,geoValue);
                                    break;
                            }
                            return ReadPropertyMessageReply
                                    .create().deviceId(deviceId)
                                    .messageId(deviceId)
                                    .success(pJson);
                        }).collect(Collectors.toList())));
    }

//    //其他事件消息
//    private Flux<DeviceMessage> handleEvent(String topic, String deviceId) {
//        EventMessage eventMessage = new EventMessage();
//        eventMessage.setDeviceId(deviceId);
//        eventMessage.setEvent(topic);
//        eventMessage.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        eventMessage.setData(new HashMap<>());
//        return Flux.just(eventMessage);
//    }
        //message = handleEvent(topic, json);
//        } else if (topic.startsWith("/fire_alarm")) {
//            message = handleFireAlarm(topic, json);
//        } else if (topic.startsWith("/fault_alarm")) {
//            message = handleFaultAlarm(topic, json);
//        } else if (topic.startsWith("/register")) {
//            message = handleRegister(json);
//        } else if (topic.startsWith("/unregister")) {
//            message = handleUnRegister(json);
//        } else if (topic.startsWith("/dev_msg")) {
//            message = handleDeviceMessage(topic, json);
//        } else if (topic.startsWith("/device_online_status")) {
//            message = handleDeviceOnlineStatus(topic, json);
//        } else if (topic.startsWith("/report-property")) { //定时上报属性
//            message = handleReportProperty(json);
//        } else if (topic.startsWith("/write-property")) {
//            message = handleWritePropertyReply(json);
//        } else if (topic.startsWith("/open-door")) {
//            message = handleOpenTheDoor(topic, json);
//        } else if (topic.startsWith("/children")) {
//            ChildDeviceMessage childDeviceMessage = new ChildDeviceMessage();
//            childDeviceMessage.setDeviceId(deviceId);
//            DeviceMessage children = doDecode(deviceId, topic.substring(9), json);
//            childDeviceMessage.setChildDeviceMessage(children);
//            childDeviceMessage.setChildDeviceId(children.getDeviceId());
//            message = childDeviceMessage;
//        }
//        // 固件相关1.0.3版本后增加,注意: 专业版中才有固件相关业务功能
//        else if (topic.startsWith("/firmware/report")) {//上报固件信息
//            message = json.toJavaObject(ReportFirmwareMessage.class);
//        } else if (topic.startsWith("/firmware/progress")) { //上报升级进度
//            message = json.toJavaObject(UpgradeFirmwareProgressMessage.class);
//        } else if (topic.startsWith("/firmware/pull")) { //拉取固件信息
//            message = json.toJavaObject(RequestFirmwareMessage.class);
//        } else if (topic.startsWith("/tags")) { //更新tags
//            message = json.toJavaObject(UpdateTagMessage.class);
//        }

//    private FunctionInvokeMessageReply handleFunctionInvokeReply(JSONObject json) {
//        return json.toJavaObject(FunctionInvokeMessageReply.class);
//    }
//
//    private DeviceRegisterMessage handleRegister(JSONObject json) {
//        DeviceRegisterMessage reply = new DeviceRegisterMessage();
//        reply.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        reply.setDeviceId(json.getString("deviceId"));
//        reply.setTimestamp(System.currentTimeMillis());
//        reply.setHeaders(json.getJSONObject("headers"));
//        return reply;
//    }
//
//    private DeviceUnRegisterMessage handleUnRegister(JSONObject json) {
//        DeviceUnRegisterMessage reply = new DeviceUnRegisterMessage();
//        reply.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        reply.setDeviceId(json.getString("deviceId"));
//        reply.setTimestamp(System.currentTimeMillis());
//        return reply;
//    }
//
//    private ReportPropertyMessage handleReportProperty(JSONObject json) {
//        ReportPropertyMessage msg = ReportPropertyMessage.create();
//        msg.fromJson(json);
//        return msg;
//    }
//
//    private ReadPropertyMessageReply handleReadPropertyReply(JSONObject json) {
//        return json.toJavaObject(ReadPropertyMessageReply.class);
//    }
//
//    private WritePropertyMessageReply handleWritePropertyReply(JSONObject json) {
//        return json.toJavaObject(WritePropertyMessageReply.class);
//    }
//
//    private EventMessage handleFireAlarm(String topic,JSONObject json) {
//        EventMessage eventMessage = new EventMessage();
//        eventMessage.setDeviceId(json.getString("deviceId"));
//        eventMessage.setEvent("fire_alarm");
//        eventMessage.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        eventMessage.setData(new HashMap<>(json));
//        return eventMessage;
//    }
//
//    private EventMessage handleOpenTheDoor(String topic, JSONObject json) {
//        EventMessage eventMessage = new EventMessage();
//        eventMessage.setDeviceId(json.getString("deviceId"));
//        eventMessage.setEvent("open-door");
//        eventMessage.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        eventMessage.setData(new HashMap<>(json));
//        return eventMessage;
//    }
//
//    private EventMessage handleFaultAlarm(String topic, JSONObject json) {
//        // String[] topics = topic.split("[/]");
//        EventMessage eventMessage = new EventMessage();
//        eventMessage.setDeviceId(json.getString("deviceId"));
//        eventMessage.setEvent("fault_alarm");
//        eventMessage.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        eventMessage.setData(new HashMap<>(json));
//        return eventMessage;
//    }
//
//    private EventMessage handleDeviceMessage(String topic, JSONObject json) {
//        EventMessage eventMessage = new EventMessage();
//        eventMessage.setDeviceId(json.getString("deviceId"));
//        eventMessage.setEvent("dev_msg");
//        eventMessage.setMessageId(IDGenerator.SNOW_FLAKE_STRING.generate());
//        eventMessage.setData(new HashMap<>(json));
//        return eventMessage;
//    }
//
//    private CommonDeviceMessage handleDeviceOnlineStatus(String topic, JSONObject json) {
//        CommonDeviceMessage deviceMessage;
//        if ("1".equals(json.getString("status"))) deviceMessage = new DeviceOnlineMessage();
//        else deviceMessage = new DeviceOfflineMessage();
//        deviceMessage.setDeviceId(json.getString("deviceId"));
//        return deviceMessage;
//    }


    private byte toByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }
    private byte[] encodeCmd(String readCmd){
        int len = (readCmd.length() / 2);
        byte[] result = new byte[len];
        char[] chs = readCmd.toCharArray();
        for(int i = 0;i < len;i++) result[i] = (byte)(toByte(chs[i * 2]) << 4 | toByte(chs[i * 2 + 1]));
        return result;
    }
    private float getDesignatedFraction(float value,int frac){
        NumberFormat f = NumberFormat.getNumberInstance();
        f.setRoundingMode(RoundingMode.DOWN);
        f.setMaximumFractionDigits(frac);
        f.setGroupingUsed(false);
        return Float.parseFloat(f.format(value));
    }
    private double getDesignatedFraction(double value,int frac){
        NumberFormat f = NumberFormat.getNumberInstance();
        f.setRoundingMode(RoundingMode.DOWN);
        f.setMaximumFractionDigits(frac);
        f.setGroupingUsed(false);
        return Double.parseDouble(f.format(value));
    }


    //消息编码中心
    @Nonnull
    @Override
    public Mono<? extends EncodedMessage> encode(@Nonnull MessageEncodeContext context) {
        return Mono.defer(() -> {
            Message message = context.getMessage();
            if (message instanceof DeviceMessage) {
                if(message instanceof WritePropertyMessage){  //写属性值
                    WritePropertyMessage mes = (WritePropertyMessage)message;
                    String deviceId = mes.getDeviceId();
                    String topic = "/zt/down/property/" + deviceId;
                    String writeCmd= Objects.requireNonNull(mes.getHeaders()).get("writeCmd")+"";
                    return Mono.just(SimpleMqttMessage
                            .builder()
                            .topic(topic)
                            .payload(encodeCmd(writeCmd)).build());

                } else if(message instanceof ReadPropertyMessage){  //读属性值
                    ReadPropertyMessage mes = (ReadPropertyMessage)message;
                    String deviceId = mes.getDeviceId();
                    String topic = "/zt/down/property/" + deviceId;
                    String readCmd= Objects.requireNonNull(mes.getHeaders()).get("readCmd")+"";
                    return Mono.just(SimpleMqttMessage
                                                .builder()
                                                .topic(topic)
                                                .payload(encodeCmd(readCmd)).build());

                } else if(message instanceof FunctionInvokeMessage){ //功能调用
                    FunctionInvokeMessage mes = ((FunctionInvokeMessage) message);
                    String topic = "/zt/down/function/" + mes.getDeviceId();
                    String cmd = mes.getInputs().get(0).getValue()+"";
                    return Mono.just(SimpleMqttMessage
                                                    .builder()
                                                    .topic(topic)
                                                    .payload(encodeCmd(cmd)).build());

                } else if (message instanceof UpgradeFirmwareMessage || message instanceof RequestFirmwareMessageReply){
                    return Mono.just(SimpleMqttMessage.builder().topic("/firmware/push").payload("").build());

                } else if (message instanceof DisconnectDeviceMessage) {   //断开连接
                    return ((ToDeviceMessageContext) context)
                            .disconnect()
                            .then(Mono.empty());
                }
            }
            return Mono.empty();
        });
    }
}
