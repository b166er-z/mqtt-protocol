package org.zt.mqtt.protocol;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.Value;
import org.jetlinks.core.defaults.Authenticator;
import org.jetlinks.core.defaults.CompositeProtocolSupport;
import org.jetlinks.core.device.*;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.metadata.DefaultConfigMetadata;
import org.jetlinks.core.metadata.types.PasswordType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.supports.official.JetLinksDeviceMetadataCodec;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

public class MqttProtocolProvider implements ProtocolSupportProvider {

    private static final DefaultConfigMetadata mqttConfig = new DefaultConfigMetadata(
            "MQTT认证配置"
            , "MQTT解析协议")
            .add("username", "username", "mqtt认证的用户名", StringType.GLOBAL)
            .add("password", "password", "mqtt认证的密码", PasswordType.GLOBAL);

    @Override
    public void dispose() {
        //协议卸载时执行

    }

    @Override
    public Mono<? extends ProtocolSupport> create(ServiceContext context) {
        CompositeProtocolSupport support = new CompositeProtocolSupport();
        support.setId("mqtt_p01");
        support.setName("mqtt协议1.0");
        support.setDescription("第一版mqtt协议");
        //固定为JetLinksDeviceMetadataCodec
        support.setMetadataCodec(new JetLinksDeviceMetadataCodec());

        {
            //自定义的MQTT消息编解码器
            MqttMessageCodec codec = new MqttMessageCodec();

            // 参数一是协议支持,和编解码类MqttMessageCodec中保持一致，
            // 参数二定义使用的编码解码类
            support.addMessageCodecSupport(DefaultTransport.MQTT, () -> Mono.just(codec));
        }

        //MQTT需要的配置信息
        support.addConfigMetadata(DefaultTransport.MQTT, mqttConfig);

        //MQTT认证策略
        support.addAuthenticator(DefaultTransport.MQTT, new Authenticator() {
            @Override
            //使用clientId作为设备ID时的认证方式
            public Mono<AuthenticationResponse> authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceOperator device) {
                MqttAuthenticationRequest mqttRequest = ((MqttAuthenticationRequest) request);
                return device.getConfigs("username", "password")
                        .flatMap(values -> {
                            String username = values.getValue("username").map(Value::asString).orElse(null);
                            String password = values.getValue("password").map(Value::asString).orElse(null);
                            if (mqttRequest.getUsername().equals(username) && mqttRequest.getPassword().equals(password)) {
                                return Mono.just(AuthenticationResponse.success());
                            } else {
                                return Mono.just(AuthenticationResponse.error(400, "密码错误"));
                            }
                        });
            }
        });
//            @Override
//            //在网关中配置使用指定的认证协议时的认证方式
//            public Mono<AuthenticationResponse> authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceRegistry registry) {
//                MqttAuthenticationRequest mqttRequest = ((MqttAuthenticationRequest) request);
//                return registry
//                        .getDevice(mqttRequest.getUsername()) //用户名作为设备ID
//                        .flatMap(device -> device
//                                .getSelfConfig("password").map(Value::asString) //密码
//                                .flatMap(password -> {
//                                    if (password.equals(mqttRequest.getPassword())) {
//                                        //认证成功，需要返回设备ID
//                                        return Mono.just(AuthenticationResponse.success(mqttRequest.getUsername()));
//                                    } else {
//                                        return Mono.just(AuthenticationResponse.error(400, "密码错误"));
//                                    }
//                                })
//                      );
//            }

        //tcp client,通过tcp客户端连接其他服务处理设备消息
//        {
//            support.addConfigMetadata(DefaultTransport.TCP, tcpClientConfig);
//            return Mono
//                .zip(
//                    Mono.justOrEmpty(context.getService(DecodedClientMessageHandler.class)),
//                    Mono.justOrEmpty(context.getService(DeviceSessionManager.class)),
//                    Mono.justOrEmpty(context.getService(Vertx.class))
//                )
//                .map(tp3 -> new TcpClientMessageSupport(tp3.getT1(), tp3.getT2(), tp3.getT3()))
//                .doOnNext(tcp -> {
//                    //设置状态检查
//                    support.setDeviceStateChecker(tcp);
//                    support.doOnDispose(tcp); //协议失效时执行
//                })
//                .thenReturn(support);
//
//        }

        return Mono.just(support);
    }
}
