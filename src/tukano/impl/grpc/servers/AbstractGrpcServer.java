package tukano.impl.grpc.servers;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;

import io.grpc.Server;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.AbstractServer;
import utils.IP;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;


public class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	protected final Server server;

	protected AbstractGrpcServer(Logger log, String service, int port, AbstractGrpcStub stub) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostName(), port, GRPC_CTX));

		try {
			var keyStore = System.getProperty("javax.net.ssl.keyStore");
    		var keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

			var keystore = KeyStore.getInstance(KeyStore.getDefaultType());
    		try (var in = new FileInputStream(keyStore)) {
    			keystore.load(in, keyStorePassword.toCharArray());
    		}
    		
    		var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    		keyManagerFactory.init(keystore, keyStorePassword.toCharArray());
    		
    		var sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build();			
			this.server = NettyServerBuilder.forPort(port).addService(stub).sslContext(sslContext).build();
		
		} catch (Exception e) {
            throw new RuntimeException(e);
        }
    	
	}

	protected void start() throws IOException {
		
		Discovery.getInstance().announce(service, super.serverURI);
		
		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}
	
}
