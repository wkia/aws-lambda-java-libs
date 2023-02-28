/* Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. */

package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import org.crac.Context;
import org.crac.Resource;

import java.io.InputStream;
import java.lang.annotation.Native;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * This module defines the native Runtime Interface Client which is responsible for all HTTP
 * interactions with the Runtime API.
 */
class NativeClient {
    private static final String nativeLibPath = "/tmp/.aws-lambda-runtime-interface-client";
    private static final String architecturePathSuffix = "/" + getArchIdentifier();
    // Implementation based on AWS CRT, but adopted to support 64-bit architectures only (ref. https://github.com/awslabs/aws-crt-java/blob/0e9c3db8b07258b57c2503cfc47c787ccef10670/src/main/java/software/amazon/awssdk/crt/CRT.java#L106-L134)
    private static final String supported_arm_architectures = "^(aarch64.*|arm64.*)$";
    private static final String supported_x86_architectures = "^(x8664|amd64|ia32e|em64t|x64|x86_64)$";
    private static final String[] libsToTry = {
            "aws-lambda-runtime-interface-client.glibc.so",
            "aws-lambda-runtime-interface-client.musl.so",
    };
    private static final Throwable[] exceptions = new Throwable[libsToTry.length];

    static class CheckpointState implements Resource {
        enum State {
            WORKING,
            SYNCING,
            SYNCED,
        };

        State state = State.WORKING;

        private void waitFor(State targetState) {
            while (state != targetState) {
                try {
                    this.wait();
                } catch (InterruptedException interruptedException) {
                }
            }
        }

        @Override
        public synchronized void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
            state = State.SYNCING;
            waitFor(State.SYNCED);
            deinitializeClient();
        }

        @Override
        public synchronized void afterRestore(Context<? extends Resource> context) throws Exception {
            initializeNativeClient();
            state = State.WORKING;
            this.notifyAll();
        }

        public synchronized void syncPoint() {
            if (state == State.SYNCING) {
                state = State.SYNCED;
                this.notifyAll();
            }
            waitFor(State.WORKING);
        }
    }

    static CheckpointState checkpointState = new CheckpointState();
    static {
        boolean loaded = false;
        String basestr = System.getProperty("com.amazonaws.services.lambda.runtime.api.client.NativeClient.libsBase", "/");
        Path base = Paths.get(basestr);
        for (int i = 0; !loaded && i < libsToTry.length; ++i) {
            Path p = base.resolve(libsToTry[i]);
            if (Files.exists(p)) {
                try {
                    System.load(p.toString());
                    loaded = true;
                } catch (UnsatisfiedLinkError e) {
                    exceptions[i] = e;
                } catch (Exception e) {
                    exceptions[i] = e;
                }
            }
            if (!loaded && exceptions[i] == null) {
                try (InputStream lib = NativeClient.class.getResourceAsStream("/" +
                        Paths.get(architecturePathSuffix, libsToTry[i]).toString())) {
                    Files.copy(lib, Paths.get(nativeLibPath), StandardCopyOption.REPLACE_EXISTING);
                    System.load(nativeLibPath);
                    loaded = true;
                } catch (UnsatisfiedLinkError e) {
                    exceptions[i] = e;
                } catch (Exception e) {
                    exceptions[i] = e;
                }
            }
        }

        if (!loaded) {
            for (int i = 0; i < libsToTry.length; ++i) {
                System.err.print(exceptions[i]);
                System.err.printf("Failed to load the native runtime interface client library %s. Exception: %s\n", libsToTry[i], exceptions[i].getMessage());
            }
            System.exit(-1);
        }
        initializeNativeClient();
        org.crac.Core.getGlobalContext().register(checkpointState);
    }

    private static void initializeNativeClient() {
        String userAgent = String.format(
                "aws-lambda-java/%s-%s" ,
                System.getProperty("java.vendor.version"),
                NativeClient.class.getPackage().getImplementationVersion());
        initializeClient(userAgent.getBytes());
    }

    /**
     * @return a string describing the detected architecture the RIC is executing on
     * @throws UnknownPlatformException
     */
    static String getArchIdentifier() {
        String arch = System.getProperty("os.arch");

        if (arch.matches(supported_x86_architectures)) {
            return "x86_64";
        } else if (arch.matches(supported_arm_architectures)) {
            return "aarch64";
        }

        throw new UnknownPlatformException("architecture not supported: " + arch);
    }

    static native void initializeClient(byte[] userAgent);

    private static native InvocationRequest next();

    static InvocationRequest nextWrapper() {
        return next();
    }

    private static native void postInvocationResponse(byte[] requestId, byte[] response);

    static void postInvocationResponseWrapper(byte[] requestId, byte[] response) {
        postInvocationResponse(requestId, response);
        checkpointState.syncPoint();
    }

    static native void deinitializeClient();
}
