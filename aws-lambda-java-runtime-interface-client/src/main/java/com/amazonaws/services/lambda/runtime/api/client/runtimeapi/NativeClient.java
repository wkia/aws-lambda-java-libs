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
                try (InputStream lib = NativeClient.class.getResourceAsStream("/" + libsToTry[i])) {
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
