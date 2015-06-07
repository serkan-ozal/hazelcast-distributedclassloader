/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.distributedclassloader;

import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

/**
 * Finds data (bytecode / byte[]) of {@link Class} with given name 
 * over Hazelcast Distributed ClassLoader group.
 * 
 * @author Serkan OZAL
 */
public class HazelcastDistributedClassLoaderProcessor {

    private static final Logger LOGGER = 
            Logger.getLogger(HazelcastDistributedClassLoaderProcessor.class.getName());
    
    private HazelcastInstance hazelcastInstance;
    private IExecutorService executorService;
    private Cluster cluster;
    private IMap<String, ClassData> classDataMap;

    public HazelcastDistributedClassLoaderProcessor() {
        init();
    }

    private void init() {
        Config config = new Config();
        config.getGroupConfig().setName("hz-distributedclassloader-group");
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        executorService = hazelcastInstance.getExecutorService("hz-distributedclassloader-executor");
        classDataMap = hazelcastInstance.getMap("hz-distributedclassloader-map");
        cluster = hazelcastInstance.getCluster();
    }

    public byte[] getClassData(String className) {
        ClassData classData = classDataMap.get(className);
        if (classData != null) {
            return classData.getClassDefinition();
        }
        for (Member member : cluster.getMembers()) {
            if (member.localMember()) {
                continue;
            }
            Future<ClassData> classDataFuture = 
                    executorService.submitToMember(
                            new ClassDataFinder(className), member);
            try {
                classData = classDataFuture.get();
                if (classData != null) {
                    classDataMap.put(className, classData);
                    byte[] classDef = classData.getClassDefinition();
                    if (classDef != null) {
                        return classDef;
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Unable to get class data for class " + className + 
                           " from member " + member, e);
            }
        }
        return null;
    }

    public void destroy() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

}
