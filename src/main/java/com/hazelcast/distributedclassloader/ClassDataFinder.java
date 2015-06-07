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

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.sun.org.apache.bcel.internal.Repository;
import com.sun.org.apache.bcel.internal.classfile.JavaClass;

/**
 * Finds {@link ClassData} of {@link Class} with given name.
 * 
 * @author Serkan OZAL
 */
@SuppressWarnings({ "serial", "restriction" })
public class ClassDataFinder implements Callable<ClassData>, Serializable {

    private static final Logger LOGGER = 
            Logger.getLogger(ClassDataFinder.class.getName());

    private final String className;

    public ClassDataFinder(String className) {
        this.className = className;
    }

    @Override
    public ClassData call() {
        LOGGER.info("Getting data for class " + className + " ...");

        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            LOGGER.severe("Couldn't find class " + className);
            return null;
        }
        JavaClass javaClass = Repository.lookupClass(clazz);
        if (javaClass == null) {
            return null;
        }
        byte[] classDef = javaClass.getBytes();
        return new ClassData(classDef);
    }

}
