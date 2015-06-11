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

package test1;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.distributedclassloader.ClassData;
import com.hazelcast.distributedclassloader.HazelcastDistributedClassLoader;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class Test {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.getGroupConfig().setName("distributed-classloader");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastDistributedClassLoader.setHazelcastInstance(instance);
        IMap<Object, Object> map = instance.getMap("hz-distributedclassloader-map");
        byte[] classDefinition = getHiddenClassDefinition();
        map.put("test1.HiddenClass", new ClassData(classDefinition));

        Class<?> hiddenClass = Class.forName("test1.HiddenClass");
        Object hiddenInstance = hiddenClass.newInstance();
        System.err.println("hiddenInstance: " + hiddenInstance);
    }

    static byte[] getHiddenClassDefinition() {
        try {
            FileInputStream fis = new FileInputStream("hidden_class");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int len;
            byte[] data = new byte[1024];
            while ((len = fis.read(data)) != -1) {
                baos.write(data, 0, len);
            }
            fis.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
