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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link URLClassLoader} implementation to search unknown classes 
 * over Hazelcast Distributed ClassLoader cluster.
 * 
 * @author Serkan OZAL
 */
// -Djava.system.class.loader=com.hazelcast.distributedclassloader.HazelcastDistributedClassLoader
public class HazelcastDistributedClassLoader extends URLClassLoader {

    private static final Logger LOGGER = 
            Logger.getLogger(HazelcastDistributedClassLoader.class.getName());

    private static final URL[] URLS = findClasspathUrls();

    private boolean initInProgress;
    private ClassLoader parent;
    private HazelcastDistributedClassLoaderProcessor processor;

    public HazelcastDistributedClassLoader() {
        super(URLS);
    }

    public HazelcastDistributedClassLoader(ClassLoader parent) {
        super(URLS, null);
        this.parent = parent;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        Class<?> clazz = findLoadedClass(name);
        if (clazz == null) {
            LOGGER.finest("Will load class: " + name);
            if (parent != null 
                &&
                (
                    name.startsWith("java") || name.startsWith("javax") || 
                    name.startsWith("sun") || name.startsWith("com.sun") || 
                    name.startsWith("com.hazelcast")
                )) {
                clazz = parent.loadClass(name);
                LOGGER.finest("Loaded class: " + name + 
                              " and its classloader is " + clazz.getClassLoader());
                if (String.class.equals(clazz)) {
                    initIfNeeded();
                }
            } else {
                clazz = findClass(name);
            }
        }
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected synchronized Class<?> findClass(String name)
            throws ClassNotFoundException {
        try {
            LOGGER.finest("Will find class: " + name);
            Class<?> clazz = super.findClass(name);
            LOGGER.finest("Found class: " + name + " and its classloader is " + 
                          clazz.getClassLoader());
            return clazz;
        } catch (ClassNotFoundException e) {
            if (initInProgress || name.startsWith("com.hazelcast")) {
                throw e;
            }

            initIfNeeded();

            byte[] classDef = processor.getClassData(name);
            if (classDef == null) {
                throw new ClassNotFoundException(name);
            } else {
                return defineClass(classDef, 0, classDef.length);
            }
        }
    }

    private static URL[] findClasspathUrls() {
        Set<URL> urls = new HashSet<URL>();
        try {
            String[] classpathProperties = 
                    System.getProperty("java.class.path").split(File.pathSeparator);
            for (String classpathProperty : classpathProperties) {
                urls.add(new File(classpathProperty).toURI().toURL());
            }
            urls.addAll(getExtURLs(getExtDirs()));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unable to find classpath urls !", e);
        }
        return urls.toArray(new URL[0]);
    }

    private static File[] getExtDirs() {
        String extDirsProperty = System.getProperty("java.ext.dirs");
        File[] extDirs;
        if (extDirsProperty != null) {
            StringTokenizer st = new StringTokenizer(extDirsProperty, File.pathSeparator);
            int count = st.countTokens();
            extDirs = new File[count];
            for (int i = 0; i < count; i++) {
                extDirs[i] = new File(st.nextToken());
            }
        } else {
            extDirs = new File[0];
        }
        return extDirs;
    }

    private static Set<URL> getExtURLs(File[] dirs) throws IOException {
        Set<URL> urls = new HashSet<URL>();
        for (int i = 0; i < dirs.length; i++) {
            String[] files = dirs[i].list();
            if (files != null) {
                for (int j = 0; j < files.length; j++) {
                    if (!files[j].equals("meta-index")) {
                        File f = new File(dirs[i], files[j]);
                        urls.add(f.toURI().toURL());
                    }
                }
            }
        }
        return urls;
    }

    private synchronized void init() throws ClassNotFoundException {
        initInProgress = true;
        try {
            LOGGER.info("Initializing ...");
            processor = new HazelcastDistributedClassLoaderProcessor();
            LOGGER.info("Initialized");
        } catch (Throwable t) {
            throw new IllegalStateException("Could not be initialized !", t);
        } finally {
            initInProgress = false;
        }
    }

    private void initIfNeeded() throws ClassNotFoundException {
        if (processor == null) {
            init();
        }
    }
    
    public void destroy() {
        if (processor != null) {
            processor.destroy();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        destroy();
    }

}
