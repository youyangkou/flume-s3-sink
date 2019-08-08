package com.xz.utils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author kouyouyang
 * @date 2019-07-24 11:34
 */
public class ReflectionUtils {

    public static Object newInstance(String className) throws IOException {
        Constructor<?> constructor = null;
        try {
            Class<?> clazz = Class.forName(className);
            constructor = clazz.getDeclaredConstructor(new Class[] {});
            constructor.setAccessible(true);
        } catch (ClassNotFoundException e) {
            throw new IOException("Fail to find class : " + className, e);
        } catch (SecurityException e) {
            throw new IOException(e);
        } catch (NoSuchMethodException e) {
            throw new IOException(e);
        }
        try {
            return constructor.newInstance();
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        } catch (InvocationTargetException e) {
            throw new IOException(e);
        }
    }
}
