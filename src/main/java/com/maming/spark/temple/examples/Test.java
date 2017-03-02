package com.maming.spark.temple.examples;

import com.cloudera.sparkts.models.ARIMA;
import com.cloudera.sparkts.models.ARIMAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vectors$;

import javax.servlet.ServletRegistration;

/**
 * Created by Lenovo on 2017/1/19.
 */
public class Test {

    public static void main(String[] args) {
        System.out.println("aa");
        Class<ServletRegistration> cls = javax.servlet.ServletRegistration.class;
        String path = cls.getResource("/" + cls.getName().replace('.', '/') + ".class").toString();
        //输出 jar:file:/C:/Users/Lenovo/.m2/repository/com/typesafe/akka/akka-actor_2.10/2.3.1/akka-actor_2.10-2.3.1.jar!/akka/actor/ActorRef.class
        System.out.println(path);

        if (path.startsWith("jar:file:")) {//从jar包找到了该class
            path = path.substring("jar:file:".length(), path.indexOf('!'));
        }
        System.out.println(path);

    }
}
