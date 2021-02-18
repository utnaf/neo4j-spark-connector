package org.neo4j.spark;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PythonTests {

    @Test
    public void runPythonTests() {
        try (Context context = Context.create()) {
            File file = new File("/Users/utnaf/Workspace/Neo4j/neo4j-spark-connector/python/helloworld.py");
            Source source = Source.newBuilder("python", file).build();
            context.eval(source);
//            Value result = context.eval("python",
//                    );
//            assert result.hasMembers();
//
//            int id = result.getMember("id").asInt();
//            assert id == 42;
//
//            String text = result.getMember("text").asString();
//            assert text.equals("42");
//
//            Value array = result.getMember("arr");
//            assert array.hasArrayElements();
//            assert array.getArraySize() == 3;
//            assert array.getArrayElement(1).asInt() == 42;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
