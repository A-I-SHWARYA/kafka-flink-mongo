package TestElements;


import com.nest.bluehydrogen.kafkaFlinkMongo.flink.FlinkConsumer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.bson.Document;

//@RunWith(SpringRunner.class)
@SpringBootTest
public class FlinkConsumerTest {

    @Autowired
    private FlinkConsumer flinkConsumer;

    @MockBean
    private StreamExecutionEnvironment env;

    @MockBean
    private FlinkKafkaConsumer<String> consumer;

    @MockBean
    private DataStream<String> dataStream;

    @MockBean
    private DataStream<Document> transformedDataStream;

    public class StringToDocumentMapper implements MapFunction<String, Document> {
        @Override
        public Document map(String value) throws Exception {
            return Document.parse(value);
        }
    }

    @Test
    public void testStart() throws Exception {
        Mockito.when(env.addSource(consumer)).thenReturn((DataStreamSource<String>) dataStream);
        Mockito.when(dataStream.map(Mockito.any(StringToDocumentMapper.class))).thenReturn(transformedDataStream);

        flinkConsumer.start();
        Mockito.verify(env).execute(flinkConsumer.getFlinkJobName());
    }
}
