/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MirrorSourceTaskFaultToleranceTest {

    @Test
    public void testTruncationDetection() {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "source", new DefaultReplicationPolicy(), null);

        TopicPartition tp = new TopicPartition("topic", 0);
        
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic", 0, 100, new byte[0], new byte[0]);
        ConsumerRecords<byte[], byte[]> records1 = new ConsumerRecords<>(Collections.singletonMap(tp, Collections.singletonList(record1)));
        
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic", 0, 500, new byte[0], new byte[0]);
        ConsumerRecords<byte[], byte[]> records2 = new ConsumerRecords<>(Collections.singletonMap(tp, Collections.singletonList(record2)));

        when(consumer.poll(any())).thenReturn(records1).thenReturn(records2);
        
        task.poll(); 
        
        assertThrows(KafkaException.class, task::poll, "Should throw KafkaException when gap is detected");
    }

    @Test
    public void testTopicResetRecovery() {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "source", new DefaultReplicationPolicy(), null);

        TopicPartition tp = new TopicPartition("topic", 0);
        
        when(consumer.poll(any())).thenThrow(new OffsetOutOfRangeException(Collections.singletonMap(tp, 1000L)));
        
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(tp, 0L);
        when(consumer.beginningOffsets(any())).thenReturn(beginningOffsets);

        task.poll();

        verify(consumer).seekToBeginning(any());
    }
}
