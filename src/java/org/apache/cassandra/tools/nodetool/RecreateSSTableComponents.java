/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "recreatesstablecomponents", description = "Recreates sstable components and writes them to disk")
public class RecreateSSTableComponents extends NodeTool.NodeToolCmd
{
    Collection<Component.Type> components = new ArrayList<>();

    @Arguments(usage = "<keyspace> <table>", description = "The keyspace and table name")
    private List<String> args = new ArrayList<>();

    @Option(title = "jobs",
    name = {"-j", "--jobs"},
    description = "Number of sstables to work on simultanously, set to 0 to use all available compaction threads")
    private int jobs = 2;

    @Option(title = "all",
    name = {"-a", "--all"},
    description = "Re-create all ss-table components (will trigger upgradesstables and execute a compaction)")
    private boolean all = false;

    @Option(title = "bloomfilter",
    name = {"-f", "--filter"},
    description = "Re-create the bloom filter")
    private boolean bloomfilter = false;

    @Option(title = "summary",
    name = {"-s", "--summary"},
    description = "Re-build the primary index summary from the index stored on disk")
    private boolean summary = false;

    @Option(title = "index",
    name = {"-i", "--index"},
    description = "Re-build the primary index (will trigger upgradesstables and execute a compaction)")
    private boolean index = false;

    @Option(title = "compression",
    name = {"-c", "--compression"},
    description = "Rebuild the compression info")
    private boolean compression = false;

    @Option(title = "statistics",
    name = {"-t", "--statistics"},
    description = "Rebuild the statistics")
    private boolean statistics = false;

    @Option(title = "contents",
    name = {"-c", "--contents"},
    description = "Rebuild the table of contents")
    private boolean contents = false;

    @Override
    public void execute(NodeProbe probe)
    {
        System.out.println(components);
        if(all){
            components = Arrays.asList(Component.Type.class.getEnumConstants());
        } else
        {

            if (index)
            {
                components.add(Component.Type.PRIMARY_INDEX);
            }

            if (bloomfilter)
            {
                components.add(Component.Type.FILTER);
            }

            if (summary)
            {
                components.add(Component.Type.SUMMARY);
            }

            if (compression)
            {
                components.add(Component.Type.COMPRESSION_INFO);
            }

            if (statistics)
            {
                components.add(Component.Type.STATS);
            }

            if (contents)
            {
                components.add(Component.Type.TOC);
            }
        }

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] cfnames = parseOptionalTables(args);
        try
        {
            for (String keyspace : keyspaces)
                probe.recreateSSTableComponents(jobs, keyspace, cfnames, components);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Got error while relocating", e);
        }
    }
}
