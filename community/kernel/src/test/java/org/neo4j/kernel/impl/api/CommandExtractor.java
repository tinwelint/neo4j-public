/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;

public class CommandExtractor implements Visitor<Command,IOException>
{
    private final List<Command> commands = new ArrayList<>();

    @Override
    public boolean visit( Command command ) throws IOException
    {
        commands.add( command );
        return false;
    }

    public Command[] commands()
    {
        return commands.toArray( new Command[commands.size()] );
    }

    public static Command[] commandsOf( TransactionRepresentation transaction ) throws IOException
    {
        CommandExtractor extractor = new CommandExtractor();
        transaction.accept( extractor );
        return extractor.commands();
    }
}
