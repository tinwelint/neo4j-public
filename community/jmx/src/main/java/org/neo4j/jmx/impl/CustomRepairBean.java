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
package org.neo4j.jmx.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Exceptions;
import org.neo4j.jmx.CustomRepair;
import org.neo4j.jmx.Description;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.repair.ZD3483InconsistenciesFix;

@Description( "Custom repair of inconsistencies" )
public final class CustomRepairBean extends Neo4jMBean implements CustomRepair
{
    private final GraphDatabaseAPI db;

    public CustomRepairBean( ManagementData data ) throws NotCompliantMBeanException
    {
        super( data );
        db = data.getKernelData().graphDatabase();
    }

    @Override
    public String doRepair()
    {
        try
        {
            new ZD3483InconsistenciesFix( db ).doRepair();
        }
        catch ( Throwable t )
        {
            return Exceptions.stringify( t );
        }
        return "Repair transaction committed";
    }
}
